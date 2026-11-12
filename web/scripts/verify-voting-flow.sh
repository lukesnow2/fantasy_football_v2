#!/usr/bin/env bash
#
# End-to-end verification that voting actually amends the constitution.
#
# Drives the real HTTP API with real sessions rather than calling the modules
# directly, so it exercises the same path a manager does. Sessions are inserted
# straight into app.session (the token is the raw value, its sha256 is the row
# id) — that is test setup, not a bypass; every request still goes through
# hooks.server.ts.
#
# Asserts:
#   · a proposal passes on the 6th yes vote, not the 10th
#   · exactly one amendment row is written
#   · a new constitution version appears, linked to that amendment
#   · the clause text actually changes in v2 while v1 still reads the original
#   · early rejection fires when yes becomes unreachable
#   · the subject of a removal cannot vote on it
#
# Usage: ORIGIN=http://localhost:5175 DATABASE_URL=... ./scripts/verify-voting-flow.sh

set -uo pipefail

ORIGIN="${ORIGIN:-http://localhost:5175}"
DB="${DATABASE_URL:-postgres://$(whoami)@localhost:5432/the_league}"

pass=0; fail=0
ok()   { echo "  ✓ $1"; pass=$((pass+1)); }
bad()  { echo "  ✗ $1"; fail=$((fail+1)); }
check(){ if [[ "$2" == "$3" ]]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }

q() { psql "$DB" -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
qraw() { psql "$DB" -tAc "$1" 2>/dev/null; }

echo "Verifying the voting lifecycle against $ORIGIN"
echo

# --- setup: ten members, ten sessions -------------------------------------
echo "Setting up ten members with active sessions"
psql "$DB" -q >/dev/null 2>&1 <<'SQL'
-- Roll the constitution back to v1. Amendments from a previous run leave a v2
-- behind, which makes the "a version 2 now exists" assertions meaningless.
delete from app.constitution_version where version_no > 1;
delete from app.rule_vote;
delete from app.rule_amendment;
delete from app.rule_proposal;
delete from app.session;
delete from app."user";
delete from app.league_member;
SQL

declare -a TOKENS
KEYS=(5 6 8 9 10 14 15 16 18 19)

for i in "${!KEYS[@]}"; do
	key="${KEYS[$i]}"
	name=$(psql "$DB" -tAc "select manager_name from edw.dim_manager where manager_key=$key" | tr -d '[:space:]')
	psql "$DB" -q -c "insert into app.league_member (id, email, manager_key, role, active, display_name)
		values ('m$key', 'manager$key@test.local', $key,
		        $( [[ "$key" == "14" ]] && echo "'commissioner'" || echo "'member'" ), true, '$name')" >/dev/null

	tok=$(node -e "const c=require('crypto');const t=c.randomBytes(18).toString('base64url');
		console.log(t+' '+c.createHash('sha256').update(t).digest('hex'));")
	raw=$(echo "$tok" | cut -d' ' -f1); hash=$(echo "$tok" | cut -d' ' -f2)
	TOKENS[$i]="$raw"

	psql "$DB" -q -c "insert into app.\"user\" (id, username, manager_key, email, account_status)
		values ('u$key', 'user$key', $key, 'manager$key@test.local', 'active')" >/dev/null
	psql "$DB" -q -c "insert into app.session (id, user_id, expires_at)
		values ('$hash', 'u$key', now() + interval '30 days')" >/dev/null
done
check "ten members seeded" "$(q 'select count(*) from app.league_member')" "10"

vote_as() { # $1 = index into KEYS, $2 = proposalKey, $3 = yes|no|abstain
	curl -s -o /dev/null -w '%{http_code}' -X POST "$ORIGIN/api/rule-votes" \
		-H 'Content-Type: application/json' \
		-H "Cookie: auth-session=${TOKENS[$1]}" \
		-d "{\"proposalKey\":$2,\"vote\":\"$3\"}"
}

# --- scenario 1: a scoring change passes on the 6th vote -------------------
echo
echo "1. A scoring change (simple majority, 6 of 10) passes on the 6th yes"

CLAUSE=$(qraw "select c.clause_uid from app.constitution_clause c
	join app.constitution_section s using (section_key)
	join app.constitution_version v using (version_key)
	where v.version_no = 1 and s.section_id = 'appendix1' and c.depth = 1 limit 1" | tr -d '[:space:]')
ORIGINAL=$(qraw "select body from app.constitution_clause c
	join app.constitution_section s using (section_key)
	join app.constitution_version v using (version_key)
	where v.version_no = 1 and c.clause_uid = '$CLAUSE'")

psql "$DB" -q -c "insert into app.rule_proposal
	(proposal_id, title, description, proposal_type, category, affected_section,
	 target_clause_uid, proposed_language, rationale, effective_season, submitted_by,
	 status, required_votes, eligible_voters, voting_start_date, voting_end_date)
	values ('p-scoring', 'Passing TDs are worth 6', 'd', 'edit_clause', 'scoring', 'appendix1',
	        '$CLAUSE', 'Passing Touchdowns: 6', 'Four is stingy', 2027, 14,
	        'active', 6, 10, now(), now() + interval '7 days')" >/dev/null

PK=$(q "select proposal_key from app.rule_proposal where proposal_id='p-scoring'")

for i in 0 1 2 3 4; do vote_as "$i" "$PK" yes >/dev/null; done
check "still active after 5 yes votes" "$(q "select status from app.rule_proposal where proposal_key=$PK")" "active"
check "no amendment written yet" "$(q 'select count(*) from app.rule_amendment')" "0"

vote_as 5 "$PK" yes >/dev/null
check "passed on the 6th yes vote" "$(q "select status from app.rule_proposal where proposal_key=$PK")" "passed"
check "settled_at is stamped" "$(q "select case when settled_at is null then 'null' else 'set' end from app.rule_proposal where proposal_key=$PK")" "set"
check "exactly one amendment row" "$(q 'select count(*) from app.rule_amendment')" "1"
check "amendment is linked to the proposal" "$(q "select proposal_key from app.rule_amendment")" "$PK"
check "a version 2 now exists" "$(q 'select count(*) from app.constitution_version where version_no=2')" "1"
check "version 2 points at the amendment" \
	"$(q "select coalesce(amendment_key::text, 'none') from app.constitution_version where version_no=2")" \
	"$(q "select amendment_key::text from app.rule_amendment limit 1")"

# A change effective in a future season must not become current today.
check "v2 is dated to the 2027 season start, so v1 is still in force" \
	"$(q "select version_no::text from app.constitution_version where effective_at <= now() order by version_no desc limit 1")" "1"

V2=$(qraw "select c.body from app.constitution_clause c
	join app.constitution_section s using (section_key)
	join app.constitution_version v using (version_key)
	where v.version_no = 2 and c.clause_uid = '$CLAUSE'")
V1=$(qraw "select c.body from app.constitution_clause c
	join app.constitution_section s using (section_key)
	join app.constitution_version v using (version_key)
	where v.version_no = 1 and c.clause_uid = '$CLAUSE'")

check "v2 carries the proposed wording" "$(echo "$V2" | xargs)" "Passing Touchdowns: 6"
check "v1 still reads the original" "$(echo "$V1" | xargs)" "$(echo "$ORIGINAL" | xargs)"
check "clause_uid survived the clone" "$(q "select count(*) from app.constitution_clause where clause_uid='$CLAUSE'")" "2"
check "v2 has the same clause count as v1" \
	"$(q 'select count(*) from app.constitution_clause c join app.constitution_section s using (section_key) join app.constitution_version v using (version_key) where v.version_no=2')" \
	"$(q 'select count(*) from app.constitution_clause c join app.constitution_section s using (section_key) join app.constitution_version v using (version_key) where v.version_no=1')"

echo
echo "2. Voting is closed once a proposal has settled"
check "a late vote is refused with 409" "$(vote_as 6 "$PK" yes)" "409"

# --- scenario 2: early rejection ------------------------------------------
echo
echo "3. A general change (super-majority, 7 of 10) is rejected once 7 yes is unreachable"
psql "$DB" -q -c "insert into app.rule_proposal
	(proposal_id, title, description, proposal_type, category, affected_section,
	 target_clause_uid, proposed_language, rationale, effective_season, submitted_by,
	 status, required_votes, eligible_voters, voting_start_date, voting_end_date)
	values ('p-general', 'Something sweeping', 'd', 'edit_clause', 'general', 'article9',
	        '$CLAUSE', 'x', 'y', 2027, 14, 'active', 7, 10, now(), now() + interval '7 days')" >/dev/null
GK=$(q "select proposal_key from app.rule_proposal where proposal_id='p-general'")

for i in 0 1 2; do vote_as "$i" "$GK" no >/dev/null; done
check "still active at 3 no votes" "$(q "select status from app.rule_proposal where proposal_key=$GK")" "active"
vote_as 3 "$GK" no >/dev/null
check "rejected at 4 no votes (7 of 10 unreachable)" "$(q "select status from app.rule_proposal where proposal_key=$GK")" "rejected"
check "no amendment was written for a rejection" "$(q 'select count(*) from app.rule_amendment')" "1"

echo
echo "4. Abstentions count against the threshold, like a no"
psql "$DB" -q -c "insert into app.rule_proposal
	(proposal_id, title, description, proposal_type, category, affected_section,
	 target_clause_uid, proposed_language, rationale, effective_season, submitted_by,
	 status, required_votes, eligible_voters, voting_start_date, voting_end_date)
	values ('p-abstain', 'Abstention test', 'd', 'edit_clause', 'general', 'article9',
	        '$CLAUSE', 'x', 'y', 2027, 14, 'active', 7, 10, now(), now() + interval '7 days')" >/dev/null
AK=$(q "select proposal_key from app.rule_proposal where proposal_id='p-abstain'")
for i in 0 1 2 3; do vote_as "$i" "$AK" abstain >/dev/null; done
check "rejected after 4 abstentions" "$(q "select status from app.rule_proposal where proposal_key=$AK")" "rejected"

# --- scenario 3: removal subject cannot vote ------------------------------
echo
echo "5. Article 8 IV — the subject of a removal does not vote on it"
psql "$DB" -q -c "insert into app.rule_proposal
	(proposal_id, title, description, proposal_type, category, affected_section,
	 target_clause_uid, subject_manager_key, proposed_language, rationale, effective_season,
	 submitted_by, status, required_votes, eligible_voters, voting_start_date, voting_end_date)
	values ('p-removal', 'Remove a manager', 'd', 'edit_clause', 'manager_removal', 'article8',
	        '$CLAUSE', 5, 'x', 'y', 2027, 14, 'active', 9, 9, now(), now() + interval '7 days')" >/dev/null
RK=$(q "select proposal_key from app.rule_proposal where proposal_id='p-removal'")
check "the subject (manager 5) is refused with 403" "$(vote_as 0 "$RK" no)" "403"
check "another manager can vote normally" "$(vote_as 1 "$RK" yes)" "200"

echo
echo "6. Unauthenticated voting is refused"
check "no session means 401" \
	"$(curl -s -o /dev/null -w '%{http_code}' -X POST "$ORIGIN/api/rule-votes" \
		-H 'Content-Type: application/json' -d "{\"proposalKey\":$RK,\"vote\":\"yes\"}")" "401"

echo
echo "──────────────────────────────"
echo "$pass passed, $fail failed"
exit $(( fail > 0 ? 1 : 0 ))
