#!/usr/bin/env bash
#
# End-to-end verification of the magic-link sign-in flow against a running dev
# server with EMAIL_PROVIDER=console.
#
# Checks the properties that matter and that are easy to regress:
#   1. A known address and an unknown one produce identical responses
#   2. No token row is created for an unknown address
#   3. A link works exactly once
#   4. An expired link is refused
#   5. Email matching is case-insensitive
#   6. Deactivating a member invalidates a link already issued
#   7. Gated routes redirect when signed out
#
# The send rate limiter lives in the dev server's memory, so back-to-back runs
# without restarting it will exhaust the budget and skew section 1. Restart the
# server between full runs.
#
# Usage: ORIGIN=http://localhost:5175 DATABASE_URL=... ./scripts/verify-login-flow.sh

set -uo pipefail

ORIGIN="${ORIGIN:-http://localhost:5175}"
DB="${DATABASE_URL:-postgres://$(whoami)@localhost:5432/the_league}"
KNOWN="${KNOWN_EMAIL:-luke@priora.io}"
UNKNOWN="definitely-not-a-manager@example.com"

pass=0; fail=0
ok()   { echo "  ✓ $1"; pass=$((pass+1)); }
bad()  { echo "  ✗ $1"; fail=$((fail+1)); }
check(){ if [[ "$2" == "$3" ]]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }

q() { psql "$DB" -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }

post_login() {
	curl -s -i -X POST "$ORIGIN/login" \
		-H 'Content-Type: application/x-www-form-urlencoded' \
		-H "Origin: $ORIGIN" \
		--data-urlencode "email=$1" 2>/dev/null
}

echo "Verifying magic-link sign-in against $ORIGIN"
echo
echo "1. Enumeration oracle — a known and an unknown address must be indistinguishable"

psql "$DB" -qc "delete from app.login_token" >/dev/null 2>&1

known_res=$(post_login "$KNOWN")
unknown_res=$(post_login "$UNKNOWN")

known_status=$(echo "$known_res" | head -1 | awk '{print $2}')
unknown_status=$(echo "$unknown_res" | head -1 | awk '{print $2}')
# SvelteKit content-negotiates form actions: a non-browser POST gets a 200 with
# {"type":"redirect","location":...} in the body rather than a Location header.
extract_loc() {
	local loc
	loc=$(echo "$1" | grep -i '^location:' | tr -d '\r' | awk '{print $2}')
	if [[ -z "$loc" ]]; then
		loc=$(echo "$1" | grep -o '"location":"[^"]*"' | cut -d'"' -f4)
	fi
	echo "$loc"
}
known_loc=$(extract_loc "$known_res")
unknown_loc=$(extract_loc "$unknown_res")

check "same status code" "$known_status" "$unknown_status"
check "same redirect target" "$known_loc" "$unknown_loc"
check "redirect goes to check-email" "$known_loc" "/login/check-email"

known_body=$(echo "$known_res" | sed -n '/^\r$/,$p')
unknown_body=$(echo "$unknown_res" | sed -n '/^\r$/,$p')
if [[ "$known_body" == "$unknown_body" ]]; then ok "identical response body"; else bad "response bodies differ"; fi

check "exactly one token row exists (none for the unknown address)" "$(q 'select count(*) from app.login_token')" "1"
check "the token belongs to the known address" "$(q "select email from app.login_token")" "$KNOWN"

echo
echo "2. Case-insensitive matching"
psql "$DB" -qc "delete from app.login_token" >/dev/null 2>&1
upper=$(echo "$KNOWN" | tr '[:lower:]' '[:upper:]')
post_login "$upper" >/dev/null
check "an uppercased address resolves to the same member" "$(q 'select count(*) from app.login_token')" "1"
check "and is stored lowercased" "$(q 'select email from app.login_token')" "$KNOWN"

echo
echo "3. Single use — a link works once"
psql "$DB" -qc "delete from app.login_token" >/dev/null 2>&1
sleep 1
# Rate limit is per-process memory; restart-free, so mint the token directly.
raw=$(node -e "
import('./src/lib/server/login-token.js').catch(() => {});
const c = require('crypto');
const t = c.randomBytes(32).toString('base64url');
console.log(t + ' ' + c.createHash('sha256').update(t).digest('hex'));
")
token=$(echo "$raw" | cut -d' ' -f1)
hash=$(echo "$raw" | cut -d' ' -f2)
psql "$DB" -qc "insert into app.login_token (token_hash, email, purpose, expires_at)
	values ('$hash', '$KNOWN', 'login', now() + interval '15 minutes')" >/dev/null

first=$(curl -s -o /dev/null -w '%{http_code}' -c /tmp/league-cookies.txt "$ORIGIN/login/verify?token=$token")
check "first click is accepted (redirect)" "$first" "303"
check "consumed_at is now set" "$(q "select case when consumed_at is null then 'null' else 'set' end from app.login_token where token_hash='$hash'")" "set"

second=$(curl -s -o /dev/null -w '%{http_code}' "$ORIGIN/login/verify?token=$token")
check "second click is refused (no redirect)" "$second" "200"
check "still exactly one session" "$(q 'select count(*) from app.session')" "1"

echo
echo "4. Expiry"
exp_raw=$(node -e "
const c=require('crypto');const t=c.randomBytes(32).toString('base64url');
console.log(t+' '+c.createHash('sha256').update(t).digest('hex'));")
exp_token=$(echo "$exp_raw" | cut -d' ' -f1); exp_hash=$(echo "$exp_raw" | cut -d' ' -f2)
psql "$DB" -qc "insert into app.login_token (token_hash, email, purpose, expires_at)
	values ('$exp_hash', '$KNOWN', 'login', now() - interval '1 minute')" >/dev/null
check "an expired link is refused" \
	"$(curl -s -o /dev/null -w '%{http_code}' "$ORIGIN/login/verify?token=$exp_token")" "200"

echo
echo "5. Revocation — deactivating a member invalidates a link already in their inbox"
rev_raw=$(node -e "
const c=require('crypto');const t=c.randomBytes(32).toString('base64url');
console.log(t+' '+c.createHash('sha256').update(t).digest('hex'));")
rev_token=$(echo "$rev_raw" | cut -d' ' -f1); rev_hash=$(echo "$rev_raw" | cut -d' ' -f2)
psql "$DB" -qc "insert into app.login_token (token_hash, email, purpose, expires_at)
	values ('$rev_hash', '$KNOWN', 'login', now() + interval '15 minutes')" >/dev/null
psql "$DB" -qc "update app.league_member set active = false where email = '$KNOWN'" >/dev/null
sessions_before=$(q 'select count(*) from app.session')
check "the link is refused after deactivation" \
	"$(curl -s -o /dev/null -w '%{http_code}' "$ORIGIN/login/verify?token=$rev_token")" "200"
check "no new session was created" "$(q 'select count(*) from app.session')" "$sessions_before"
psql "$DB" -qc "update app.league_member set active = true where email = '$KNOWN'" >/dev/null

echo
echo "6. Route protection"
check "/settings redirects when signed out" \
	"$(curl -s -o /dev/null -w '%{http_code}' "$ORIGIN/settings")" "302"
check "/admin/members redirects when signed out" \
	"$(curl -s -o /dev/null -w '%{http_code}' "$ORIGIN/admin/members")" "302"
check "GET /api/rule-votes is gated" \
	"$(curl -s -o /dev/null -w '%{http_code}' "$ORIGIN/api/rule-votes")" "401"
check "the constitution text stays public" \
	"$(curl -s -o /dev/null -w '%{http_code}' "$ORIGIN/constitution")" "200"
check "the record book stays public" \
	"$(curl -s -o /dev/null -w '%{http_code}' "$ORIGIN/api/standings")" "200"

echo
echo "7. Rate limiting — runs last because it deliberately exhausts the budget"
psql "$DB" -qc "delete from app.login_token" >/dev/null 2>&1
for _ in 1 2 3 4 5 6 7; do post_login "$KNOWN" >/dev/null; done
tokens=$(q 'select count(*) from app.login_token')
if [[ "$tokens" -ge 1 && "$tokens" -le 5 ]]; then
	ok "capped at $tokens tokens from 7 attempts (limit 5)"
else
	bad "created $tokens tokens from 7 attempts, expected between 1 and 5"
fi

echo
echo "──────────────────────────────"
echo "$pass passed, $fail failed"
exit $(( fail > 0 ? 1 : 0 ))
