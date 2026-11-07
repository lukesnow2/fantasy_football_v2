"""A league-season's full Yahoo settings: format, scoring rules, bonuses, lineup.

Yahoo's league settings response is the only record of how a season was
played: points per stat, yardage bonuses, lineup slots, playoff field,
waiver and trade rules. The scoring changed more than once in this league's
history (receptions scored from 2008, six-point passing touchdowns from
2025), so comparing seasons without it compares different games.

parse_league_settings turns the raw response (``league/{key}/settings``) into
plain rows. It is pure, so it is tested against Yahoo's real shapes without a
network. The response also carries things that must never be stored (the join
password, invitation keys, chat channel ids); only the fields named here are
kept.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _int(value) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _num(value) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _flag(value) -> Optional[bool]:
    """Yahoo sends booleans as 1/0, '1'/'0' or true/false; absent is unknown."""
    if value is None or value == '':
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip() == '1'


def _date(value) -> Optional[str]:
    try:
        return datetime.strptime(str(value), '%Y-%m-%d').date().isoformat()
    except ValueError:
        return None


def _text(value) -> Optional[str]:
    return str(value) if value not in (None, '') else None


def _as_list(value) -> list:
    """Yahoo sends a one-item collection as the item itself."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _previous_league_id(renew) -> Optional[str]:
    """'461_654923' is last season's league, 461.l.654923."""
    parts = str(renew or '').split('_')
    if len(parts) == 2 and all(p.isdigit() for p in parts):
        return f'{parts[0]}.l.{parts[1]}'
    return None


def _draft_time(value) -> Optional[datetime]:
    seconds = _int(value)
    return datetime.fromtimestamp(seconds, tz=timezone.utc) if seconds else None


def split_settings_response(raw: Dict[str, Any]) -> tuple:
    """(league metadata, settings) dicts out of the raw response."""
    league = raw.get('fantasy_content', {}).get('league')
    if not isinstance(league, list) or len(league) < 2:
        raise ValueError('settings response has no league node')
    meta = league[0] if isinstance(league[0], dict) else {}
    settings = league[1].get('settings') if isinstance(league[1], dict) else None
    settings = settings[0] if isinstance(settings, list) and settings else settings
    if not isinstance(settings, dict):
        raise ValueError('settings response has no settings node')
    return meta, settings


def parse_league_settings(raw: Dict[str, Any], league_id: str) -> Dict[str, Any]:
    """Raw settings response -> {'league', 'scoring', 'bonuses', 'roster_slots'}.

    'league' holds the scalar settings as public.leagues columns. The three
    lists are rows for public.league_scoring, league_scoring_bonuses and
    league_roster_slots. A value Yahoo did not send is None, never a guess.
    """
    meta, s = split_settings_response(raw)
    uses_playoff = _flag(s.get('uses_playoff'))
    playoff = uses_playoff is not False

    league = {
        'start_date': _date(meta.get('start_date')),
        'end_date': _date(meta.get('end_date')),
        'previous_league_id': _previous_league_id(meta.get('renew')),
        'scoring_type': _text(s.get('scoring_type')),
        'draft_type': _text(s.get('draft_type')),
        'is_auction_draft': _flag(s.get('is_auction_draft')),
        'draft_time': _draft_time(s.get('draft_time')),
        'uses_playoff': uses_playoff,
        'playoff_start_week': (_int(s.get('playoff_start_week')) or None) if playoff else None,
        'num_playoff_teams': (_int(s.get('num_playoff_teams')) or None) if playoff else None,
        'num_playoff_consolation_teams': _int(s.get('num_playoff_consolation_teams')) if playoff else None,
        'uses_playoff_reseeding': _flag(s.get('uses_playoff_reseeding')),
        'has_multiweek_championship': _flag(s.get('has_multiweek_championship')),
        'uses_median_score': _flag(s.get('uses_median_score')),
        'waiver_type': _text(s.get('waiver_type')),
        'waiver_rule': _text(s.get('waiver_rule')),
        'waiver_time': _int(s.get('waiver_time')),
        'uses_faab': _flag(s.get('uses_faab')),
        'trade_end_date': _date(s.get('trade_end_date')),
        'trade_ratify_type': _text(s.get('trade_ratify_type')),
        'trade_reject_time': _int(s.get('trade_reject_time')),
        'post_draft_players': _text(s.get('post_draft_players')),
        'uses_fractional_points': _flag(s.get('uses_fractional_points')),
        'uses_negative_points': _flag(s.get('uses_negative_points')),
    }

    categories = {}
    for order, item in enumerate(_as_list(s.get('stat_categories', {}).get('stats'))):
        stat = item.get('stat', {}) if isinstance(item, dict) else {}
        stat_id = _int(stat.get('stat_id'))
        if stat_id is not None:
            categories[stat_id] = (order, stat)

    modifiers = {}
    bonuses = []
    for item in _as_list(s.get('stat_modifiers', {}).get('stats')):
        stat = item.get('stat', {}) if isinstance(item, dict) else {}
        stat_id = _int(stat.get('stat_id'))
        if stat_id is None:
            continue
        modifiers[stat_id] = _num(stat.get('value'))
        for b in _as_list(stat.get('bonuses')):
            bonus = b.get('bonus', {}) if isinstance(b, dict) else {}
            target, points = _num(bonus.get('target')), _num(bonus.get('points'))
            if target is not None and points is not None:
                bonuses.append({'league_id': league_id, 'stat_id': stat_id,
                                'target': target, 'points': points})

    scoring = []
    # Every stat the league tracks, scored or not, plus any modifier Yahoo
    # sends for a stat missing from the category list (kept, not dropped).
    for stat_id in sorted(set(categories) | set(modifiers),
                          key=lambda i: (categories.get(i, (10**6,))[0], i)):
        _, stat = categories.get(stat_id, (None, {}))
        scoring.append({
            'league_id': league_id,
            'stat_id': stat_id,
            'stat_name': _text(stat.get('name')),
            'display_name': _text(stat.get('display_name')),
            'stat_group': _text(stat.get('group')),
            'position_type': _text(stat.get('position_type')),
            'points': modifiers.get(stat_id),
            'is_display_only': _flag(stat.get('is_only_display_stat')) or False,
            'sort_order': categories.get(stat_id, (None,))[0],
        })

    roster_slots = []
    for order, item in enumerate(_as_list(s.get('roster_positions'))):
        pos = item.get('roster_position', {}) if isinstance(item, dict) else {}
        if not pos.get('position'):
            continue
        roster_slots.append({
            'league_id': league_id,
            'position': pos['position'],
            'position_type': _text(pos.get('position_type')),
            'slot_count': _int(pos.get('count')) or 0,
            'is_starting': _flag(pos.get('is_starting_position')),
            'sort_order': order,
        })

    return {'league': league, 'scoring': scoring, 'bonuses': bonuses,
            'roster_slots': roster_slots}
