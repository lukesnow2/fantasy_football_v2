"""FAAB bids must survive transaction parsing.

Yahoo puts faab_bid on the transaction, beside type/status/timestamp, not in
each player's transaction_data. The parser only looked at the player, so
every bid ever loaded was NULL and FAAB Efficiency read 0.0 for everyone.
Checked against Yahoo on 2026-11-06: 71 of 2025's 367 add/drop transactions
carry a bid, and every team's bids plus its remaining balance come to $100.
"""
from types import SimpleNamespace

from src.extractors.comprehensive_data_extractor import YahooFantasyExtractor


def player(key, name, move, team=None):
    data = {'type': move}
    if move == 'add':
        data.update(destination_type='team', destination_team_key=team)
    else:
        data.update(source_type='team', source_team_key=team)
    return {'player': [[{'player_key': key}, {'name': {'full': name}}],
                       {'transaction_data': [data]}]}


# Shaped like _fetch_transactions' merged dicts for a winning waiver claim.
CLAIM = {
    'transaction_key': '461.l.1.tr.7', 'type': 'add/drop',
    'status': 'successful', 'timestamp': '1766866078', 'faab_bid': '26',
    'players': {
        '0': player('461.p.1', 'Claimed Back', 'add', '461.l.1.t.3'),
        '1': player('461.p.2', 'Dropped Receiver', 'drop', '461.l.1.t.3'),
        'count': 2,
    },
}
FREE_AGENT = {
    'transaction_key': '461.l.1.tr.8', 'type': 'add',
    'status': 'successful', 'timestamp': '1766866099',
    'players': {'0': player('461.p.3', 'Free Agent', 'add', '461.l.1.t.4'),
                'count': 1},
}


def extract(transactions):
    ex = YahooFantasyExtractor.__new__(YahooFantasyExtractor)
    ex.game = SimpleNamespace(to_league=lambda league_id: SimpleNamespace())
    ex._rate_limited_request = lambda fn: fn()
    ex._fetch_transactions = lambda league, types, count: (
        transactions if types == 'add,drop' else [])
    return {t.player_name: t.faab_bid for t in ex.extract_transactions_for_league('461.l.1')}


def test_bid_goes_to_the_claimed_player_only():
    bids = extract([CLAIM])
    assert bids == {'Claimed Back': 26.0, 'Dropped Receiver': None}


def test_free_agent_add_has_no_bid():
    assert extract([FREE_AGENT]) == {'Free Agent': None}
