"""Phase 1 tests: ReliableYHandler retry/timeout/pacing behavior (no network)."""
import json
from unittest.mock import Mock

import pytest

from src.extractors import yahoo_http
from src.extractors.yahoo_http import (
    ReliableYHandler, RequestPacer, YahooApiError,
    SERVER_ERROR_BACKOFF, RATE_LIMIT_BACKOFF,
)


class FakeResponse:
    def __init__(self, status_code=200, body=b'{"ok": 1}', headers=None):
        self.status_code = status_code
        self.content = body
        self.headers = headers or {}

    def json(self):
        return json.loads(self.content.decode('utf-8'))


class FakeSession:
    """Yields scripted responses; raising entries are raised instead."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        assert timeout is not None, "every request must carry a timeout"
        self.calls += 1
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class NullPacer(RequestPacer):
    def before_request(self):
        self.request_count += 1


def make_handler(script):
    sc = Mock()
    sc.session = FakeSession(script)
    return ReliableYHandler(sc, pacer=NullPacer())


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    sleeps = []
    monkeypatch.setattr(yahoo_http.time, 'sleep', sleeps.append)
    return sleeps


def test_success_returns_json():
    h = make_handler([FakeResponse(200, b'{"fantasy_content": {}}')])
    assert h.get('game/nfl') == {'fantasy_content': {}}


def test_5xx_ladder_then_success(no_sleep):
    h = make_handler([FakeResponse(502, b'bad gateway'),
                      FakeResponse(504, b'timeout'),
                      FakeResponse(200, b'{"ok": 1}')])
    assert h.get('league/x') == {'ok': 1}
    assert h.sc.session.calls == 3
    assert len(no_sleep) == 2  # two backoff sleeps


def test_5xx_exhausted_raises_with_status():
    script = [FakeResponse(500, b'err')] * (len(SERVER_ERROR_BACKOFF) + 1)
    h = make_handler(script)
    with pytest.raises(YahooApiError) as ei:
        h.get('league/x')
    assert ei.value.status_code == 500
    assert ei.value.body == b'err'


def test_retry_after_header_is_honored(no_sleep):
    h = make_handler([
        FakeResponse(429, b'slow down', headers={'Retry-After': '7'}),
        FakeResponse(200, b'{"ok": 1}'),
    ])
    assert h.get('league/x') == {'ok': 1}
    # jittered 7s sleep: within +/-25%
    assert 7 * 0.75 <= no_sleep[0] <= 7 * 1.25


def test_rate_denial_cooldown_then_success(no_sleep):
    h = make_handler([FakeResponse(999, b'Request denied\r\n'),
                      FakeResponse(200, b'{"ok": 1}')])
    assert h.get('league/x') == {'ok': 1}
    assert RATE_LIMIT_BACKOFF[0] * 0.75 <= no_sleep[0] <= RATE_LIMIT_BACKOFF[0] * 1.25


def test_rate_denial_by_body_not_status():
    h = make_handler([FakeResponse(200 + 799, b'Request denied\r\n'),
                      FakeResponse(200, b'{"ok": 1}')])
    assert h.get('league/x') == {'ok': 1}


def test_rate_denial_exhausted_raises():
    script = [FakeResponse(999, b'Request denied\r\n')] * (len(RATE_LIMIT_BACKOFF) + 1)
    h = make_handler(script)
    with pytest.raises(YahooApiError) as ei:
        h.get('league/x')
    assert 'rate limit' in str(ei.value).lower()
    assert ei.value.status_code == 999


def test_connection_error_retried_then_raised():
    script = [ConnectionError('reset')] * (len(SERVER_ERROR_BACKOFF) + 1)
    h = make_handler(script)
    with pytest.raises(YahooApiError) as ei:
        h.get('league/x')
    assert 'ConnectionError' in str(ei.value)


def test_bad_json_on_200_raises():
    h = make_handler([FakeResponse(200, b'<html>not json</html>')])
    with pytest.raises(YahooApiError) as ei:
        h.get('league/x')
    assert ei.value.status_code == 200


def test_token_expired_delegates_to_library_refresh(monkeypatch):
    h = make_handler([FakeResponse(401, b'"token_expired"')])
    monkeypatch.setattr(h, '_refresh_token_and_retry',
                        lambda *a, **k: FakeResponse(200, b'{"ok": 1}'))
    assert h.get('league/x') == {'ok': 1}


def test_every_http_call_is_charged_to_pacer():
    h = make_handler([FakeResponse(503, b'x'), FakeResponse(200, b'{"ok": 1}')])
    assert h.get('league/x') == {'ok': 1}
    assert h.pacer.request_count == 2  # real HTTP calls, not logical calls


def test_pacer_enforces_min_interval(monkeypatch):
    clock = {'now': 0.0}
    sleeps = []
    monkeypatch.setattr(yahoo_http.time, 'monotonic', lambda: clock['now'])
    monkeypatch.setattr(yahoo_http.time, 'sleep', sleeps.append)
    p = RequestPacer(min_interval=1.5)
    p.before_request()          # first call: no wait
    clock['now'] += 0.5
    p.before_request()          # 0.5s elapsed -> must sleep ~1.0s
    assert sleeps and abs(sleeps[-1] - 1.0) < 1e-6
    assert p.request_count == 2
