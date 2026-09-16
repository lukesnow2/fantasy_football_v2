#!/usr/bin/env python3
"""Reliable HTTP layer for the Yahoo Fantasy API.

A YHandler subclass injected via ``game.inject_yhandler()`` — the library's
supported extension point. Every read in yahoo_fantasy_api routes through
``YHandler.get`` (verified: all 18 *_raw methods), so this single override
gives the whole pipeline:

  - a request timeout (the historical extractor had none; hung sockets
    stalled sessions indefinitely)
  - status-aware retries with backoff + jitter, honoring Retry-After
  - Yahoo "Request denied" rate-limit rejections retried with escalating
    cooldowns instead of aborting the week (117 such denials silently
    skipped statistics weeks during the historical load)
  - honest request pacing: the pacer is charged per REAL HTTP request,
    here at the choke point, so library fan-out (player_stats chunks of
    25) can never under-count again
  - the in-library token refresh from yahoo_fantasy_api >= 2.12.1
    (refresh + session rebuild + retry), which this subclass inherits

Failures raise YahooApiError with the real status code attached — never
None, never a silently empty result.
"""
import json
import logging
import random
import time

from yahoo_fantasy_api import yhandler

logger = logging.getLogger(__name__)

YAHOO_ENDPOINT = yhandler.YAHOO_ENDPOINT

# Retry ladders (seconds). Jitter of +/-25% is applied to each sleep.
SERVER_ERROR_BACKOFF = [5, 20, 60]          # 500/502/504 and timeouts
RATE_LIMIT_BACKOFF = [60, 300, 900]         # "Request denied" / 999
RETRY_AFTER_CAP = 300                        # never sleep longer than this on a header

REQUEST_TIMEOUT = 30


class YahooApiError(RuntimeError):
    """A Yahoo API request failed after all retries.

    Carries the HTTP status code (when one was received) and the response
    body so callers can classify without substring-sniffing str(e).
    """

    def __init__(self, message, status_code=None, body=None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class RequestPacer:
    """Paces and counts real HTTP requests.

    Lives at the HTTP choke point so every request — including library
    fan-out — is spaced and counted. Replaces the extractor-level pacing
    that under-counted bulk calls ~8x.
    """

    def __init__(self, min_interval=1.5, max_per_hour=900):
        self.min_interval = min_interval
        self.max_per_hour = max_per_hour
        self.request_count = 0
        self._hour_started = time.monotonic()
        self._hour_count = 0
        self._last_request = 0.0

    def before_request(self):
        now = time.monotonic()

        # Roll the hourly window.
        if now - self._hour_started >= 3600:
            self._hour_started = now
            self._hour_count = 0

        # If we are at the hourly budget, sleep out the window remainder.
        if self._hour_count >= self.max_per_hour:
            wait = 3600 - (now - self._hour_started)
            if wait > 0:
                logger.warning(
                    "Hourly request budget (%d) reached; sleeping %.0fs",
                    self.max_per_hour, wait)
                time.sleep(wait)
            self._hour_started = time.monotonic()
            self._hour_count = 0

        elapsed = time.monotonic() - self._last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

        self._last_request = time.monotonic()
        self._hour_count += 1
        self.request_count += 1
        if self.request_count % 25 == 0:
            logger.info("HTTP requests this run: %d", self.request_count)


def _sleep_with_jitter(seconds):
    time.sleep(seconds * random.uniform(0.75, 1.25))


def _is_rate_denial(response):
    """Yahoo's rate-limit rejection: status 999 or a 'Request denied' body."""
    if response.status_code == 999:
        return True
    try:
        return b'Request denied' in response.content
    except Exception:
        return False


def _retry_after_seconds(response, fallback):
    value = response.headers.get('Retry-After') if response.headers else None
    if value:
        try:
            return min(int(value), RETRY_AFTER_CAP)
        except ValueError:
            pass
    return fallback


class ReliableYHandler(yhandler.YHandler):
    """YHandler with timeouts, status-aware retries, and request pacing."""

    def __init__(self, sc, pacer=None):
        super().__init__(sc)
        self.pacer = pacer or RequestPacer()

    def _request_once(self, full_url, params):
        """One paced HTTP GET with a timeout. Returns the response.

        requests-level exceptions (connect/read timeouts, connection
        resets) are surfaced as None with the exception attached so the
        retry loop can treat them like a 5xx.
        """
        self.pacer.before_request()
        return self.sc.session.get(full_url, params=params,
                                   timeout=REQUEST_TIMEOUT)

    def get(self, uri):
        full_url = "{}/{}".format(YAHOO_ENDPOINT, uri)
        params = {'format': 'json'}

        server_error_attempt = 0
        denial_attempt = 0
        last_status = None
        last_body = None

        while True:
            try:
                response = self._request_once(full_url, params)
            except Exception as exc:
                # Timeout / connection error: retry on the 5xx ladder.
                if server_error_attempt < len(SERVER_ERROR_BACKOFF):
                    delay = SERVER_ERROR_BACKOFF[server_error_attempt]
                    server_error_attempt += 1
                    logger.warning(
                        "Request error (%s); retry %d in ~%ds: %s",
                        type(exc).__name__, server_error_attempt, delay, uri)
                    _sleep_with_jitter(delay)
                    continue
                raise YahooApiError(
                    "Request failed after {} retries: {}: {}".format(
                        len(SERVER_ERROR_BACKOFF), type(exc).__name__, exc)
                ) from exc

            last_status = response.status_code
            last_body = response.content[:500] if response.content else b''

            # Expired token: the upstream library's refresh knows how to
            # rebuild the session; reuse it, then re-check the response.
            if self._is_token_expired_error(response):
                response = self._refresh_token_and_retry(
                    'get', full_url, params=params)
                last_status = response.status_code
                last_body = response.content[:500] if response.content else b''

            if response.status_code == 200:
                try:
                    return response.json()
                except (json.JSONDecodeError, ValueError) as exc:
                    raise YahooApiError(
                        "Yahoo returned 200 with an unparseable body",
                        status_code=200, body=last_body) from exc

            # Rate-limit denial: escalating cooldown, then retry.
            if _is_rate_denial(response):
                if denial_attempt < len(RATE_LIMIT_BACKOFF):
                    delay = _retry_after_seconds(
                        response, RATE_LIMIT_BACKOFF[denial_attempt])
                    denial_attempt += 1
                    logger.warning(
                        "Yahoo rate limit (status %s); cooldown %d of %d, ~%ds: %s",
                        response.status_code, denial_attempt,
                        len(RATE_LIMIT_BACKOFF), delay, uri)
                    _sleep_with_jitter(delay)
                    continue
                raise YahooApiError(
                    "Yahoo rate limit persisted through {} cooldowns".format(
                        len(RATE_LIMIT_BACKOFF)),
                    status_code=response.status_code, body=last_body)

            # Retryable server-side errors.
            if response.status_code in (429, 500, 502, 503, 504):
                if server_error_attempt < len(SERVER_ERROR_BACKOFF):
                    delay = _retry_after_seconds(
                        response, SERVER_ERROR_BACKOFF[server_error_attempt])
                    server_error_attempt += 1
                    logger.warning(
                        "Yahoo %s; retry %d in ~%ds: %s",
                        response.status_code, server_error_attempt, delay, uri)
                    _sleep_with_jitter(delay)
                    continue

            # Anything else (or retries exhausted): raise with the facts.
            raise YahooApiError(
                "Yahoo API error {} for {}".format(response.status_code, uri),
                status_code=response.status_code, body=last_body)
