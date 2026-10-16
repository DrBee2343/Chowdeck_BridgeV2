#!/usr/bin/env python3
""" Chowdeck X (Twitter) Bridge - polls GetXAPI for new promo tweets and forwards the isolated code to the main bot's authenticated webhook. Rewrite notes ------------- * Authenticated forwarding: every request to the bot now carries the shared X-Webhook-Secret header. Without it the bot (correctly) rejects the call with 401 - that mismatch was why the two services were "not communicating". * One long-lived httpx.AsyncClient with keep-alive connection pooling instead of a fresh client per loop iteration (removes a TCP/TLS handshake every cycle). * Poll interval is configurable, jittered and adaptive: fast while a burst of new tweets is arriving, slower when idle, so we surface codes with low latency without hammering the upstream API. * Bounded, de-duplicated tweet tracking (ring buffer) so memory cannot grow forever during long runs. * GetXAPI key rotation with per-key backoff; a transient 5xx/network error no longer burns a key. * Graceful shutdown on SIGTERM/SIGINT (Railway sends SIGTERM on redeploy). * Optional start-up baseline so a redeploy does not re-forward old codes. """

import asyncio
import os
import random
import re
import signal
import time
from collections import deque

import httpx

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
RAILWAY_WEBHOOK_URL = os.getenv(
    "RAILWAY_WEBHOOK_URL", "https://chow-bot-production.up.railway.app/tweet"
)
STATUS_URL = os.getenv(
    "STATUS_URL", "https://chow-bot-production.up.railway.app/status"
)

# Shared secret with the bot. Must equal WEBHOOK_SECRET on the bot service.
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()

RAW_KEYS = os.getenv("TWITTER_API_KEYS", "")
API_KEYS = [k.strip() for k in RAW_KEYS.split(",") if k.strip()]

TARGET_USER_NAME = os.getenv("TARGET_USER_NAME", "Oyinlola6464")
GETXAPI_URL = os.getenv("GETXAPI_URL", "https://api.getxapi.com/twitter/user/tweets")

# Polling / latency tuning.
POLL_ACTIVE_SECONDS = float(os.getenv("POLL_ACTIVE_SECONDS", "3.0"))
POLL_IDLE_SECONDS = float(os.getenv("POLL_IDLE_SECONDS", "6.0"))
POLL_JITTER = float(os.getenv("POLL_JITTER", "0.75"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "10.0"))
SEEN_MAXLEN = max(100, int(os.getenv("SEEN_MAXLEN", "800")))
BASELINE_ON_START = os.getenv("BASELINE_ON_START", "true").strip().lower() in (
    "1", "true", "yes", "on",
)

# Promo-code extraction: tokens that look like codes (upper-case + digits) are
# preferred; a bare word is only used as a last resort.
CODE_PATTERN = re.compile(r"\b[A-Z0-9]{4,12}\b")
MIN_CODE_LEN = int(os.getenv("MIN_CODE_LEN", "4"))

_shutdown = asyncio.Event()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def extract_code(text):
    """Return the best promo-code candidate from a tweet body, or ''."""
    if not text:
        return ""
    candidates = CODE_PATTERN.findall(text.upper())
    # Prefer mixed alpha+digit tokens (e.g. CHOW5K); they are almost always the
    # real promo code rather than an ordinary word or a hashtag.
    mixed = [c for c in candidates if any(ch.isdigit() for ch in c) and any(ch.isalpha() for ch in c)]
    if mixed:
        return max(mixed, key=len)
    if candidates:
        return max(candidates, key=len)
    return ""


async def send_admin_alert(client, text):
    if not TELEGRAM_BOT_TOKEN or not ADMIN_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        await client.post(url, json={"chat_id": int(ADMIN_CHAT_ID), "text": text, "parse_mode": "Markdown"})
    except Exception as e:  # noqa: BLE001
        print(f"[bridge] telegram alert failed: {e}", flush=True)


def _auth_headers():
    headers = {}
    if WEBHOOK_SECRET:
        headers["X-Webhook-Secret"] = WEBHOOK_SECRET
        headers["Authorization"] = f"Bearer {WEBHOOK_SECRET}"
    return headers


async def bot_is_active(client):
    """Return the bot's active flag. Defaults to True (fail-open for polling)."""
    try:
        res = await client.get(STATUS_URL, headers=_auth_headers(), timeout=REQUEST_TIMEOUT)
        if res.status_code == 401:
            print("[bridge] /status returned 401 - WEBHOOK_SECRET mismatch with the bot.", flush=True)
            return True
        if res.status_code == 200:
            return bool(res.json().get("active", True))
    except Exception:
        pass
    return True


async def forward_code(client, code):
    try:
        res = await client.post(
            RAILWAY_WEBHOOK_URL,
            json={"text": code},
            headers=_auth_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        if res.status_code == 401:
            print("[bridge] /tweet returned 401 - WEBHOOK_SECRET mismatch with the bot.", flush=True)
            return False
        if res.status_code >= 400:
            print(f"[bridge] forward failed [{res.status_code}]: {res.text[:200]}", flush=True)
            return False
        print(f"[bridge] forwarded code {code!r} -> {res.status_code}", flush=True)
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[bridge] forward error: {e}", flush=True)
        return False


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
async def poll_loop(client, key_state):
    params = {"userName": TARGET_USER_NAME}
    seen = deque(maxlen=SEEN_MAXLEN)
    seen_set = set()
    baseline_done = False
    last_new = 0.0

    while not _shutdown.is_set():
        if not await bot_is_active(client):
            print("[bridge] bot paused by admin - waiting 20s.", flush=True)
            await _sleep_or_stop(20)
            continue

        if not API_KEYS:
            print("[bridge] no GetXAPI keys configured - waiting 30s.", flush=True)
            await _sleep_or_stop(30)
            continue

        key_index = key_state["index"] % len(API_KEYS)
        current_key = API_KEYS[key_index]
        headers = {"Authorization": f"Bearer {current_key}"}

        exhausted = False
        reason = "unknown"
        try:
            res = await client.get(GETXAPI_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            reason = f"HTTP {res.status_code}"

            if res.status_code in (401, 403, 402):
                exhausted = True
            elif res.status_code == 429:
                exhausted = True
                reason = "HTTP 429 (rate limited)"
            elif res.status_code >= 500:
                # Transient upstream error: do NOT burn the key, just back off.
                print(f"[bridge] upstream {res.status_code} - backing off.", flush=True)
                await _sleep_or_stop(POLL_IDLE_SECONDS + 3)
                continue
            elif res.status_code == 200:
                try:
                    body = res.json()
                except Exception:
                    body = {}
                body_str = res.text.lower()
                if isinstance(body, dict) and body.get("error"):
                    exhausted = True
                    reason = "API error in body"
                elif any(w in body_str for w in ("quota", "exhausted", "out of credits")):
                    exhausted = True
                    reason = "quota/credits exhausted in body"

                if not exhausted:
                    tweets = body.get("tweets", []) if isinstance(body, dict) else []

                    if not baseline_done:
                        baseline_done = True
                        if BASELINE_ON_START:
                            for tw in tweets:
                                tid = str(tw.get("id") or "")
                                if tid:
                                    if tid not in seen_set:
                                        seen.append(tid)
                                        seen_set.add(tid)
                            print(f"[bridge] baseline locked: {len(seen_set)} known tweet(s).", flush=True)
                            await _sleep_or_stop(POLL_ACTIVE_SECONDS)
                            continue

                    for tw in tweets:
                        tid = str(tw.get("id") or "")
                        text = str(tw.get("text") or "")
                        if not tid or not text or tid in seen_set:
                            continue
                        if len(seen) == seen.maxlen:
                            old = seen.popleft()
                            seen_set.discard(old)
                        seen.append(tid)
                        seen_set.add(tid)

                        code = extract_code(text) or text.strip()
                        if len(code) < MIN_CODE_LEN:
                            continue
                        print(f"[bridge] new tweet {tid} -> code {code!r}", flush=True)
                        await forward_code(client, code)
                        last_new = time.monotonic()

        except (httpx.RequestError, OSError) as e:
            print(f"[bridge] network error: {e}", flush=True)
            await _sleep_or_stop(3)
            continue
        except Exception as e:  # noqa: BLE001
            print(f"[bridge] polling error: {e}", flush=True)

        if exhausted:
            failed_key_num = key_index + 1
            key_state["index"] = key_index + 1
            if key_state["index"] < len(API_KEYS):
                await send_admin_alert(
                    client,
                    f"âš ï¸ *GetXAPI Key #{failed_key_num} Exhausted/Invalid!*\n"
                    f"Reason: {reason}. Rotating to key #{key_state['index'] + 1}.",
                )
            else:
                await send_admin_alert(
                    client,
                    f"âŒ *CRITICAL: All GetXAPI Keys Failed!*\nReason: {reason}\n"
                    "ðŸš¨ Please check your keys immediately.",
                )
            await _sleep_or_stop(3)
            continue

        # Adaptive cadence: quick right after new content, relaxed when idle.
        recent = (time.monotonic() - last_new) < 15
        base = POLL_ACTIVE_SECONDS if recent else POLL_IDLE_SECONDS
        await _sleep_or_stop(base + random.uniform(0, POLL_JITTER))


async def _sleep_or_stop(seconds):
    try:
        await asyncio.wait_for(_shutdown.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


def _install_signal_handlers():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _shutdown.set)
        except NotImplementedError:
            pass


async def main():
    _install_signal_handlers()
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    timeout = httpx.Timeout(REQUEST_TIMEOUT, connect=5.0)
    print(f"[bridge] active for @{TARGET_USER_NAME} | keys={len(API_KEYS)} "
          f"| secret={'set' if WEBHOOK_SECRET else 'MISSING'}", flush=True)
    if not WEBHOOK_SECRET:
        print("[bridge] WARNING: WEBHOOK_SECRET is not set - the bot will reject "
              "forwarded codes with 401.", flush=True)

    key_state = {"index": 0}
    async with httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=True) as client:
        await poll_loop(client, key_state)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[bridge] stopped.", flush=True)
