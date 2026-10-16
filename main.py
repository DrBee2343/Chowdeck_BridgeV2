#!/usr/bin/env python3
""" Chowdeck X (Twitter) Bridge =========================== Polls GetXAPI for new promo tweets and forwards every isolated promo code to the main bot's authenticated webhook (POST /tweet). This revision fixes the pause/resume behaviour and makes key handling bulletproof ------------------------------------------------------------------------------ * PAUSE / RESUME reacts in ~1-2 seconds. The bot's /status is read by a small background monitor task, never on the detection critical path, so toggling X detection in Telegram takes effect almost instantly and detection latency is unaffected. Logs and admin notices are emitted only on a real state change (no more repeated "waiting 20s" spam). * GETXAPI KEY POOL with per-key state. A dead key goes on a cooldown, rotation is immediate (no sleep), and the admin is told which key failed, why, and which key took over - exactly like the original, but without alert flooding. * Transient upstream failures (5xx / network) never burn a key: bounded exponential backoff with jitter. * 429 is treated as a temporary throttle (cooldown), not a permanent death. * Multi-code tweets: every promo code in the tweet is forwarded, not just the first. * Everything is bounded: de-duplicated ring buffer, capped fan-out, per-request timeouts, no unbounded retry loops. A bad response never crashes the process. """

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
).strip()
STATUS_URL = os.getenv(
    "STATUS_URL", "https://chow-bot-production.up.railway.app/status"
).strip()

# Shared secret with the bot. Must equal WEBHOOK_SECRET on the bot service.
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()

RAW_KEYS = os.getenv("TWITTER_API_KEYS", "")
API_KEYS = [k.strip() for k in RAW_KEYS.split(",") if k.strip()]

TARGET_USER_NAME = os.getenv("TARGET_USER_NAME", "Oyinlola6464").strip()
GETXAPI_URL = os.getenv(
    "GETXAPI_URL", "https://api.getxapi.com/twitter/user/tweets"
).strip()

# Detection cadence.
POLL_ACTIVE_SECONDS = float(os.getenv("POLL_ACTIVE_SECONDS", "3.0"))
POLL_IDLE_SECONDS = float(os.getenv("POLL_IDLE_SECONDS", "6.0"))
POLL_JITTER = float(os.getenv("POLL_JITTER", "0.75"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "10.0"))
SEEN_MAXLEN = max(100, int(os.getenv("SEEN_MAXLEN", "800")))
BASELINE_ON_START = os.getenv("BASELINE_ON_START", "true").strip().lower() in (
    "1", "true", "yes", "on",
)

# Pause/resume responsiveness.
STATUS_CHECK_SECONDS = float(os.getenv("STATUS_CHECK_SECONDS", "3.0"))
PAUSED_CHECK_SECONDS = float(os.getenv("PAUSED_CHECK_SECONDS", "2.0"))
NOTIFY_ON_PAUSE = os.getenv("NOTIFY_ON_PAUSE", "true").strip().lower() in (
    "1", "true", "yes", "on",
)
ALERTS_ON_START = os.getenv("ALERTS_ON_START", "true").strip().lower() in (
    "1", "true", "yes", "on",
)

# Key rotation / alerting.
KEY_COOLDOWN_SECONDS = float(os.getenv("KEY_COOLDOWN_SECONDS", "900"))
ALL_KEYS_ALERT_COOLDOWN = float(os.getenv("ALL_KEYS_ALERT_COOLDOWN", "1800"))
UPSTREAM_BACKOFF_MAX = float(os.getenv("UPSTREAM_BACKOFF_MAX", "60"))

# Promo-code extraction. Chowdeck codes (e.g. CHOW5K, OYIN5K, ABC12) mix letters
# and digits, so that is the default requirement - it keeps ordinary English
# words out of the claim pipeline. Set CODE_REQUIRE_MIXED=false to accept any
# alphanumeric token >= MIN_CODE_LEN instead.
CODE_TOKEN = re.compile(r"[A-Za-z0-9]{4,20}")
URL_OR_HANDLE = re.compile(r"(https?://\S+|www\.\S+|[#@]\w+)", re.IGNORECASE)
CODE_REQUIRE_MIXED = os.getenv("CODE_REQUIRE_MIXED", "true").strip().lower() in (
    "1", "true", "yes", "on",
)
# Minimum promo-code length. Keep at 5 to match the bot's X-fed minimum so
# nothing the bot accepts is silently dropped here.
MIN_CODE_LEN = int(os.getenv("MIN_CODE_LEN", "5"))
MAX_CODES_PER_TWEET = int(os.getenv("MAX_CODES_PER_TWEET", "4"))

_shutdown = asyncio.Event()
_client = None

# Shared bot state, refreshed by status_monitor() and read by poll_loop().
_bot = {"active": True, "reachable": False}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def extract_codes(text):
    """Return every promo-code candidate in a tweet body (deduped, ordered). Every code found in the tweet is returned, not just the first, so multi-code tweets are fully forwarded. Ordinary words are excluded unless the whole tweet is essentially just the code (short-tweet fallback). """
    if not text:
        return []

    cleaned = URL_OR_HANDLE.sub(" ", text)
    tokens = []
    for t in CODE_TOKEN.findall(cleaned.upper()):
        if t not in tokens:
            tokens.append(t)
    if not tokens:
        return []

    def _is_mixed(tok):
        return any(ch.isdigit() for ch in tok) and any(ch.isalpha() for ch in tok)

    if CODE_REQUIRE_MIXED:
        chosen = [t for t in tokens if _is_mixed(t)]
    else:
        chosen = list(tokens)

    if not chosen:
        # Last resort: a short tweet that is essentially just the code itself
        # (covers a bare code tweet that has no digit).
        stripped = re.sub(r"\s+", " ", cleaned.strip())
        if len(tokens) == 1 and 0 < len(stripped) <= 60:
            chosen = tokens
        else:
            return []

    out = []
    for tok in chosen:
        if len(tok) >= MIN_CODE_LEN and tok not in out:
            out.append(tok)
    return out[:MAX_CODES_PER_TWEET]


def _mask(key):
    """Non-reversible fingerprint so an alert identifies the key safely."""
    if len(key) >= 6:
        return "\u2026" + key[-4:]
    return "\u2026" + "*" * max(2, len(key))


def _backoff(streak):
    """Bounded exponential backoff with jitter (seconds)."""
    base = min(UPSTREAM_BACKOFF_MAX, float(2 ** min(max(streak, 1), 6)))
    return base + random.uniform(0.0, 1.5)


def _auth_headers():
    headers = {}
    if WEBHOOK_SECRET:
        headers["X-Webhook-Secret"] = WEBHOOK_SECRET
        headers["Authorization"] = f"Bearer {WEBHOOK_SECRET}"
    return headers


async def alert(text):
    """Best-effort Telegram alert to the admin. Never raises, never blocks long."""
    if not TELEGRAM_BOT_TOKEN or not ADMIN_CHAT_ID or _client is None:
        return
    try:
        chat_id = int(ADMIN_CHAT_ID)
    except (TypeError, ValueError):
        print(f"[bridge] ADMIN_CHAT_ID is not numeric ({ADMIN_CHAT_ID!r}) - alert skipped.", flush=True)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for attempt in range(2):
        try:
            await _client.post(
                url,
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
                timeout=10.0,
            )
            return
        except Exception as e:  # noqa: BLE001
            if attempt == 0:
                await _sleep_or_stop(0.5)
                continue
            print(f"[bridge] telegram alert failed: {e}", flush=True)


async def _sleep_or_stop(seconds):
    """Sleep, but return immediately when shutdown is requested."""
    if seconds <= 0:
        return
    try:
        await asyncio.wait_for(_shutdown.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


# ---------------------------------------------------------------------------
# GetXAPI key pool
# ---------------------------------------------------------------------------
class KeyPool:
    """Per-key health so rotation is instant and alerts never flood."""

    def __init__(self, keys):
        self.keys = list(keys)
        self.index = 0
        self.cool_until = [0.0] * len(self.keys)
        self.reason = [""] * len(self.keys)
        self.alerted_down = [False] * len(self.keys)
        self.all_alerted_at = 0.0

    def pick(self):
        """Index of the next usable key, or None when all are cooling down."""
        now = time.time()
        n = len(self.keys)
        if not n:
            return None
        for offset in range(n):
            i = (self.index + offset) % n
            if self.cool_until[i] <= now:
                self.index = i
                return i
        return None

    def mark_down(self, i, reason, cooldown):
        self.cool_until[i] = time.time() + cooldown
        self.reason[i] = reason

    def mark_ok(self, i):
        """Clear a key's cooldown; report whether it had been flagged down."""
        was_down = self.alerted_down[i]
        self.cool_until[i] = 0.0
        self.reason[i] = ""
        self.alerted_down[i] = False
        return was_down

    def fingerprints(self):
        return ", ".join(f"#{i + 1}{_mask(k)}" for i, k in enumerate(self.keys))


async def startup_alert(pool):
    if not pool.keys:
        await alert(
            "\u274c *X bridge started with no GetXAPI keys.*\n"
            "Set `TWITTER_API_KEYS` or X detection cannot run."
        )
        return
    await alert(
        f"\U0001f7e2 *X bridge online*\n"
        f"Target: @{TARGET_USER_NAME}\n"
        f"Keys loaded: {len(pool.keys)} ({pool.fingerprints()})\n"
        f"X detection: \u25b6\ufe0f active"
    )


async def rotate_down(pool, idx, reason):
    """Mark a key dead, switch to the next one immediately, alert on transition."""
    num = idx + 1
    pool.mark_down(idx, reason, KEY_COOLDOWN_SECONDS)
    nxt = pool.pick()
    if nxt is None:
        await all_keys_down_alert(pool, reason)
        return
    pool.index = nxt
    if pool.alerted_down[idx]:
        return
    pool.alerted_down[idx] = True
    print(f"[bridge] key #{num} down ({reason}) -> key #{nxt + 1}", flush=True)
    await alert(
        f"\u26a0\ufe0f *GetXAPI key #{num}* {_mask(pool.keys[idx])} is out of action.\n"
        f"Reason: {reason}\n"
        f"\U0001f504 Rotating to *key #{nxt + 1}* {_mask(pool.keys[nxt])} \u2014 no requests lost."
    )


async def all_keys_down_alert(pool, reason=""):
    now = time.time()
    if now - pool.all_alerted_at < ALL_KEYS_ALERT_COOLDOWN:
        return
    pool.all_alerted_at = now
    lines = [
        f"\u2022 key #{i + 1}{_mask(k)}: {pool.reason[i] or 'failed'}"
        for i, k in enumerate(pool.keys)
    ]
    detail = "\n".join(lines) if lines else "\u2022 no keys configured"
    print(f"[bridge] all keys down ({reason}) - alert sent", flush=True)
    await alert(
        "\u274c *All GetXAPI keys are out of action.*\n"
        f"{detail}\n\n"
        f"\U0001f6a8 X detection is stalled until a key recovers "
        f"(retrying every {int(min(KEY_COOLDOWN_SECONDS, 60))}s). Check your keys."
    )


# ---------------------------------------------------------------------------
# Bot pause state (background monitor - keeps the hot path free of extra RTT)
# ---------------------------------------------------------------------------
async def fetch_bot_active(client):
    """Return (active, reachable). Fails open so a bot blip cannot stall X."""
    try:
        res = await client.get(STATUS_URL, headers=_auth_headers(), timeout=REQUEST_TIMEOUT)
    except Exception:  # noqa: BLE001
        return True, False
    if res.status_code == 401:
        return True, False
    if res.status_code != 200:
        return True, False
    try:
        body = res.json()
    except Exception:  # noqa: BLE001
        return True, False
    if not isinstance(body, dict):
        return True, False
    return bool(body.get("active", True)), True


async def status_monitor(client):
    """Refresh the bot's active flag. Reacts to Pause/Resume in ~1-2 seconds."""
    warned_auth = False
    while not _shutdown.is_set():
        active, reachable = await fetch_bot_active(client)

        if reachable and not _bot["reachable"]:
            print("[bridge] bot /status reachable.", flush=True)
        elif not reachable and _bot["reachable"]:
            print("[bridge] WARNING: bot /status unreachable - assuming active.", flush=True)
        _bot["reachable"] = reachable

        if not reachable and not warned_auth:
            warned_auth = True
        elif reachable:
            warned_auth = False

        prev = _bot["active"]
        _bot["active"] = active

        if active != prev:
            if active:
                print("[bridge] X detection RESUMED - polling active.", flush=True)
                if NOTIFY_ON_PAUSE:
                    await alert(
                        f"\u25b6\ufe0f *X detection resumed.*\n"
                        f"Polling @{TARGET_USER_NAME} again."
                    )
            else:
                print("[bridge] X detection PAUSED by admin - polling halted.", flush=True)
                if NOTIFY_ON_PAUSE:
                    await alert(
                        "\u23f8\ufe0f *X detection paused by admin.*\n"
                        "Polling halted - no GetXAPI credits used while paused."
                    )

        await _sleep_or_stop(
            PAUSED_CHECK_SECONDS if not _bot["active"] else STATUS_CHECK_SECONDS
        )


# ---------------------------------------------------------------------------
# Forwarding
# ---------------------------------------------------------------------------
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
# Main detection loop
# ---------------------------------------------------------------------------
async def poll_loop(client, pool):
    params = {"userName": TARGET_USER_NAME}
    seen = deque(maxlen=SEEN_MAXLEN)
    seen_set = set()
    baseline_done = False
    last_new = 0.0
    err_streak = 0

    while not _shutdown.is_set():
        # Honour admin Pause instantly - no upstream calls while paused.
        if not _bot["active"]:
            await _sleep_or_stop(0.5)
            continue

        if not pool.keys:
            print("[bridge] no GetXAPI keys configured - waiting 30s.", flush=True)
            await _sleep_or_stop(30)
            continue

        idx = pool.pick()
        if idx is None:
            await all_keys_down_alert(pool)
            await _sleep_or_stop(30)
            continue

        headers = {"Authorization": f"Bearer {pool.keys[idx]}"}

        try:
            res = await client.get(
                GETXAPI_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT
            )
        except asyncio.CancelledError:
            raise
        except (httpx.RequestError, OSError) as e:
            err_streak += 1
            delay = _backoff(err_streak)
            print(f"[bridge] network error: {e} - retry in {delay:.0f}s", flush=True)
            await _sleep_or_stop(delay)
            continue
        except Exception as e:  # noqa: BLE001
            err_streak += 1
            delay = _backoff(err_streak)
            print(f"[bridge] polling error: {e} - retry in {delay:.0f}s", flush=True)
            await _sleep_or_stop(delay)
            continue

        status = res.status_code

        # Transient upstream trouble: back off, keep the key.
        if status >= 500:
            err_streak += 1
            delay = _backoff(err_streak)
            print(f"[bridge] upstream {status} - backing off {delay:.0f}s (key kept).", flush=True)
            await _sleep_or_stop(delay)
            continue

        # Dead / unpaid / throttled key: cool it down and move on immediately.
        if status in (401, 402, 403):
            await rotate_down(pool, idx, f"HTTP {status}")
            continue
        if status == 429:
            await rotate_down(pool, idx, "HTTP 429 (rate limited)")
            continue
        if status != 200:
            err_streak += 1
            delay = _backoff(err_streak)
            print(f"[bridge] unexpected upstream {status} - retry in {delay:.0f}s", flush=True)
            await _sleep_or_stop(delay)
            continue

        # HTTP 200 - still inspect the body for a logical exhaustion signal.
        err_streak = 0
        try:
            body = res.json()
        except Exception:  # noqa: BLE001
            body = {}
        body_str = res.text.lower()

        body_error = isinstance(body, dict) and body.get("error")
        quota_hit = any(
            w in body_str
            for w in ("quota", "exhausted", "out of credits", "credits exhausted", "no credits")
        )
        if body_error or quota_hit:
            await rotate_down(pool, idx, "quota/credits exhausted (API body)")
            continue

        if pool.mark_ok(idx):
            print(f"[bridge] key #{idx + 1} recovered.", flush=True)
            await alert(
                f"\u267b\ufe0f *GetXAPI key #{idx + 1}* {_mask(pool.keys[idx])} "
                f"is working again.\nNow serving requests."
            )

        tweets = body.get("tweets", []) if isinstance(body, dict) else []
        if not isinstance(tweets, list):
            tweets = []

        if not baseline_done:
            baseline_done = True
            if BASELINE_ON_START:
                for tw in tweets:
                    if not isinstance(tw, dict):
                        continue
                    tid = str(tw.get("id") or "")
                    if tid and tid not in seen_set:
                        seen.append(tid)
                        seen_set.add(tid)
                print(f"[bridge] baseline locked: {len(seen_set)} known tweet(s).", flush=True)
                await _sleep_or_stop(POLL_ACTIVE_SECONDS)
                continue

        for tw in tweets:
            if not isinstance(tw, dict):
                continue
            tid = str(tw.get("id") or "")
            text = str(tw.get("text") or "")
            if not tid or not text or tid in seen_set:
                continue
            if len(seen) == seen.maxlen:
                seen_set.discard(seen.popleft())
            seen.append(tid)
            seen_set.add(tid)

            codes = extract_codes(text)
            if not codes:
                print(f"[bridge] tweet {tid}: no promo code isolated.", flush=True)
                continue

            print(f"[bridge] new tweet {tid} -> {codes}", flush=True)
            for code in codes:
                await forward_code(client, code)
            last_new = time.monotonic()

        # Adaptive cadence: quick right after new content, relaxed when idle.
        recent = (time.monotonic() - last_new) < 15
        base = POLL_ACTIVE_SECONDS if recent else POLL_IDLE_SECONDS
        await _sleep_or_stop(base + random.uniform(0, POLL_JITTER))


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def _install_signal_handlers():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _shutdown.set)
        except NotImplementedError:
            pass


async def main():
    global _client
    _install_signal_handlers()

    limits = httpx.Limits(max_keepalive_connections=20, max_connections=40)
    timeout = httpx.Timeout(REQUEST_TIMEOUT, connect=5.0)

    print(
        f"[bridge] active for @{TARGET_USER_NAME} | keys={len(API_KEYS)} "
        f"| secret={'set' if WEBHOOK_SECRET else 'MISSING'} "
        f"| poll={POLL_ACTIVE_SECONDS}s/{POLL_IDLE_SECONDS}s "
        f"| status={STATUS_CHECK_SECONDS}s",
        flush=True,
    )
    if not WEBHOOK_SECRET:
        print("[bridge] WARNING: WEBHOOK_SECRET is not set - the bot will reject "
              "forwarded codes with 401.", flush=True)
    if not API_KEYS:
        print("[bridge] WARNING: TWITTER_API_KEYS is empty - X detection cannot run.", flush=True)

    pool = KeyPool(API_KEYS)

    async with httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=True) as client:
        _client = client
        if ALERTS_ON_START:
            await startup_alert(pool)

        monitor = asyncio.create_task(status_monitor(client))
        try:
            await poll_loop(client, pool)
        finally:
            monitor.cancel()
            try:
                await monitor
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[bridge] stopped.", flush=True)
