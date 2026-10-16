#!/usr/bin/env python3
""" Chowdeck X (Twitter) Bridge =========================== Polls GetXAPI for new promo tweets and forwards every isolated promo code to the main bot's authenticated webhook (POST /tweet). This revision fixes the pause/resume behaviour and makes key handling bulletproof ------------------------------------------------------------------------------ * PAUSE / RESUME reacts in ~1-2 seconds. The bot's /status is read by a small background monitor task, never on the detection critical path, so toggling X detection in Telegram takes effect almost instantly and detection latency is unaffected. Logs and admin notices are emitted only on a real state change (no more repeated "waiting 20s" spam). * GETXAPI KEY POOL with per-key state. A dead key goes on a cooldown, rotation is immediate (no sleep), and the admin is told which key failed, why, and which key took over - exactly like the original, but without alert flooding. * Transient upstream failures (5xx / network) never burn a key: bounded exponential backoff with jitter. * 429 is treated as a temporary throttle (cooldown), not a permanent death. * CAPS-ONLY CODES. A token is only treated as a promo code when the poster typed it in ALL CAPS with at least one letter (ENDEAVOR26, FIVEFORFIVE). Sentence-case chatter ("Slight change of plans.", "First voucher now drops at 5:50pm.") is never forwarded, so ordinary words can no longer masquerade as codes. * Multi-code tweets: every promo code in the tweet is forwarded, not just the first. * NEW TWEETS ONLY. A persisted high-water-mark tweet id makes the bridge announce only posts that appear AFTER it is running. The backlog that was already on the timeline is never replayed - not on restart, not after a key rotation. A burst of catch-up tweets collapses to the single latest post (MAX_NEW_PER_POLL), and a carried-forward tweet older than MAX_TWEET_AGE_SECONDS is dropped. * Everything is bounded: de-duplicated ring buffer, capped fan-out, per-request timeouts, no unbounded retry loops. A bad response never crashes the process. """

import asyncio
import json
import os
import random
import re
import signal
import time
from collections import deque
from datetime import datetime, timezone

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
# Base URL for the Telegram Bot API. Override only for testing (e.g. a mock).
TELEGRAM_API_BASE = os.getenv(
    "TELEGRAM_API_BASE", "https://api.telegram.org"
).strip().rstrip("/")

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

# Promo-code extraction. Chowdeck codes can be MIXED (CHOW5K, OYIN5K) or
# LETTERS-ONLY (e.g. MRBEEISTESTING). By default the bridge accepts BOTH: a
# token qualifies when it mixes letters and digits, OR when it is letters-only,
# at least MIN_CODE_LEN long and not a common English word. Set
# CODE_REQUIRE_MIXED=true to restore the strict "must contain a digit" rule
# (it keeps a few extra words out but will DROP letters-only codes).
CODE_TOKEN = re.compile(r"[A-Za-z0-9]{4,20}")
URL_OR_HANDLE = re.compile(r"(https?://\S+|www\.\S+|[#@]\w+)", re.IGNORECASE)
CODE_REQUIRE_MIXED = os.getenv("CODE_REQUIRE_MIXED", "false").strip().lower() in (
    "1", "true", "yes", "on",
)
CODE_REQUIRE_CAPS = os.getenv("CODE_REQUIRE_CAPS", "true").strip().lower() in (
    "1", "true", "yes", "on",
)
# Minimum promo-code length. Keep at 5 to match the bot's X-fed minimum so
# nothing the bot accepts is silently dropped here.
MIN_CODE_LEN = int(os.getenv("MIN_CODE_LEN", "5"))
MAX_CODES_PER_TWEET = int(os.getenv("MAX_CODES_PER_TWEET", "4"))

# CASE GATE (default ON) - the single most effective false-positive filter.
# Chowdeck posts real codes in ALL CAPS (ENDEAVOR26, FIVEFORFIVE) while ordinary
# chatter is sentence-case ("Slight change of plans.", "First voucher now drops
# at 5:50pm."). With this on, only all-upper-case tokens that contain at least
# one letter can ever be forwarded, which removes the whole lowercase class of
# bogus codes (PLANS / SLIGHT / DROPS). Set CODE_REQUIRE_CAPS=false only if a
# poster ever starts writing codes in lowercase - matching is case-insensitive
# again in that mode, and the COMMON_WORDS stop-list becomes the only filter.

# ---------------------------------------------------------------------------
# New-tweets-only policy
# ---------------------------------------------------------------------------
# The bridge must never replay old tweets. It keeps a HIGH-WATER MARK: the
# numeric id of the newest tweet it has ever observed. Each poll forwards only
# tweets whose id is STRICTLY GREATER than that mark - the backlog that was
# already on the timeline when the bridge (re)started is therefore never
# announced, even after a restart or a key rotation.
#
# At most MAX_NEW_PER_POLL tweets are forwarded per poll, newest first, so a
# sudden burst of catch-up tweets collapses to the single latest post.
MAX_NEW_PER_POLL = max(1, int(os.getenv("MAX_NEW_PER_POLL", "1")))
# Reject a carried-forward tweet whose timestamp is older than this many
# seconds. Guards against an upstream/query hiccup surfacing a stale tweet as
# if it were fresh. Set to 0 to disable the age gate.
MAX_TWEET_AGE_SECONDS = float(os.getenv("MAX_TWEET_AGE_SECONDS", "180"))
# Where the high-water mark is persisted. On Railway, point this at a mounted
# volume (e.g. /data/bridge_state.json) so it survives redeploys. When the
# path is not writable the mark is still kept in memory for this process.
_default_state_path = (
    "/data/bridge_state.json" if os.path.isdir("/data") else "bridge_state.json"
)
STATE_FILE = os.getenv("STATE_FILE", _default_state_path).strip()
PERSIST_HWM = os.getenv("PERSIST_HWM", "true").strip().lower() in (
    "1", "true", "yes", "on",
)

# Stop-list of ordinary English words that must never be treated as a
# letters-only promo code. Extend it with COMMON_WORDS (comma separated).
_DEFAULT_COMMON_WORDS = (
    "about above after again against almost alone along already also although "
    "always among another answer around because before being below better "
    "between both came change check claim code codes come could daily deals "
    "does done down each even ever every find first found free from give going "
    "good great have here high hold hope into just keep know last left less "
    "life like link list look made make many might more most much must near "
    "need never next nice none only open other over people please post promo "
    "push right same send share should since some soon such sure take tell test "
    "testing than that their them then there these they thing think this those "
    "time today tweet under until very want watch well were what when where "
    "which while will with within would your yours "
    "checkout cart order orders vendor discount deliver delivery enjoy tasty "
    "fresh hungry snack food meal treat chicken burger rice pizza drink eats "
    "guys dude hello welcome thanks thank awesome amazing hurry grab rush "
    "limited available soon check "
    "plans slight drops seconds wild tomorrow tonight maybe later tonight "
    "stop start yes no true false all any more less best worst"
)
COMMON_WORDS = {w for w in _DEFAULT_COMMON_WORDS.split() if w}
COMMON_WORDS.update(
    w.strip().lower()
    for w in os.getenv("COMMON_WORDS", "").split(",")
    if w.strip()
)

_shutdown = asyncio.Event()
_client = None

# Shared bot state, refreshed by status_monitor() and read by poll_loop().
_bot = {"active": True, "reachable": False}

# One-time guard so a missing-credentials warning is logged only once.
_alert_warned = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def extract_codes(text):
    """Return every promo-code candidate in a tweet body (deduped, ordered). Every code found in the tweet is returned, not just the first, so multi-code tweets are fully forwarded. The tweet is tokenised WITH ITS ORIGINAL CASE intact and a token only qualifies when the poster typed it in ALL CAPS with at least one letter. Uppercasing the tweet first (the old behaviour) turned sentence-case chatter such as "Slight change of plans." into the bogus codes SLIGHT / PLANS, so case is preserved and checked explicitly instead. """
    if not text:
        return []

    cleaned = URL_OR_HANDLE.sub(" ", text)
    tokens = []
    for t in CODE_TOKEN.findall(cleaned):  # case preserved on purpose
        if t not in tokens:
            tokens.append(t)

    # CASE GATE: all-caps token carrying at least one letter. Pure-digit tokens
    # ("48", "550") are never codes and are dropped here as well.
    if CODE_REQUIRE_CAPS:
        tokens = [
            t for t in tokens
            if any(ch.isalpha() for ch in t) and t == t.upper()
        ]
    if not tokens:
        return []

    def _is_mixed(tok):
        return any(ch.isdigit() for ch in tok) and any(ch.isalpha() for ch in tok)

    def _is_code_like(tok):
        if _is_mixed(tok):
            return True
        if CODE_REQUIRE_MIXED:
            return False
        # Letters-only token: reject ordinary English words. Tokens keep their
        # original case here, so compare against the lower-case stop-list
        # case-insensitively.
        return tok.lower() not in COMMON_WORDS

    chosen = [
        t for t in tokens
        if len(t) >= MIN_CODE_LEN and _is_code_like(t)
    ]

    if not chosen:
        # Strict-mode escape hatch: a short tweet that is essentially just the
        # code itself (covers a bare code tweet with no digit). It is only
        # needed when permissive letters-only matching is disabled - otherwise
        # an empty result genuinely means "no code", so do not second-guess it.
        stripped = re.sub(r"\s+", " ", cleaned.strip())
        if CODE_REQUIRE_MIXED and len(tokens) == 1 and 0 < len(stripped) <= 60:
            chosen = tokens
        else:
            return []

    out = []
    for tok in chosen:
        if tok not in out:
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
    """Best-effort Telegram alert to the admin. Never raises, never blocks long. Every failure path is logged: missing credentials, a non-numeric chat id, a transport error, and a non-200 response from the Telegram API (whose body is printed so 401/400/403 reasons are visible instead of being swallowed). """
    global _alert_warned
    if not TELEGRAM_BOT_TOKEN or not ADMIN_CHAT_ID:
        if not _alert_warned:
            _alert_warned = True
            missing = []
            if not TELEGRAM_BOT_TOKEN:
                missing.append("TELEGRAM_BOT_TOKEN")
            if not ADMIN_CHAT_ID:
                missing.append("ADMIN_CHAT_ID")
            print(
                "[bridge] WARNING: Telegram alerting is DISABLED - "
                f"{' and '.join(missing)} not set. No admin notice will be sent.",
                flush=True,
            )
        return
    if _client is None:
        return
    try:
        chat_id = int(ADMIN_CHAT_ID)
    except (TypeError, ValueError):
        print(f"[bridge] ADMIN_CHAT_ID is not numeric ({ADMIN_CHAT_ID!r}) - alert skipped.", flush=True)
        return
    url = f"{TELEGRAM_API_BASE}/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    for attempt in range(2):
        try:
            res = await _client.post(url, json=payload, timeout=10.0)
        except Exception as e:  # noqa: BLE001
            if attempt == 0:
                await _sleep_or_stop(0.5)
                continue
            print(f"[bridge] telegram alert failed: {e}", flush=True)
            return
        if res.status_code == 200:
            return
        body = (res.text or "").strip()
        short = body[:300] + ("..." if len(body) > 300 else "")
        if res.status_code == 400 and payload.get("parse_mode"):
            # Markdown rejected - retry once as plain text so the alert still lands.
            print(
                f"[bridge] telegram alert rejected (HTTP 400): {short} "
                "- retrying as plain text.",
                flush=True,
            )
            payload = {"chat_id": chat_id, "text": text}
            continue
        if res.status_code >= 500 and attempt == 0:
            await _sleep_or_stop(0.5)
            continue
        print(
            f"[bridge] telegram alert rejected (HTTP {res.status_code}): {short}",
            flush=True,
        )
        return
    print("[bridge] telegram alert could not be delivered.", flush=True)


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
# Tweet identity, age and the high-water mark
# ---------------------------------------------------------------------------
def _tweet_id_of(tw):
    """Return the tweet id as an int, or None when it cannot be parsed."""
    if not isinstance(tw, dict):
        return None
    raw = tw.get("id")
    if raw in (None, ""):
        for key in ("id_str", "tweetId", "tweet_id", "rest_id"):
            if tw.get(key) not in (None, ""):
                raw = tw.get(key)
                break
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return None


_TS_KEYS = (
    "createdAt", "created_at", "time", "timestamp", "date",
    "tweetedAt", "tweetCreatedAt", "created",
)


def _parse_tweet_time(value):
    """Parse an epoch (seconds or milliseconds) or ISO-8601 stamp to epoch seconds. Returns None when the format is not recognised. """
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        return ts / 1000.0 if ts > 1e11 else ts
    text = str(value).strip()
    if re.fullmatch(r"\d+(\.\d+)?", text):
        ts = float(text)
        return ts / 1000.0 if ts > 1e11 else ts
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        dt = None
    if dt is None:
        for fmt in ("%a %b %d %H:%M:%S %z %Y", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                dt = None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        return dt.timestamp()
    except (OverflowError, OSError, ValueError):
        return None


def _tweet_age_seconds(tw, now=None):
    """Seconds since the tweet was posted, or None when the age is unknown."""
    stamp = None
    for key in _TS_KEYS:
        stamp = _parse_tweet_time(tw.get(key))
        if stamp is not None:
            break
    if stamp is None:
        return None
    return (now if now is not None else time.time()) - stamp


def _load_hwm():
    """Best-effort read of the persisted high-water-mark tweet id."""
    if not PERSIST_HWM or not STATE_FILE:
        return None
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return None
    except Exception as e:  # noqa: BLE001
        print(
            f"[bridge] state read failed ({e}) - starting without a saved high-water mark.",
            flush=True,
        )
        return None
    raw = data.get("last_tweet_id") if isinstance(data, dict) else None
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _save_hwm(value):
    """Best-effort, atomic persist of the high-water-mark tweet id."""
    if not PERSIST_HWM or not STATE_FILE:
        return
    tmp = f"{STATE_FILE}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"last_tweet_id": int(value), "updated_at": time.time()}, fh)
        os.replace(tmp, STATE_FILE)
    except Exception as e:  # noqa: BLE001
        print(
            f"[bridge] state write failed ({e}) - high-water mark kept in memory only.",
            flush=True,
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

    # Highest tweet id ever observed. Only ids STRICTLY greater than this are
    # eligible for forwarding, so the backlog can never be replayed - not after
    # a restart (the mark is persisted) and not after a key rotation.
    hwm = _load_hwm()
    if hwm is not None:
        print(
            f"[bridge] high-water mark loaded: tweets newer than id {hwm} count as new.",
            flush=True,
        )

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

        observed = [
            (tid, tw)
            for tid, tw in ((_tweet_id_of(tw), tw) for tw in tweets)
            if tid is not None
        ]

        # ---- Baseline: lock the mark to the newest tweet, forward nothing ---
        # Everything currently on the timeline is, by definition, old. Without a
        # persisted mark the first usable poll locks to the newest id, so a cold
        # start can never dump the backlog into Telegram. Nothing is added to the
        # seen buffer here: the mark alone decides what is new, otherwise a tweet
        # posted right after startup would be swallowed by the baseline.
        if not baseline_done:
            baseline_done = True
            newest = max((tid for tid, _ in observed), default=None)
            if hwm is None and newest is not None:
                hwm = newest
                _save_hwm(hwm)
                print(
                    f"[bridge] no saved high-water mark - locked to newest tweet id "
                    f"{hwm}; existing tweets never forwarded.",
                    flush=True,
                )
            print(
                f"[bridge] baseline locked: {len(observed)} tweet(s) on the timeline, "
                f"high-water mark = {hwm}. Nothing forwarded.",
                flush=True,
            )
            if BASELINE_ON_START:
                await _sleep_or_stop(POLL_ACTIVE_SECONDS)
                continue

        # ---- Only tweets strictly newer than the mark are candidates --------
        fresh = [
            (tid, tw)
            for tid, tw in observed
            if (hwm is None or tid > hwm) and tid not in seen_set
        ]
        # Newest first, so a burst collapses to the single latest post.
        fresh.sort(key=lambda pair: pair[0], reverse=True)

        # Advance the mark past everything now observed, forwarded or not, so an
        # old (or age-rejected) tweet can never be revisited on a later poll.
        if fresh and (hwm is None or fresh[0][0] > hwm):
            hwm = fresh[0][0]
            _save_hwm(hwm)

        handled = 0
        for tid, tw in fresh:
            if handled >= MAX_NEW_PER_POLL:
                print(
                    f"[bridge] backlog guard: {len(fresh) - handled} older tweet(s) "
                    f"skipped (MAX_NEW_PER_POLL={MAX_NEW_PER_POLL}) - only the latest "
                    f"post is announced.",
                    flush=True,
                )
                break

            text = str(tw.get("text") or "")
            if not text:
                continue

            age = _tweet_age_seconds(tw)
            if MAX_TWEET_AGE_SECONDS > 0 and age is not None and age > MAX_TWEET_AGE_SECONDS:
                print(
                    f"[bridge] tweet {tid} is {age:.0f}s old "
                    f"(gate {MAX_TWEET_AGE_SECONDS:.0f}s) - not forwarded.",
                    flush=True,
                )
                continue

            handled += 1
            if tid not in seen_set:
                if len(seen) == seen.maxlen:
                    seen_set.discard(seen.popleft())
                seen.append(tid)
                seen_set.add(tid)

            codes = extract_codes(text)
            if not codes:
                preview = " ".join(text.split())[:140]
                print(f"[bridge] tweet {tid}: no promo code isolated. text={preview!r}", flush=True)
                continue

            age_txt = f"{age:.0f}s" if age is not None else "unknown"
            print(f"[bridge] new tweet {tid} (age={age_txt}) -> {codes}", flush=True)
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
        f"| status={STATUS_CHECK_SECONDS}s "
        f"| alerts={'on' if (TELEGRAM_BOT_TOKEN and ADMIN_CHAT_ID) else 'OFF'} "
        f"| codes={'mixed-only' if CODE_REQUIRE_MIXED else 'mixed+letters'} "
        f"| caps={'on' if CODE_REQUIRE_CAPS else 'off'} "
        f"| stoplist={len(COMMON_WORDS)} "
        f"| new=latest-only(max {MAX_NEW_PER_POLL}/poll, "
        f"age<={MAX_TWEET_AGE_SECONDS:.0f}s) "
        f"| state={STATE_FILE if (PERSIST_HWM and STATE_FILE) else 'memory-only'}",
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
