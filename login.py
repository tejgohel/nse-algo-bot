# ─────────────────────────────────────────────────────────────────────────────
#  login.py  —  Shared broker connection
#
#  NOTE: the request headers are built by a function (get_headers), NOT a
#  module-level dict. A static dict would be built at import time — before
#  auto_login patches config.ACCESS_TOKEN — so every API call would use the
#  old, expired token (HTTP 401 / DH-906).
#
#  By calling get_headers() inside every request we always pick up whatever
#  token is currently in config.ACCESS_TOKEN.
#
#  ⚠️  No credentials live here. config.CLIENT_ID / config.ACCESS_TOKEN come
#      from your own `.env` file — see config.py for the full setup steps.
# ─────────────────────────────────────────────────────────────────────────────

import config

dhan = None


def mask_client_id(cid: str = "") -> str:
    """Client ID with all but the last 4 digits hidden.

    The startup line ends up in terminal screenshots, shared logs and bug
    reports far more often than anyone intends, and a client ID is half of
    what an attacker needs. The tail is kept so you can still tell which
    account a run used.
    """
    cid = str(cid or config.CLIENT_ID or "")
    return "*" * max(len(cid) - 4, 0) + cid[-4:] if cid else "(not set)"


def reload_dhan():
    """
    (Re)creates the broker SDK client with the current config.ACCESS_TOKEN.
    Safe to call when credentials are missing — it leaves `dhan` as None
    instead of raising at import time.
    """
    global dhan
    if not (config.CLIENT_ID and config.ACCESS_TOKEN):
        dhan = None
        return None
    try:
        from dhanhq import dhanhq
        dhan = dhanhq(config.CLIENT_ID, config.ACCESS_TOKEN)
        print(f"[OK] Broker client ready | Client: {mask_client_id()}")
    except Exception as e:
        dhan = None
        print(f"[WARN] Broker SDK unavailable: {e}")
    return dhan


def get_headers() -> dict:
    """
    Returns the API request headers using the CURRENT access token.

    Always call this right before each request. Do NOT cache the result.
    """
    return {
        "Accept"       : "application/json",
        "Content-Type" : "application/json",
        "access-token" : config.ACCESS_TOKEN,
        "client-id"    : config.CLIENT_ID,
    }


if config.CLIENT_ID and config.ACCESS_TOKEN:
    reload_dhan()
