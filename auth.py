import urllib.parse
import requests
import streamlit as st

GOOGLE_AUTH_URL  = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USER_URL  = "https://www.googleapis.com/oauth2/v3/userinfo"

SCOPES = "openid email profile"


def _cfg():
    return st.secrets["google_oauth"]


def get_auth_url() -> str:
    params = {
        "client_id":     _cfg()["client_id"],
        "redirect_uri":  _cfg()["redirect_uri"],
        "response_type": "code",
        "scope":         SCOPES,
        "access_type":   "offline",
        "prompt":        "select_account",
    }
    return f"{GOOGLE_AUTH_URL}?{urllib.parse.urlencode(params)}"


def exchange_code(code: str) -> dict:
    """Exchange OAuth code for user info dict."""
    cfg = _cfg()
    token_resp = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            "code":          code,
            "client_id":     cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "redirect_uri":  cfg["redirect_uri"],
            "grant_type":    "authorization_code",
        },
        timeout=10,
    )
    token_resp.raise_for_status()
    access_token = token_resp.json()["access_token"]

    user_resp = requests.get(
        GOOGLE_USER_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=10,
    )
    user_resp.raise_for_status()
    return user_resp.json()
    # returns: {"sub", "email", "name", "picture", "email_verified", ...}


def handle_oauth_callback():
    """
    Call once per page load. Detects ?code= in the URL, exchanges it,
    stores user info in st.session_state, then cleans the URL.
    """
    params = st.query_params
    if "code" not in params:
        return

    code = params["code"]
    try:
        user_info = exchange_code(code)
        st.session_state["user"] = {
            "email":        user_info["email"],
            "display_name": user_info.get("name", user_info["email"]),
            "picture":      user_info.get("picture", ""),
        }
    except Exception as e:
        st.error(f"Login failed: {e}")
    finally:
        st.query_params.clear()


def is_logged_in() -> bool:
    return "user" in st.session_state and st.session_state["user"] is not None


def logout():
    st.session_state.pop("user", None)
    st.session_state.pop("user_id", None)
