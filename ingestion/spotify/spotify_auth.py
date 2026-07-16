"""
One-time Spotify authorization.

Launches your browser so you can sign in and approve access, then writes the
resulting access + refresh tokens to the project .env file. You only need to
run this once (or again if you revoke access / change scopes).

Prereqs (already in .env): SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET

In your Spotify app dashboard (https://developer.spotify.com/dashboard),
under "Edit Settings", add this exact Redirect URI:

    http://127.0.0.1:8888/callback
"""

import base64
import os
import secrets
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
from dotenv import find_dotenv, load_dotenv, set_key

REDIRECT_URI = "http://127.0.0.1:8888/callback"
SCOPE = "user-read-recently-played"
AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"

load_dotenv()
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError("SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET must be set in .env")


class _CallbackHandler(BaseHTTPRequestHandler):
    """Catches the ?code=... that Spotify sends back to the redirect URI."""

    auth_code = None
    state = None

    def do_GET(self):
        params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        _CallbackHandler.auth_code = params.get("code", [None])[0]
        _CallbackHandler.state = params.get("state", [None])[0]

        message = "Spotify authorization complete. You can close this tab."
        if params.get("error"):
            message = f"Authorization failed: {params['error'][0]}"

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(f"<html><body><h2>{message}</h2></body></html>".encode())

    def log_message(self, *args):  # silence default request logging
        pass


def get_auth_code():
    state = secrets.token_urlsafe(16)
    query = urllib.parse.urlencode({
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPE,
        "state": state,
    })
    auth_url = f"{AUTH_URL}?{query}"

    print("Opening browser for Spotify sign-in...")
    print(f"If it doesn't open automatically, visit:\n{auth_url}\n")
    webbrowser.open(auth_url)

    # Serve exactly one request (the redirect back from Spotify), then stop.
    server = HTTPServer(("127.0.0.1", 8888), _CallbackHandler)
    server.handle_request()
    server.server_close()

    if _CallbackHandler.state != state:
        raise ValueError("State mismatch — possible CSRF, aborting.")
    if not _CallbackHandler.auth_code:
        raise ValueError("No authorization code received from Spotify.")
    return _CallbackHandler.auth_code


def exchange_code_for_tokens(code):
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    response = requests.post(
        TOKEN_URL,
        headers={"Authorization": f"Basic {auth_header}"},
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
        },
    )
    response.raise_for_status()
    return response.json()


def main():
    code = get_auth_code()
    tokens = exchange_code_for_tokens(code)

    dotenv_path = find_dotenv() or ".env"
    set_key(dotenv_path, "SPOTIFY_ACCESS_TOKEN", tokens["access_token"])
    set_key(dotenv_path, "SPOTIFY_REFRESH_TOKEN", tokens["refresh_token"])

    print("\nSaved SPOTIFY_ACCESS_TOKEN and SPOTIFY_REFRESH_TOKEN to .env")
    print(f"Access token expires in {tokens['expires_in']} seconds "
          "(raw_spotify_plays.py refreshes it automatically).")


if __name__ == "__main__":
    main()
