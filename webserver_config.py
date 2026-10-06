"""FAB auth config for the Airflow API server (mounted at /opt/airflow/webserver_config.py).

Members log in with GitHub through the AlgoGators Dex; their GitHub teams map to
Airflow roles. Set AIRFLOW_LOGIN=db to fall back to the local FAB users (the
break-glass path, and the easy option for local development).
"""

from __future__ import annotations

import logging
import os
from typing import Any

from airflow.providers.fab.auth_manager.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)
from flask_appbuilder.const import AUTH_DB, AUTH_OAUTH

log = logging.getLogger(__name__)

DEX_ISSUER = "https://vault.algogators.com/dex"

# Dex group (org:team-slug) -> Airflow roles. Members of any other team (e.g.
# quant-research, quant-trading) are refused at login.
DEX_ROLES_MAPPING = {
    "AlgoGators:admin": ["Admin"],
    "AlgoGators:leadership": ["Admin"],
    "AlgoGators:quant-dev": ["Op"],
}

WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None


class DexSecurityManager(FabAirflowSecurityManagerOverride):
    """Map Dex's ID token claims onto a FAB user. FAB has no built-in mapping for a generic OIDC provider."""

    def get_oauth_user_info(self, provider: str, resp: dict[str, Any]) -> dict[str, Any]:
        if provider != "dex":
            return super().get_oauth_user_info(provider, resp)
        # Authlib puts the verified ID token claims under "userinfo" when the openid scope is requested.
        claims = resp.get("userinfo") or self.oauth_remotes[provider].userinfo()
        name = claims.get("name") or ""
        first_name, _, last_name = name.partition(" ")
        groups = claims.get("groups", [])
        log.debug("Dex login for %s with groups %s", claims.get("preferred_username"), groups)
        if not set(groups) & DEX_ROLES_MAPPING.keys():
            # FAB's OAuth view treats an exception here as a failed login.
            raise PermissionError(f"{claims.get('preferred_username')} is not in a team with Airflow access")
        return {
            "username": claims["preferred_username"],
            "email": claims.get("email", ""),
            "first_name": first_name,
            "last_name": last_name,
            "role_keys": groups,
        }


if os.environ.get("AIRFLOW_LOGIN", "dex") == "db":
    AUTH_TYPE = AUTH_DB
else:
    if not os.environ.get("AIRFLOW_OIDC_SECRET"):
        raise RuntimeError("AIRFLOW_OIDC_SECRET is unset -- set it in .env, or set AIRFLOW_LOGIN=db")
    AUTH_TYPE = AUTH_OAUTH
    SECURITY_MANAGER_CLASS = DexSecurityManager

    OAUTH_PROVIDERS = [
        {
            "name": "dex",
            "icon": "fa-github",
            "token_key": "access_token",
            "remote_app": {
                "client_id": "airflow",
                "client_secret": os.environ.get("AIRFLOW_OIDC_SECRET"),
                "server_metadata_url": f"{DEX_ISSUER}/.well-known/openid-configuration",
                "client_kwargs": {"scope": "openid email profile groups"},
            },
        }
    ]

    # Members of a mapped team get an account on first login.
    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Viewer"

    # Re-applied on every login, so team changes in GitHub take effect the next
    # time the user signs in.
    AUTH_ROLES_MAPPING = DEX_ROLES_MAPPING
    AUTH_ROLES_SYNC_AT_LOGIN = True
