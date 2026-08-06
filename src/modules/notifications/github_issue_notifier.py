"""Airflow on_failure_callback that files/updates a GitHub issue for a DAG failure.

Right now a DAG failing at 07:00 ET is silent unless someone happens to check the
Airflow UI -- default_args already sets email_on_failure=True, but nothing in this
deployment configures SMTP, so those emails almost certainly go nowhere. This
gives DAG failures a channel that doesn't depend on that: a GitHub issue.

Configuration (read from the environment at call time, so it works whether set via
the Airflow container's env or an Airflow Variable exported into the environment):
  GITHUB_TOKEN  -- a GitHub PAT with `issues:write` on the target repo. REQUIRED;
                   if unset, this logs a warning and returns without raising -- a
                   missing notifier configuration must never mask the DAG's real
                   failure by raising a second exception out of the callback.
  GITHUB_REPO   -- "owner/repo". Defaults to "AlgoGators/data-ngin".

Repeat failures of the same DAG update the existing open issue (by searching for
one with the `dag-failure` label whose title names the DAG) instead of opening a
duplicate every run.
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)

DEFAULT_REPO = "AlgoGators/data-ngin"
ISSUE_LABEL = "dag-failure"
_TIMEOUT_SECONDS = 10


def notify_dag_failure(context):
    """Airflow on_failure_callback signature: takes the task-instance context dict."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        logger.warning(
            "GITHUB_TOKEN not set; skipping DAG-failure GitHub issue notification "
            "for dag_id=%s",
            context.get("dag").dag_id if context.get("dag") else "<unknown>",
        )
        return

    repo = os.environ.get("GITHUB_REPO", DEFAULT_REPO)
    dag_id = context["dag"].dag_id
    task_instance = context["task_instance"]
    # logical_date is the Airflow 3 name; execution_date is the Airflow 2 name.
    run_id = context.get("logical_date") or context.get("execution_date")

    title = f"[{ISSUE_LABEL}] {dag_id} failed"
    body = (
        f"**DAG:** `{dag_id}`\n"
        f"**Task:** `{task_instance.task_id}`\n"
        f"**Run:** `{run_id}`\n"
        f"**Exception:**\n```\n{context.get('exception')}\n```\n"
        f"**Logs:** {task_instance.log_url}\n"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }

    try:
        existing = _find_open_issue(repo, dag_id, headers)
        if existing is not None:
            _comment_on_issue(repo, existing, body, headers)
            logger.info(
                "Added failure comment to existing issue #%s for %s", existing, dag_id
            )
        else:
            number = _create_issue(repo, title, body, headers)
            logger.info("Filed new issue #%s for %s failure", number, dag_id)
    except Exception:
        # Never let a notification failure mask the DAG's actual failure.
        logger.exception(
            "Failed to file/update GitHub issue for %s failure (non-fatal)", dag_id
        )


def _find_open_issue(repo, dag_id, headers):
    query = f'repo:{repo} is:issue is:open label:{ISSUE_LABEL} in:title "{dag_id}"'
    resp = requests.get(
        "https://api.github.com/search/issues",
        params={"q": query},
        headers=headers,
        timeout=_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])
    return items[0]["number"] if items else None


def _create_issue(repo, title, body, headers):
    resp = requests.post(
        f"https://api.github.com/repos/{repo}/issues",
        json={"title": title, "body": body, "labels": [ISSUE_LABEL]},
        headers=headers,
        timeout=_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()["number"]


def _comment_on_issue(repo, issue_number, body, headers):
    resp = requests.post(
        f"https://api.github.com/repos/{repo}/issues/{issue_number}/comments",
        json={"body": body},
        headers=headers,
        timeout=_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
