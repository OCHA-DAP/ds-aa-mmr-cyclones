"""
Slack bot for Myanmar cyclone monitoring.
Posts workflow status and active monitoring alerts to Slack via incoming webhooks.
See: https://api.slack.com/messaging/webhooks
"""

import datetime
import os
from dotenv import load_dotenv

import ocha_stratus as stratus
import requests

import src.utils.constants as constants
from src.utils.logging import get_logger

load_dotenv()
logger = get_logger(__name__)

GITHUB_REPO = os.getenv("GITHUB_REPO", "ocha-dap/ds-aa-mmr-cyclones")
WORKFLOWS = [
    "run_monitoring.yml",
    "run_update_ecmwf.yml",
    "run_update_chirps_gefs.yml",
]


# ---------------------------------------------------------------------------
# Slack message builder
# ---------------------------------------------------------------------------


def post_message(header_text: str, signals_text: str, status_text: str) -> None:
    slack_url = os.environ["SLACK_WEBHOOK_URL"]

    msg = {
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": header_text}},
            {"type": "section", "text": {"type": "mrkdwn", "text": signals_text}},
            {"type": "divider"},
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": status_text}],
            },
            {"type": "divider"},
        ]
    }

    response = requests.post(slack_url, json=msg)
    if response.status_code != 200:
        raise RuntimeError(f"Error posting Slack message: {response.text}")


def build_header(n_alerts: int) -> str:
    date_str = datetime.date.today().strftime("%d %b %Y")
    if n_alerts == 0:
        return f"{date_str}: No active cyclone alerts"
    return f":rotating_light: <!channel> {date_str}: {n_alerts} active alert(s)"


# ---------------------------------------------------------------------------
# Blob storage signal checks
# ---------------------------------------------------------------------------


def _latest_blob_today(prefix: str) -> list[str]:
    """Return blobs with given prefix updated today."""
    today = datetime.date.today().strftime("%Y-%m-%d")
    blobs = stratus.list_container_blobs(
        name_starts_with=f"projects/{constants.PROJECT_PREFIX}/processed/{prefix}"
    )
    return [b for b in blobs if today in b]


def build_signals_text() -> tuple[str, int]:
    """Check blob storage and return (signals_text, n_alerts)."""
    lines = []
    n_alerts = 0

    cyclone_blobs = _latest_blob_today("monitoring_")
    if cyclone_blobs:
        n_alerts += 1
        lines.append(":rotating_light: *Cyclone in area of interest* — track data available today")
    else:
        lines.append(":large_green_circle: No cyclone in area of interest today")

    wind_blobs = _latest_blob_today("wind_exceedance_")
    if wind_blobs:
        n_alerts += 1
        lines.append(
            f":rotating_light: *Wind threshold exceeded* ({constants.wind_speed_alert_level} kt)"
        )
    else:
        lines.append(f":large_green_circle: Wind threshold not exceeded ({constants.wind_speed_alert_level} kt)")

    rainfall_blobs = _latest_blob_today("rainfall_")
    if rainfall_blobs:
        n_alerts += 1
        lines.append(":rotating_light: *Rainfall threshold exceeded*")
    else:
        lines.append(":large_green_circle: Rainfall threshold not exceeded")

    return "\n".join(lines), n_alerts


# ---------------------------------------------------------------------------
# GitHub Actions status check
# ---------------------------------------------------------------------------


def _query_github_runs(workflow_filename: str) -> list[dict]:
    token = os.environ["GH_TOKEN"]
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{workflow_filename}/runs"
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    response.raise_for_status()
    return response.json().get("workflow_runs", [])


def build_workflow_status(workflow_filename: str) -> str:
    name = workflow_filename.removesuffix(".yml")
    try:
        runs = _query_github_runs(workflow_filename)
    except Exception as e:
        logger.error(str(e))
        return f":red_circle: {name}: Failed to fetch status — {e}\n"

    today = datetime.date.today().isoformat()
    scheduled_today = [
        r
        for r in runs
        if r.get("event") == "schedule"
        and r.get("head_branch") == "main"
        and r.get("created_at", "").startswith(today)
    ]

    if len(scheduled_today) == 0:
        return f":heavy_minus_sign: {name}: No scheduled run today\n"

    run = scheduled_today[-1]
    conclusion = run.get("conclusion")
    run_url = f"https://github.com/{GITHUB_REPO}/actions/runs/{run['id']}"

    if conclusion == "success":
        return f":large_green_circle: {name}: Successful update\n"
    if conclusion == "failure":
        return f":red_circle: {name}: Failed — <{run_url}|Check logs>\n"
    return f":white_circle: {name}: Status `{conclusion}`\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    status_lines = []
    for workflow in WORKFLOWS:
        logger.info(f"Checking GitHub Actions status for {workflow}...")
        status_lines.append(build_workflow_status(workflow))

    status_text = "".join(status_lines)

    logger.info("Checking blob storage for active alerts...")
    signals_text, n_alerts = build_signals_text()

    header = build_header(n_alerts)
    post_message(header, signals_text, status_text)
    logger.info("Slack message posted successfully")


if __name__ == "__main__":
    main()
