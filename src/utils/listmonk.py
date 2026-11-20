import base64
import os
from typing import Dict, List

import pandas as pd
import requests

from src.utils import constants

BASE_URL = "https://listmonk-demo-afhcg8e2hde0fxca.eastus2-01.azurewebsites.net/api"  # noqa

GIULIA_ONLY_LIST_ID = 21
GIULIA_SUBSCRIBER_ID = "giulia.martini@un.org"
#DSCI_LIST_ID = 6

BASE_CAMPAIGN_ID = 100
BASE_TRANSACTIONAL_ID = 100

USERNAME_LISTMONK = os.getenv("DSCI_LISTMONK_API_USERNAME")
PASSWORD_LISTMONK = os.getenv("DSCI_LISTMONK_API_KEY")


def create_campaign(
    name: str = "test_campaign",
    subject: str = "Test Subject",
    list_ids: list[int] = None,
    template_id: int = BASE_CAMPAIGN_ID,
    body: str = "TEST CONTENT",
):
    if list_ids is None:
        list_ids = [GIULIA_ONLY_LIST_ID]
    create_payload = {
        "name": name,
        "subject": subject,
        "lists": list_ids,  # list IDs to send to
        "template_id": template_id,  # your template ID
        "type": "regular",
        "content_type": "html",
        "body": body,
    }

    r = requests.post(
        f"{BASE_URL}/campaigns", auth=(USERNAME_LISTMONK, PASSWORD_LISTMONK), json=create_payload
    )

    r.raise_for_status()
    campaign = r.json()["data"]
    campaign_id = campaign["id"]
    return campaign_id


def send_campaign(campaign_id: int):
    r = requests.put(
        f"{BASE_URL}/campaigns/{campaign_id}/status",
        auth=(USERNAME_LISTMONK, PASSWORD_LISTMONK),
        json={"status": "running"},
    )
    r.raise_for_status()


def send_transactional(
    to_emails: list[tuple[str, str]],
    subject: str,
    template_id: int = BASE_TRANSACTIONAL_ID,
    cc_emails: list[tuple[str, str]] = None,
    data: dict = None,
):
    payload = {
        "subscriber_email": to_emails[0][1],  # primary recipient
        "template_id": template_id,
        "from_email": "OCHA Data Science <ocha-datascience@un.org>",
        "content_type": "html",
        "subject": subject,
        "data": data or {},
        "headers": [
            {
                "To": ", ".join(
                    [f"{name} <{email}>" for name, email in to_emails]
                )
            },
            {
                "Cc": ", ".join(
                    [f"{name} <{email}>" for name, email in cc_emails or []]
                )
            },
        ],
    }

    r = requests.post(
        f"{BASE_URL}/tx", auth=(USERNAME_LISTMONK, PASSWORD_LISTMONK), json=payload
    )
    if not r.ok:
        print("Status:", r.status_code)
        print("Response text:", r.text)
        print(
            "Response JSON:",
            r.json()
            if "application/json" in r.headers.get("Content-Type", "")
            else None,
        )
        print("Payload sent:", payload)

    r.raise_for_status()
    return r.json()

_PHASE_INTROS: Dict[str, str] = {
    "readiness": (
        "The <strong>readiness threshold</strong> has been reached for Cyclone {storm_name}. "
        "The forecast indicates landfall in approximately 3&ndash;5 days. "
        "Readiness activities in line with the anticipatory action framework should now commence."
    ),
    "action": (
        "The <strong>action threshold</strong> has been reached for Cyclone {storm_name}. "
        "The forecast indicates landfall within approximately 72 hours. "
        "All anticipatory actions in line with the framework should now commence."
    ),
    "observational": (
        "Cyclone {storm_name} is now in the <strong>observational phase</strong>. "
        "The situation is being closely monitored."
    ),
}


_TABLE_CELL = "border: 1px solid #ccc; padding: 4px 10px;"

_AA_TOWNSHIP_LEADS: Dict[str | None, str] = {
    "readiness": (
        "Readiness activities in line with the anticipatory action framework "
        "should now commence in the following townships prioritized for "
        "anticipatory action:"
    ),
    "action": (
        "All anticipatory actions in line with the framework should now "
        "commence in the following townships prioritized for anticipatory "
        "action:"
    ),
}
_AA_TOWNSHIP_LEAD_DEFAULT = (
    "The trigger thresholds are currently projected to be met in the "
    "following townships prioritized for anticipatory action:"
)
_REST_TOWNSHIP_LEAD = (
    "<strong>Additional information:</strong> The trigger thresholds are "
    "currently also projected to be met in the following townships in "
    "Rakhine not prioritized for anticipatory action under the framework:"
)


def _format_forecast(value: float, threshold: float) -> str:
    """Format a forecast value, in bold when it reaches the threshold.

    Args:
        value: Forecast value, NaN when there is no data.
        threshold: Threshold above which the value is highlighted.

    Returns:
        HTML string, "n/a" when the value is missing.
    """
    if pd.isna(value):
        return "n/a"
    text = f"{value:.0f}"
    return f"<strong>{text}</strong>" if value >= threshold else text


def _render_township_table(townships: pd.DataFrame) -> str:
    """Render the townships reaching a trigger threshold as an HTML table.

    Args:
        townships: DataFrame with 'township', 'wind' (knots) and 'rainfall'
            (mm, 3-day sum) columns. Values at or above their threshold are
            shown in bold.

    Returns:
        HTML table string.
    """
    header = "".join(
        f'<th style="{_TABLE_CELL}">{title}</th>'
        for title in (
            "Township",
            "Wind speed forecast (kt)",
            "3-day rainfall forecast (mm)",
        )
    )
    rows = "".join(
        f'<tr><td style="{_TABLE_CELL}">{row.township}</td>'
        f'<td style="{_TABLE_CELL}">'
        f"{_format_forecast(row.wind, constants.wind_speed_alert_level)}</td>"
        f'<td style="{_TABLE_CELL}">'
        f"{_format_forecast(row.rainfall, constants.rainfall_alert_level_forecast)}</td></tr>"
        for row in townships.itertuples()
    )
    return f'<table style="border-collapse: collapse;"><tr>{header}</tr>{rows}</table>'


def _render_township_sections(
    townships_reached: Dict[str, pd.DataFrame] | None, phase: str | None
) -> str:
    """Render the AA township table and the other townships as a sentence.

    Args:
        townships_reached: Mapping of area label to the DataFrame of townships
            reaching a threshold in that area, or None.
        phase: Trigger phase, used to word the lead sentence.

    Returns:
        HTML string, empty when no township reached a threshold.
    """
    if not townships_reached:
        return ""
    target = townships_reached.get(constants.AREA_LABELS["adm3"])
    rest = townships_reached.get(constants.AREA_LABELS["rest"])
    sections = []
    if target is not None and not target.empty:
        lead = _AA_TOWNSHIP_LEADS.get(phase, _AA_TOWNSHIP_LEAD_DEFAULT)
        sections.append(f"{lead}<br>{_render_township_table(target)}")
    if rest is not None and not rest.empty:
        sections.append(f"{_REST_TOWNSHIP_LEAD} {', '.join(rest['township'])}.")
    return "".join(f"<br>{section}<br>" for section in sections)


def _render_threshold_table(
    threshold_table: Dict[str, Dict[str, str]] | None,
) -> str:
    """Render the threshold status as an HTML table.

    Args:
        threshold_table: Mapping of row label to {column label: value}, or
            None.

    Returns:
        HTML table string, or an empty string when there is no table.
    """
    if not threshold_table:
        return ""
    columns = list(next(iter(threshold_table.values())))
    header = "".join(f'<th style="{_TABLE_CELL}">{column}</th>' for column in columns)
    rows = "".join(
        f'<tr><td style="{_TABLE_CELL}">{label}</td>'
        + "".join(
            f'<td style="{_TABLE_CELL}">{values[column]}</td>' for column in columns
        )
        + "</tr>"
        for label, values in threshold_table.items()
    )
    return (
        '<br><table style="border-collapse: collapse;">'
        f'<tr><th style="{_TABLE_CELL}"></th>{header}</tr>{rows}</table>'
    )


def generate_body_email(
    storm_name,
    date_myanmar,
    info: Dict = None,
    plot_bytes: List[bytes] | None = None,
    phase: str | None = None,
    threshold_table: Dict[str, Dict[str, str]] | None = None,
    townships_reached: Dict[str, pd.DataFrame] | None = None,
):
    """Generate HTML email body for cyclone monitoring/trigger emails.

    Args:
        storm_name: Name of the storm.
        date_myanmar: Formatted datetime string in Myanmar local time.
        info: Dict with keys 'wind_speed_threshold_reached' and
            'rainfall_threshold_reached'.
        plot_bytes: list of Raw PNG bytes of the needed plot, or None.
        phase: Trigger phase - 'readiness', 'action', 'observational', or
            None for a generic monitoring email.
        threshold_table: Optional mapping of row label to {column label:
            value}, rendered as a table of the threshold, worst forecast and
            status of each trigger component.
        townships_reached: Optional mapping of area label to the townships
            reaching a threshold there, rendered as tables.

    Returns:
        HTML string for the email body.
    """
    if phase in _PHASE_INTROS:
        phase_text = _PHASE_INTROS[phase].format(storm_name=storm_name)
        intro_body = (
            f"{phase_text}<br><br>"
            f"Please find more information below.<br>"
        )
    else:
        intro_body = (
            "A new forecast track has been released by ECMWF (European Centre for "
            "Medium-Range Weather Forecasts). Also included is the forecasted "
            "precipitation from CHIRPS-GEFS.<br>"
            "Please find more information below.<br>"
        )

    HTML_INTRO = f"""
    Dear colleagues,<br><br>
    {storm_name} - {date_myanmar} (Myanmar local time) <br>
    {intro_body}
    Wind speed threshold: {info["wind_speed_threshold_reached"]}<br>
    Precipitation threshold: {info["rainfall_threshold_reached"]}<br>
    {_render_threshold_table(threshold_table)}
    {_render_township_sections(townships_reached, phase)}
    <br><br>
    """
    HTML_PLOT = ""
    if plot_bytes is not None:
        for plot in plot_bytes:
            if plot is not None:
                encoded = base64.b64encode(plot).decode("ascii")
                HTML_PLOT = HTML_PLOT + f"""
                <div style="text-align: center;">
                    <img src="data:image/png;base64,{encoded}" alt="Missing Plot" style="max-width: 80%; height: auto;">
                </div>
                <br><br>
                """

    HTML_CONCLUSION = """
    <br>
    This email is purely informational and does not serve as an official notice for the anticipatory action framework. Official activation notices are sent in another email.
    <br>
    The code used to produce this alert is available on GitHub <a  href="https://github.com/OCHA-DAP/ds-aa-mmr-cyclones/">here</a>.
    <br><br>
    Best regards,<br>
    OCHA Centre for Humanitarian Data
    """
    return HTML_INTRO + HTML_PLOT + HTML_CONCLUSION