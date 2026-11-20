import datetime

import pandas as pd

from src.monitoring import slack_bot


def test_build_workflow_status_uses_latest_scheduled_run_of_the_day(monkeypatch) -> None:
    """Regression test for a stale-run bug in the Slack status update.

    When a workflow has two scheduled runs the same day (morning and
    evening), the Slack status must report the most recent one. The GitHub
    API returns runs newest-first, and a previous implementation picked the
    last item of that list assuming it was the newest, which actually
    surfaced the morning's stale run number and conclusion instead.
    """
    today = datetime.date.today().isoformat()
    runs = [
        {
            "event": "schedule",
            "head_branch": "main",
            "created_at": f"{today}T20:29:12Z",
            "id": 274,
            "conclusion": "failure",
        },
        {
            "event": "schedule",
            "head_branch": "main",
            "created_at": f"{today}T08:57:34Z",
            "id": 272,
            "conclusion": "success",
        },
    ]
    monkeypatch.setattr(slack_bot, "_query_github_runs", lambda _: runs)

    status = slack_bot.build_workflow_status("run_update_ecmwf.yml")

    assert "274" in status
    assert "272" not in status
    assert ":red_circle:" in status


def test_build_signals_text_names_the_area_of_each_exceedance(monkeypatch) -> None:
    """Wind and rainfall alerts state which area exceeded its threshold."""
    today = datetime.date.today().strftime("%Y-%m-%d")
    blobs = {
        "monitoring_": [f"p/monitoring_{today}.csv"],
        "wind_exceedance_": [f"p/wind_exceedance_rest_{today}_ecmwf.csv"],
        "rainfall_exceedance": [f"p/rainfall_exceedance_{today}.csv"],
        "rest_rainfall_exceedance": [f"p/rest_rainfall_exceedance_{today}.csv"],
    }
    frames = {
        blobs["monitoring_"][0]: pd.DataFrame({"sid": ["s1"], "storm_name": ["ALPHA"]}),
        blobs["wind_exceedance_"][0]: pd.DataFrame({"sid": ["s1"]}),
    }
    monkeypatch.setattr(slack_bot, "_latest_blob_today", lambda p: blobs.get(p, []))
    monkeypatch.setattr(
        slack_bot.stratus, "load_csv_from_blob", lambda blob_name: frames[blob_name]
    )

    text, n_alerts = slack_bot.build_signals_text()

    assert "Rest of Rakhine: ALPHA" in text
    assert "AA Target area: ALPHA" not in text
    assert "Rainfall threshold exceeded* — AA Target area, Rest of Rakhine" in text
    assert n_alerts == 3
