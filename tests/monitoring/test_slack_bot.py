import datetime

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
