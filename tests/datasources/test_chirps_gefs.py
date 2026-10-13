import datetime
from unittest import mock

import pandas as pd
import pytest
import requests

from src.datasources import chirps_gefs
from src.utils import constants

LEAD_TIME = constants.chirps_gefs_lead_time


def _blob_names(issue_date: str, n_files: int) -> list[str]:
    issue = pd.Timestamp(issue_date)
    return [
        "ds-aa-mmr-cyclones/raw/chirps_gefs/"
        + chirps_gefs._blob_filename(issue, issue + pd.Timedelta(days=i))
        for i in range(n_files)
    ]


def _listing(days: list[int]) -> mock.Mock:
    html = "".join(f'<a href="{d:02d}/">{d:02d}/</a>' for d in days)
    response = mock.Mock(text=f'<a href="../">../</a>{html}')
    response.raise_for_status.return_value = None
    return response


def test_source_for_switches_to_v3_on_july_first() -> None:
    assert (
        chirps_gefs._source_for(datetime.date(2026, 6, 30))
        is chirps_gefs._CHIRPS_GEFS_V12
    )
    assert (
        chirps_gefs._source_for(datetime.date(2026, 7, 1))
        is chirps_gefs._CHIRPS_GEFS_V3
    )


def test_file_urls_match_each_server_layout() -> None:
    kwargs = {
        "iss_year": 2026,
        "iss_month": 7,
        "iss_day": 1,
        "valid_year": 2026,
        "valid_month": 7,
        "valid_day": 3,
    }
    assert chirps_gefs._CHIRPS_GEFS_V3.file_url.format(**kwargs) == (
        "https://data.chc.ucsb.edu/products/CHIRPS-GEFS/v3/daily/global/"
        "2026/07/01/c3g_2026.07.03.tif"
    )
    assert chirps_gefs._CHIRPS_GEFS_V12.file_url.format(**kwargs) == (
        "https://data.chc.ucsb.edu/products/EWX/data/forecasts/"
        "CHIRPS-GEFS_precip_v12/daily_16day/2026/07/01/data.2026.0703.tif"
    )


def test_fetch_available_issue_dates_queries_the_matching_source() -> None:
    candidates = [pd.Timestamp("2026-06-30"), pd.Timestamp("2026-07-02")]
    with mock.patch.object(
        chirps_gefs.requests, "get", return_value=_listing([1, 2])
    ) as get:
        available = chirps_gefs._fetch_available_issue_dates(candidates)

    urls = sorted(call.args[0] for call in get.call_args_list)
    assert urls[0].startswith("https://data.chc.ucsb.edu/products/CHIRPS-GEFS/v3/")
    assert urls[1].startswith("https://data.chc.ucsb.edu/products/EWX/")
    assert datetime.date(2026, 7, 2) in available


def test_fetch_available_issue_dates_raises_on_403() -> None:
    response = mock.Mock(status_code=403)
    error = requests.HTTPError(response=response)
    with mock.patch.object(
        chirps_gefs.requests, "get", side_effect=error
    ), pytest.raises(RuntimeError, match="403"):
        chirps_gefs._fetch_available_issue_dates([pd.Timestamp("2026-07-02")])


@pytest.fixture
def run_download():
    """Run download_recent_chirps_gefs with storage and server mocked."""

    def _run(existing: list[str], available_days: list[int], **kwargs):
        with (
            mock.patch.object(chirps_gefs.codab, "load_codab_from_blob"),
            mock.patch.object(
                chirps_gefs.stratus, "list_container_blobs", return_value=existing
            ),
            mock.patch.object(
                chirps_gefs,
                "_fetch_available_issue_dates",
                return_value={
                    datetime.date(2026, 7, d) for d in available_days
                },
            ),
            mock.patch.object(chirps_gefs.time, "sleep"),
            mock.patch.object(chirps_gefs, "download_chirps_gefs") as download,
        ):
            chirps_gefs.download_recent_chirps_gefs(
                date=datetime.date(2026, 7, 5), **kwargs
            )
        return [
            (call.args[0].date(), call.args[1].date())
            for call in download.call_args_list
        ]

    return _run


def test_download_takes_newest_dates_first_and_respects_cap(run_download) -> None:
    calls = run_download(existing=[], available_days=[1, 2, 3, 4], max_issue_dates=2)

    issue_dates = list(dict.fromkeys(issue for issue, _ in calls))
    assert issue_dates == [datetime.date(2026, 7, 4), datetime.date(2026, 7, 3)]
    assert len(calls) == 2 * LEAD_TIME


def test_download_without_cap_takes_every_available_date(run_download) -> None:
    calls = run_download(existing=[], available_days=[1, 2, 3], max_issue_dates=None)

    assert len({issue for issue, _ in calls}) == 3


def test_complete_dates_are_skipped_and_incomplete_dates_topped_up(
    run_download,
) -> None:
    existing = _blob_names("2026-07-02", LEAD_TIME) + _blob_names(
        "2026-07-01", 4
    )

    calls = run_download(existing=existing, available_days=[1, 2], max_issue_dates=None)

    assert {issue for issue, _ in calls} == {datetime.date(2026, 7, 1)}
    assert len(calls) == LEAD_TIME - 4
    assert all(valid >= datetime.date(2026, 7, 5) for _, valid in calls)


def test_upload_errors_are_not_swallowed() -> None:
    response = mock.Mock(content=b"")
    response.raise_for_status.return_value = None
    with (
        mock.patch.object(chirps_gefs.requests, "get", return_value=response),
        mock.patch.object(chirps_gefs.rxr, "open_rasterio", side_effect=OSError("bad")),
        pytest.raises(OSError, match="bad"),
    ):
        chirps_gefs.download_chirps_gefs(
            pd.Timestamp("2026-07-01"), pd.Timestamp("2026-07-01"), (0, 0, 1, 1)
        )


def test_missing_remote_file_is_a_warning_not_an_error() -> None:
    response = mock.Mock(status_code=404)
    error = requests.HTTPError(response=response)
    with (
        mock.patch.object(chirps_gefs.requests, "get", side_effect=error),
        mock.patch.object(chirps_gefs.stratus, "upload_blob_data") as upload,
    ):
        chirps_gefs.download_chirps_gefs(
            pd.Timestamp("2026-07-01"), pd.Timestamp("2026-07-01"), (0, 0, 1, 1)
        )

    upload.assert_not_called()
