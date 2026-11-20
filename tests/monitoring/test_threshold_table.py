import pandas as pd

from src.utils.listmonk import (
    _render_threshold_table,
    _render_township_sections,
    generate_body_email,
)

_INFO = {
    "wind_speed_threshold_reached": "REACHED",
    "rainfall_threshold_reached": "NOT REACHED",
}


def _townships(names: list[str], wind: float, rainfall: float) -> pd.DataFrame:
    return pd.DataFrame({"township": names, "wind": wind, "rainfall": rainfall})


def test_render_threshold_table_has_columns_and_rows():
    html = _render_threshold_table(
        {"Wind speed (ECMWF)": {"Threshold": "47 kt", "Status": "NOT REACHED"}}
    )
    assert "<th" in html and ">Threshold<" in html and ">Status<" in html
    assert "Wind speed (ECMWF)" in html and "47 kt" in html


def test_render_threshold_table_empty():
    assert _render_threshold_table(None) == ""


def test_township_sections_action_phase():
    html = _render_township_sections(
        {
            "AA Target area": _townships(["Sittwe"], wind=50.0, rainfall=10.0),
            "Rest of Rakhine": _townships(["Thandwe"], wind=48.0, rainfall=10.0),
        },
        phase="action",
    )
    assert "should now commence in the following townships prioritized" in html
    assert "Additional information:" in html
    assert html.index("Sittwe") < html.index("Additional information:")
    assert html.count("<table") == 1 and "Thandwe." in html
    assert "<strong>50</strong>" in html and "<td" in html


def test_township_sections_without_rest_townships_skips_additional_info():
    html = _render_township_sections(
        {
            "AA Target area": _townships(["Sittwe"], wind=50.0, rainfall=10.0),
            "Rest of Rakhine": _townships([], wind=0.0, rainfall=0.0),
        },
        phase="action",
    )
    assert "Sittwe" in html and "Additional information" not in html


def test_township_sections_empty():
    assert _render_township_sections(None, phase="action") == ""
    assert _render_township_sections({}, phase=None) == ""


def test_body_starts_with_greeting_in_every_phase():
    for phase in (None, "readiness", "action", "observational"):
        body = generate_body_email("TEST", "12h00", info=_INFO, phase=phase)
        assert body.strip().startswith("Dear colleagues,")
        assert body.count("Dear colleagues,") == 1
