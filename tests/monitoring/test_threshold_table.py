from src.utils.listmonk import _render_threshold_table


def test_render_threshold_table_has_areas_and_rows():
    html = _render_threshold_table(
        {"Wind speed (ECMWF)": {"A": "REACHED", "B": "NOT REACHED"}}
    )
    assert "<th" in html and ">A<" in html and ">B<" in html
    assert "Wind speed (ECMWF)" in html and "NOT REACHED" in html


def test_render_threshold_table_empty():
    assert _render_threshold_table(None) == ""
