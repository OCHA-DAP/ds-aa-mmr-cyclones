import datetime
import io
import math

import geopandas as gpd
import matplotlib as mpl
import matplotlib.cm as cm
import matplotlib.lines as mlines
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import ocha_stratus as stratus
import pandas as pd
from matplotlib.transforms import offset_copy

import src.utils.constants as constants
from src.utils.constants import ADM_LIST
from src.utils.logging import get_logger

logger = get_logger(__name__)

dpi = 400

def plot_map_storms(gdf, adm, analysis_suff , trace: bool = False, save: bool = False):
    adm_level = analysis_suff[0]
    adm_column = f"ADM{adm_level}_EN"

    mpl.rcParams["hatch.linewidth"] = 0.3

    fig, ax = plt.subplots(dpi=300)

    if trace:
        # Factorize sid and map to storm_id
        sid_codes, sid_uniques = pd.factorize(gdf["sid"])
        cmap = plt.get_cmap("tab20", len(sid_uniques))

        # Map sid to storm_id (take first occurrence for each sid)
        sid_to_stormid = (
            gdf.drop_duplicates("sid")
            .set_index("sid")["storm_id"]
            .to_dict()
        )

        # Scatter plot
        ax.scatter(
            gdf["longitude"], gdf["latitude"],
            c=sid_codes, cmap=cmap, s=10
        )

        # Create legend with storm_id labels
        handles = [
            mpatches.Patch(color=cmap(i), label=str(sid_to_stormid.get(sid_uniques[i], sid_uniques[i])))
            for i in range(len(sid_uniques))
        ]
        ax.legend(
            handles=handles,
            title="Storm Identifier",
            bbox_to_anchor=(1.05, 1),
            loc="upper left",
            fontsize=6,
            title_fontsize=7,
            frameon=True
        )
        suptitle = "Myanmar storm tracks since 2006"
        title = "colored per storm_id"

    else:
        # Blue = offshore, Red = landfall
        gdf_offshore = gdf[~gdf["landfall"]]
        gdf_landfall = gdf[gdf["landfall"]]

        ax.scatter(gdf_offshore["longitude"], gdf_offshore["latitude"],
                   color="dodgerblue", s=10, label="No Landfall in Rakhine")
        ax.scatter(gdf_landfall["longitude"], gdf_landfall["latitude"],
                   color="crimson", s=10, label="Landfall in Rakhine")

        ax.legend(
            title="Landfall",
            bbox_to_anchor=(1.05, 1),
            loc="upper left",
            fontsize=7,
            title_fontsize=8,
            frameon=True
        )
        suptitle = "Myanmar storm tracks since 2006"
        title = "highlighted landfall points in Rakhine"

    adm.boundary.plot(linewidth=0.2, ax=ax, color="k")
    # Color specific ADM1s by filtering
    if int(adm_level)<3:
        # Color specific ADMs by filtering
        adm.loc[adm[adm_column].isin(ADM_LIST)].plot(
        ax=ax,
        facecolor="none",   # keep background transparent
        edgecolor="k",      # keep same edge if you want
        hatch="///",        # diagonal lines, try '\\\\', 'xx', '---'
        linewidth=0.2
        )
    else:
        # Color specific ADMs by filtering
        adm.loc[adm[adm_column].isin(ADM_LIST)].plot(ax=ax,
        color="lightblue", edgecolor="k", linewidth=0.2, alpha=0.5
        )
    plt.title(title, fontsize=8)
    plt.suptitle(suptitle, fontsize=10)
    ax.axis("off")


    if save:
        plt.savefig(f"mmr_trace_{trace}_{analysis_suff}.png", bbox_inches="tight", dpi=dpi)
    plt.show()


def plot_map_storms_speed_area_interest(gdf, adm, analysis_suff,  save: bool = False):
    adm_level = analysis_suff[0]
    adm_column = f"ADM{adm_level}_EN"

    mpl.rcParams["hatch.linewidth"] = 0.3

    fig, ax = plt.subplots(dpi=300)


    # ✅ Keep only rows where landfall is False
    gdf = gdf[gdf["landfall"] == False]

    # Turn sid into numeric codes
    sid_codes, sid_uniques = pd.factorize(gdf["sid"])
    cmap = plt.get_cmap("tab20", len(sid_uniques))

    # Plot, coloring by sid codes
    gdf.drop(columns="geometry").plot(
        x="longitude", y="latitude",
        ax=ax, kind="scatter",
        c=sid_codes, cmap=cmap, s=10,
        colorbar=False
    )

    gdf.plot(
        x="nearest_lon", y="nearest_lat",
        ax=ax, kind="scatter",
        c=sid_codes, cmap=cmap, s=10,
        colorbar=False, marker="v"
    )


    adm.boundary.plot(linewidth=0.2, ax=ax, color="k")

    if int(adm_level)<3:
        # Color specific ADMs by filtering
        adm.loc[adm[adm_column].isin(ADM_LIST)].plot(
        ax=ax,
        facecolor="none",   # keep background transparent
        edgecolor="k",      # keep same edge if you want
        hatch="///",        # diagonal lines, try '\\\\', 'xx', '---'
        linewidth=0.2
        )
    else:
        # Color specific ADMs by filtering
        adm.loc[adm[adm_column].isin(ADM_LIST)].plot(ax=ax,
        color="lightblue", edgecolor="k", linewidth=0.2, alpha=0.5
        )
    # --- Legend for marker meaning ---
    import matplotlib.lines as mlines
    dot_marker = mlines.Line2D(
        [], [], color="black", marker="o", linestyle="None", markersize=6,
        label="Storm point producing max speed at land"
    )
    triangle_marker = mlines.Line2D(
        [], [], color="black", marker="v", linestyle="None", markersize=6,
        label="Point with max speed at land"
    )
    legend = ax.legend(
        handles=[dot_marker, triangle_marker],
        loc="upper center",
        bbox_to_anchor=(0.5, -0.08),  # move below plot
        ncol=2,  # put both markers side by side
        frameon=True,
        fontsize=6
    )
    ax.axis("off")

    if save:
        plt.savefig(f"mmr_max_speed_{analysis_suff}.png", bbox_inches="tight", dpi=dpi)
    plt.show()


def overview_situation(df, analysis_suff, y_column:str="3days_rain_mean",save:bool = False, adm_level = 1, cerf=False, title_suff:str=""):
    adm_column = f"ADM{adm_level}_EN"
    unique_adms = df[adm_column].unique()
    n_adms = len(unique_adms)

    if cerf:
        color_column = "cerf_allocation"
        cerf_allocation = df["cerf_allocation"].unique()
        color_map = {cerf_allocation[0]: "tab:blue", cerf_allocation[1]: "tab:orange"}
    else:
        color_column = adm_column
        # Choose colors
        if n_adms == 1:
            color_map = {unique_adms[0]: "tab:blue"}
        elif n_adms == 2:
            # Pick two colors that are visually distinct
            color_map = {unique_adms[0]: "tab:blue", unique_adms[1]: "tab:orange"}
        else:
            # Use tab20 colormap for more than 2 categories
            cmap = plt.get_cmap("tab10", n_adms)
            color_map = {adm: cmap(i) for i, adm in enumerate(unique_adms)}

    # --- Plot ---
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot circles (landfall == False)
    mask_circle = ~df["landfall"]
    ax.scatter(
        df.loc[mask_circle, "max_wind_speed_land"],
        df.loc[mask_circle, y_column],
        c=df.loc[mask_circle, color_column].map(color_map),
        marker="o", alpha=0.7, edgecolor="k", label=None
    )

    # Plot squares (landfall == True)
    mask_square = df["landfall"]
    ax.scatter(
        df.loc[mask_square, "max_wind_speed_land"],
        df.loc[mask_square, y_column],
        c=df.loc[mask_square, color_column].map(color_map),
        marker="s", alpha=0.7, edgecolor="k", label=None
    )

    # Axis labels
    ax.set_xlabel("Max wind speed [knots]", fontsize=12)
    ax.set_ylabel("3 Days mean precipitation [mm/per unit area]", fontsize=12)

    # ✅ Add storm names as text
    for _, row in df.iterrows():
        ax.text(
            row["max_wind_speed_land"], row[y_column],
            str(row["storm_name"]),
            fontsize=8, transform=offset_copy(ax.transData, x=3, y=3, units='points', fig=fig)
        )

    # Add vertical category lines
    bins = [0, 16, 27, 33, 47, 63, 89, 119, float("inf")]
    labels = [
        "Below Depression",
        "Depression",
        "Deep Depression",
        "Cyclonic Storm",
        "Severe Cyclonic Storm",
        "Very Severe Cyclonic Storm",
        "Extremely Severe Cyclonic Storm",
        "Super Cyclonic Storm",
    ]

    for b, label in zip(bins[1:], labels):  # skip first (0)
        ax.axvline(b, color="grey", linestyle="--", alpha=0.7)
        # ax.text(
        #     b, ax.get_ylim()[1], label,
        #     rotation=90, verticalalignment="top", horizontalalignment="right",
        #     fontsize=9, color="grey"
        # )
    # Horizontal text labels between lines
    y_top = ax.get_ylim()[1] * 0.98  # consistent height near the top

    for left, right, label in zip(bins[:-1], bins[1:], labels):
        if right == float("inf"):
            continue

        midpoint = (left + right) / 2

        ax.text(
            midpoint,
            y_top,
            label,
            rotation=90,
            ha="center",  # center horizontally between vertical lines
            va="top",  # anchor near top
            fontsize=9,
            color="grey"
        )
    # Legend for ADM1_EN
    for adm1, color in color_map.items():
        ax.scatter([], [], color=color, label=adm1)

    # Legend for shapes (landfall)
    ax.scatter([], [], color="k", marker="o", label="No landfall")
    ax.scatter([], [], color="k", marker="s", label="Landfall")

    ax.legend(title="Legend", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.title(f"{title_suff}")
    plt.tight_layout()
    if save:
        plt.savefig(f"mmr_overview_{analysis_suff}_{title_suff}.png", bbox_inches="tight", dpi=dpi)
    plt.show()



def plot_storm_track_comparison(
    df_observed: pd.DataFrame,
    df_forecasted: pd.DataFrame,
    adm: gpd.GeoDataFrame,
    adm_column: str = "ADM1_EN",
    save: bool = False,
):
    """
    Plot observed (red line with triangles) and individual forecasted tracks (colored line with circles)
    for each issued_time. Up to 4 subplots per figure are generated.
    """

    # --- Setup ---
    df_observed = df_observed.sort_values(by="valid_time").reset_index(drop=True)
    df_forecasted["issued_time"] = pd.to_datetime(df_forecasted["issued_time"])
    df_forecasted = df_forecasted.sort_values("issued_time")

    storm_name = df_forecasted["storm_name"].iloc[0]
    unique_times = df_forecasted["issued_time"].unique()
    n_times = len(unique_times)

    # Discrete colormap (reversed so oldest first)
    cmap = cm.get_cmap("viridis_r", n_times)

    # --- Plot in groups of 4 ---
    plots_per_fig = 4
    n_figs = math.ceil(n_times / plots_per_fig)

    for fig_idx in range(n_figs):
        start = fig_idx * plots_per_fig
        end = min((fig_idx + 1) * plots_per_fig, n_times)
        times_subset = unique_times[start:end]
        n_subplots = len(times_subset)

        fig, axes = plt.subplots(
            nrows=2, ncols=2, figsize=(10, 10), constrained_layout=True
        )
        axes = axes.flatten()

        for i, issued_time in enumerate(times_subset):
            ax = axes[i]

            # --- Plot administrative boundaries ---
            adm.boundary.plot(linewidth=0.4, ax=ax, color="black", zorder=1)

            # Optional: highlight ADM areas if global ADM_LIST exists
            if "ADM_LIST" in globals():
                adm.loc[adm[adm_column].isin(ADM_LIST)].plot(
                    ax=ax,
                    facecolor="none",
                    edgecolor="k",
                    hatch="///",
                    linewidth=0.2,
                )

            # --- Plot observed track ---
            ax.plot(
                df_observed["longitude"],
                df_observed["latitude"],
                color="red",
                marker="^",
                markersize=5,
                linewidth=1.8,
                label="Observed",
                zorder=3,
            )

            # --- Plot forecasted track (sorted by valid_time_forecasted) ---
            df_seg = df_forecasted[df_forecasted["issued_time"] == issued_time].copy()
            df_seg = df_seg.sort_values("valid_time_forecasted")

            ax.plot(
                df_seg["longitude"],
                df_seg["latitude"],
                color=cmap(i / n_times),
                marker="o",
                markersize=4,
                linewidth=1.5,
                label="Forecasted",
                zorder=4,
            )

            # --- Title per subplot ---
            ax.set_title(
                f"Issued: {issued_time.strftime('%Y-%m-%d %H:%M')}",
                fontsize=10,
            )
            ax.axis("off")

        # Hide unused subplots
        for j in range(n_subplots, 4):
            axes[j].axis("off")

        # --- Legend at the bottom ---
        obs_marker = mlines.Line2D([], [], color="red", marker="^", linestyle="-", markersize=6, label="Observed")
        fc_marker = mlines.Line2D([], [], color="black", marker="o", linestyle="-", markersize=6, label="Forecasted")
        fig.legend(
            handles=[obs_marker, fc_marker],
            loc="lower center",
            ncol=2,
            frameon=True,
        )

        fig.suptitle(f"{storm_name} — Observed vs Forecasted Tracks", fontsize=13)

        if save:
            plt.savefig(f"{storm_name}_tracks_page_{fig_idx+1}.png", bbox_inches="tight", dpi=dpi)

        plt.show()

def plot_rainfall_forecast(df, dataprovider:str, save:bool = True):
    storm_name = df.storm_name.iloc[0]
    fig, ax = plt.subplots(dpi=300)
    df = df.sort_values(["issued_date", "valid_date"])

    rainfall_observed = df["3days_rain_mean"].iloc[0]
    landfall_date = df["landfall_adm0_date"].unique()[0]
    # Unique issued dates
    issued_dates = df["issued_date"].unique()
    # Plot one line per issued_date
    for issued in issued_dates:
        sub = df[df["issued_date"] == issued]
        ax.plot(
            sub["valid_date"],
            sub["rolling_sum_3"],
            label=f"Issued {issued.date()}",
            linewidth=2
        )
    ax.axvline(x=landfall_date)
    ax.scatter(x=landfall_date, y=rainfall_observed, color="red", s=10, zorder=3)
    plt.title(f"Rolling 3-day Sum for Storm: {storm_name}")
    plt.xlabel("Valid Date")
    plt.ylabel("Rolling Sum (3 days)")
    plt.xticks(rotation=90)

    plt.grid(True, alpha=0.3)
    # Move legend outside the plot
    plt.legend(
        title="Issued Date",
        bbox_to_anchor=(1.05, 1),
        loc="upper left"
    )
    plt.tight_layout()
    if save:
        plt.savefig(f"{storm_name}_rainfall_forecast_{dataprovider}.png", bbox_inches="tight", dpi=dpi)
    plt.show()


def plot_chirps_gefs_forecast(
    df: pd.DataFrame,
    today: str | None = None,
    file_name: str | None = None,
    save: bool = True,
):
    """Create and upload a stacked bar chart of CHIRPS-GEFS forecast for Rakhine.

    Displays the 3-day rolling sum of precipitation per forecast date for the
    most recent issue date. Each bar is split into daily contributions (day t,
    t-1, t-2), and a dashed threshold line marks the rainfall alert level.

    Args:
        df: DataFrame with columns 'issue_date', 'valid_date', and 'mean'.
            May optionally include a pre-computed 'rolling_sum_3' column.
        today: Date string (YYYY-MM-DD) used in the blob file name.
            Defaults to today's date.
        file_name: Override the auto-generated blob file name.

    Returns:

    """
    if today is None:
        today = datetime.date.today().strftime("%Y-%m-%d")

    df = df.copy()
    if "rolling_sum_3" not in df.columns:
        df = df.sort_values(["issue_date", "valid_date"])
        df["rolling_sum_3"] = (
            df.groupby("issue_date")["mean"]
            .rolling(3, min_periods=1)
            .sum()
            .reset_index(level=0, drop=True)
        )

    latest_issue = df["issue_date"].max()
    df_plot = (
        df[df["issue_date"] == latest_issue]
        .sort_values("valid_date")
        .reset_index(drop=True)
    )

    # Stacked segments: contributions from d-2, d-1, and d to the rolling sum
    df_plot["contrib_d2"] = df_plot["mean"].shift(periods=2, fill_value=0.0)
    df_plot["contrib_d1"] = df_plot["mean"].shift(periods=1, fill_value=0.0)
    df_plot["contrib_d0"] = df_plot["mean"]

    x_pos = range(len(df_plot))
    bar_width = 0.6
    colors = ["#8ecae6", "#f4a261", "#a0522d"]
    date_labels = pd.to_datetime(df_plot["valid_date"]).dt.strftime("%Y-%m-%d")

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.bar(x_pos, df_plot["contrib_d2"], bar_width, color=colors[0], label="Day t+2")
    ax.bar(
        x_pos,
        df_plot["contrib_d1"],
        bar_width,
        bottom=df_plot["contrib_d2"],
        color=colors[1],
        label="Day t+1",
    )
    ax.bar(
        x_pos,
        df_plot["contrib_d0"],
        bar_width,
        bottom=df_plot["contrib_d2"] + df_plot["contrib_d1"],
        color=colors[2],
        label="Day t",
    )

    for i, row in df_plot.iterrows():
        ax.text(
            i,
            row["rolling_sum_3"] + 1,
            str(int(round(row["rolling_sum_3"]))),
            ha="center",
            va="bottom",
            fontsize=8,
        )

    threshold = constants.rainfall_alert_level_forecast
    ax.axhline(y=threshold, color="crimson", linestyle="--", linewidth=1.5)
    ax.text(
        len(df_plot) - 0.5,
        threshold + 2,
        f"Threshold: {threshold} mm",
        color="crimson",
        fontsize=9,
        ha="right",
    )

    ax.set_xticks(list(x_pos))
    ax.set_xticklabels(date_labels, rotation=45, ha="right", fontsize=8)
    ax.set_xlabel("Date")
    ax.set_ylabel("Total precipitation over 3 days (mm) - forecasted")
    ax.set_title(
        f"Total precipitation forecasted over 3 days, for Rakhine\n"
        f"(issued: {pd.Timestamp(latest_issue).strftime('%Y-%m-%d')})"
    )
    ax.legend(title="Date", loc="upper left")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    plt.close()

    if file_name is None:
        file_name = f"rainfall_forecast_plot_{today}.png"
    if save:
        stratus.upload_blob_data(
            data=buf,
            blob_name=file_name,
            stage="dev",
            container_name=(
                f"projects/{constants.PROJECT_PREFIX}/processed/rainfall_forecast_plot"
            ),
        )
        logger.info(f"Rainfall forecast plot uploaded to blob storage: {file_name}")

    return fig, ax
