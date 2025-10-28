import geopandas as gpd
import matplotlib as mpl
import pandas as pd
import math

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.cm as cm
import matplotlib.lines as mlines


from matplotlib.transforms import offset_copy

from src.utils.constants import ADM_LIST

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
        suptitle = "Myanmar storm tracks after year 2000"
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
        suptitle = "Myanmar storm tracks after year 2000"
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


def overview_situation(df, analysis_suff, save:bool = False, adm_level = 1, cerf=False):
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
        df.loc[mask_circle, "3days_rain_mean"],
        c=df.loc[mask_circle, color_column].map(color_map),
        marker="o", alpha=0.7, edgecolor="k", label=None
    )

    # Plot squares (landfall == True)
    mask_square = df["landfall"]
    ax.scatter(
        df.loc[mask_square, "max_wind_speed_land"],
        df.loc[mask_square, "3days_rain_mean"],
        c=df.loc[mask_square, color_column].map(color_map),
        marker="s", alpha=0.7, edgecolor="k", label=None
    )

    # Axis labels
    ax.set_xlabel("Max wind speed [knots]", fontsize=12)
    ax.set_ylabel("3 Days mean precipitation [mm/per unit area]", fontsize=12)

    # ✅ Add storm names as text
    for _, row in df.iterrows():
        ax.text(
            row["max_wind_speed_land"], row["3days_rain_mean"],
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
        ax.text(
            b, ax.get_ylim()[1], label,
            rotation=90, verticalalignment="top", horizontalalignment="right",
            fontsize=9, color="grey"
        )

    # Legend for ADM1_EN
    for adm1, color in color_map.items():
        ax.scatter([], [], color=color, label=adm1)

    # Legend for shapes (landfall)
    ax.scatter([], [], color="k", marker="o", label="No landfall")
    ax.scatter([], [], color="k", marker="s", label="Landfall")

    ax.legend(title="Legend", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    if save:
        plt.savefig(f"mmr_overview_{analysis_suff}.png", bbox_inches="tight", dpi=dpi)
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
