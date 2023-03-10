from datetime import date
from pathlib import Path

import geopandas
import pandas as pd

from data_common.db.duck import DuckQuery
from data_common.pandas import GovLayers


def create_2022_council_percentages_pop():
    """
    This creates an alternate lookup using population
    Basically using the onspd, we know the mapping of postcodes to lsoas, local authorities, and pcons
    Using the lsoa population, we can work out the rough population of each postcode
    We then sum this back up for pcons, and for each overlap between pcons and local authorities
    """
    print("Calcuating population")

    duck = DuckQuery()

    onspd = Path("data", "raw", "ONSPD_NOV_2022_UK_reduced.parquet")
    lsoa_pop_df = pd.read_csv(Path("data", "raw", "2019_population.csv"), thousands=",")

    duck.register("onspd", onspd)
    duck.register("lsoa_pop", lsoa_pop_df)

    # get the count of how many postcodes in each lsoa

    duck.query(
        "SELECT lsoa11 as lsoa, count(distinct pcd) as count FROM onspd group by all"
    ).to_view("lsoa_count").df().head()

    # calculate average population per postcode -
    # where this is 0, set to 1 (areas with high commerical, low residence, roughly this works out fine)
    query = """
    select
        lsoa,
        pop,
        count,
        case when cast(pop as float)/cast(count as float) = 0 then 1 else cast(pop as float)/cast(count as float) end as average_pop_per_postcode
    from
        lsoa_count
    join
        lsoa_pop using(lsoa)
    """

    duck.query(query).to_view("lsoa_count_pop")

    # sum up the average population per postcode for each pcon
    query = """
    select
        pcon,
        sum(average_pop_per_postcode) as pcon_pop
    from
        onspd
    join
        lsoa_count_pop on (onspd.lsoa11 = lsoa_count_pop.lsoa)
    group by
        ALL
    """

    duck.query(query).to_view("pcon_pop")

    # this is the poplation overlap between the pcon and the la
    query = """
    select
        pcon,
        oslaua,
        sum(average_pop_per_postcode) as pop_overlap
    from
        onspd
    join
        lsoa_count_pop on (onspd.lsoa11 = lsoa_count_pop.lsoa)
    group by
        ALL
    order by
        pcon
    """

    duck.query(query).to_view("pop_overlap")

    # finally we can get the percentage of the pcon population that overlaps with the la

    query = """
    select
        pcon as PCON21CD,
        oslaua as LAD21CD,
        pop_overlap/pcon_pop as percentage_overlap
    from
        pop_overlap
    join
        pcon_pop using (pcon)
    """

    df = duck.query(query).df()

    df.to_csv(
        Path("data", "interim", "percentage_overlap_2022_councils_pop.csv"),
        index=False,
    )

    df_2023 = update_to_2023(df)
    df_2023.to_csv(
        Path("data", "interim", "percentage_overlap_2023_councils_pop.csv"),
        index=False,
    )


def overlap(row: pd.Series) -> float:
    pc_geo = row["pc_geo"]
    la_geo = row["la_geo"]
    intersect = pc_geo.intersection(la_geo)
    percentage = intersect.area / pc_geo.area
    return percentage


def create_2022_council_percentages_area():
    """
    Create overlap of area between constituencies and local authorities as of 2022 from shapefile
    """
    print("Calcuating area")
    raw_data = Path("data", "raw", "geopackages")
    la_file = raw_data / "Local_Authority_Districts_(May_2021)_UK_BFC.gpkg"
    pa_file = (
        raw_data / "Westminster_Parliamentary_Constituencies_(Dec_2021)_UK_BFC.gpkg"
    )
    la_df = geopandas.read_file(la_file)
    pa_df = geopandas.read_file(pa_file)
    la_df["geometry"] = la_df["geometry"].buffer(0)
    pa_df["geometry"] = pa_df["geometry"].buffer(0)

    # merge both on geometry
    df = pa_df.sjoin(la_df, how="left")[["PCON21CD", "PCON21NM", "LAD21CD", "LAD21NM"]]

    # get the geometry column back in
    df = df.merge(pa_df[["PCON21CD", "geometry"]], on="PCON21CD").rename(
        columns={"geometry": "pc_geo"}
    )
    pa_df = None
    df = df.merge(la_df[["LAD21CD", "geometry"]], on="LAD21CD").rename(
        columns={"geometry": "la_geo"}
    )
    la_df = None

    # calculate the percentage overlap between a constituency and a local authority based on area
    df["percentage_overlap"] = df.apply(overlap, axis="columns")

    df = (
        df[df["percentage_overlap"] >= 0.01]
        .sort_values("percentage_overlap")
        .drop(columns=["pc_geo", "la_geo"])
    )
    df.to_csv(
        Path("data", "interim", "percentage_overlap_2022_councils_area.csv"),
        index=False,
    )

    df_2023 = update_to_2023(df)
    df_2023.to_csv(
        Path("data", "interim", "percentage_overlap_2023_councils_area.csv"),
        index=False,
    )


def update_to_2023(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update 2022 to 2023 boundaries, including rolling up to higher tiers
    Results in some double counting - but that's fine as long as interpreted right at the next stgae.
    """
    councils_2023 = date(2023, 4, 2)

    df = GovLayers(df).create_code_column("gss", "LAD21CD")
    df = GovLayers(df).get_council_info(
        ["replaced-by"], include_historical=True, as_of_date=councils_2023
    )

    df["future-code"] = df["replaced-by"].fillna(df["local-authority-code"])

    lower_tiers = (
        df.groupby(["PCON21CD", "future-code"])
        .agg({"percentage_overlap": "sum"})
        .reset_index()
        .rename(columns={"future-code": "local-authority-code"})
    )

    layers = ["county-la", "combined-authority"]

    gdf = GovLayers(lower_tiers).get_council_info(
        layers, include_historical=True, as_of_date=councils_2023
    )

    dfs = [lower_tiers]
    for layer in layers:
        # reduce to just nonnas in the layer
        df = gdf[gdf[layer].notna()]
        # then group by the layer and sum the percentage overlap
        df = (
            df.groupby(["PCON21CD", layer])
            .agg({"percentage_overlap": "sum"})
            .reset_index()
        )
        # then rename the layer column to be the same as the layer
        df = df.rename(columns={layer: "local-authority-code"})
        dfs.append(df)

    df = pd.concat(dfs)

    return df


def merge_data():
    area_df = pd.read_csv(
        Path("data", "interim", "percentage_overlap_2023_councils_area.csv")
    ).rename(columns={"percentage_overlap": "percentage_overlap_area"})
    pop_df = pd.read_csv(
        Path("data", "interim", "percentage_overlap_2023_councils_pop.csv")
    ).rename(columns={"percentage_overlap": "percentage_overlap_pop"})

    # if percentage_overlap_area > 0.999 - round up to 1
    area_df.loc[
        area_df["percentage_overlap_area"] > 0.9999, "percentage_overlap_area"
    ] = 1

    df = area_df.merge(
        pop_df, on=["PCON21CD", "local-authority-code"], how="outer"
    ).fillna(0)
    df.to_csv(
        Path("data", "interim", "percentage_overlap_2023_councils_both.csv"),
        index=False,
    )
    df.to_csv(
        Path(
            "data",
            "packages",
            "constituencies_to_local_authorities",
            "percentage_overlap_2023_councils_both.csv",
        ),
        index=False,
    )


def run_conversion():
    create_2022_council_percentages_area()
    create_2022_council_percentages_pop()
    merge_data()


if __name__ == "__main__":
    run_conversion()
