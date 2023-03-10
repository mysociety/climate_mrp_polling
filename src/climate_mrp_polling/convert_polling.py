import pandas as pd
from pathlib import Path
from data_common.pandas import GovLayers
from typing import Annotated, Literal
from datetime import date

# create top_level folder with is two levels above this file
top_level = Path(__file__).parents[2]

annotated_df = Annotated[
    pd.DataFrame,
    "Dataframe where first column is PCON2010, all other columns are percentage polling",
]


def convert_polling_from_con_to_la(
    polling_df: annotated_df,
    values: Literal["percentages", "people"] = "percentages",
    overlap_measure: Literal["area", "population"] = "population",
) -> pd.DataFrame:
    """
    Convert the polling data from constituency (2010 boundaries) to local authority (2023).
    """

    # Get the intersection of of constituency populations and overlapping with las
    councils_2023 = date(2023, 4, 2)
    df = pd.read_csv(
        Path(
            top_level,
            "data",
            "interim",
            "percentage_overlap_2023_councils_both.csv",
        )
    )

    if overlap_measure == "population":
        overlap_column = "percentage_overlap_pop"
        df = df.drop(columns=["percentage_overlap_area"])
    elif overlap_measure == "area":
        overlap_column = "percentage_overlap_area"
        df = df.drop(columns=["percentage_overlap_pop"])
    else:
        raise ValueError("overlap_measure must be either 'population' or 'area'")

    df_con = pd.read_csv(Path(top_level, "data", "interim", "con_2010_pops.csv"))
    df = df.merge(df_con, how="left", left_on="PCON21CD", right_on="PCON11CD")
    df["proportion_pop"] = df["con_pop"] * df[overlap_column]
    df = df.rename(columns={"PCON21CD": "PCON2010"})

    if values == "people":
        # if we've been given raw people, convert them into percentage before conversion
        original_columns = polling_df.columns
        polling_df = polling_df.merge(
            df_con, how="left", left_on="PCON2010", right_on="PCON11CD"
        )
        for c in original_columns:
            polling_df[c] = polling_df[c] / polling_df["con_pop"]

    # merge these two
    # which will give us all the possible overlaps between las and cons
    # and we then adjust by the proportion of the population to get the estimated 'actual' people

    ndf = df.merge(polling_df)
    for c in polling_df.columns[1:]:
        # adjust to the proportion of pop in fragment
        ndf[c] = ndf[c].astype(float) * ndf["proportion_pop"]

    # now we need to aggregate by local authority to get the total number of people
    # hopefully add back up to the pop of each local authority based on composite of constituencies

    final = (
        ndf.groupby("local-authority-code")
        .agg("sum")
        .drop(columns=[overlap_column, "con_pop"])
    )

    # if we want percentages, reconvert based on the summed proportion_pop (which should now be roughly the la pop)
    if values == "percentages":
        for c in final.columns[1:]:
            final[c] = final[c] / final["proportion_pop"]

    final = final.drop(columns=["proportion_pop"])
    final = final.reset_index()

    final = GovLayers(final).get_council_info(
        ["official-name"], include_historical=True, as_of_date=councils_2023
    )
    # move these columns to the front
    first = ["local-authority-code", "official-name"]
    final = final[first + [c for c in final.columns if c not in first]]

    return final
