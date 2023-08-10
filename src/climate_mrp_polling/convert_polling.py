import pandas as pd
from pathlib import Path
from data_common.pandas import GovLayers
from typing import Annotated, Literal
from datetime import date


ValidGeographies = Literal["LSOA11", "PARL10", "PARL25", "LAD23"]
DataValues = Literal["percentage", "absolute"]
OverlapTypes = Literal["area", "population"]
PollingDataFrame = Annotated[
    pd.DataFrame,
    "Dataframe where first column is PCON2010, all other columns are percentage polling",
]


def get_dataset_url(
    repo_name: str, package_name: str, version_name: str, file_name: str
):
    """
    Get url to a dataset from the pages.mysociety.org website.
    """
    return f"https://pages.mysociety.org/{repo_name}/data/{package_name}/{version_name}/{file_name}"


def get_overlap_df(
    input_geography: ValidGeographies, output_geography: ValidGeographies
) -> pd.DataFrame:
    """
    Get a df from the mySociety repo with the percentage overlap between geographies
    """

    url = get_dataset_url(
        repo_name="2025-constituencies",
        package_name="geographic_overlaps",
        version_name="latest",
        file_name=f"{input_geography}_{output_geography}_combo_overlap.parquet",
    )

    return pd.read_parquet(url)


def convert_data_geographies(
    df: pd.DataFrame,
    *,
    input_geography: ValidGeographies,
    output_geography: ValidGeographies,
    input_code_col: str | None = None,
    input_values_type: DataValues = "percentage",
    output_values_type: DataValues | None = None,
    output_code_col: str | None = None,
    overlap_measure: OverlapTypes = "population",
) -> pd.DataFrame:
    """
    Convert data from one geography to another.
    Works best when output geographies are bigger (e.g. parl cons to LAs).
    Expects a dataframe with the first column being the input geography codes.
    All other columns are assumed to be ready to be converted.

    It will return an output dataframe with the first column being the output geography codes.
    """

    # validate inputs
    if input_code_col is None:
        input_code_col = input_geography
    if output_code_col is None:
        output_code_col = output_geography
    if output_values_type is None:
        output_values_type = input_values_type

    if input_code_col not in df.columns:
        raise ValueError(f"input geography {input_code_col} not in dataframe")

    if input_values_type not in ["percentage", "absolute"]:
        raise ValueError("values must be either 'percentage' or 'absolute'")

    if output_values_type not in ["percentage", "absolute"]:
        raise ValueError("values must be either 'percentage' or 'absolute'")

    # get correct overlap column
    if overlap_measure == "population":
        overlap_column = "percentage_overlap_pop"
    elif overlap_measure == "area":
        overlap_column = "percentage_overlap_area"
    else:
        raise ValueError("overlap_measure must be either 'population' or 'area'")

    # input_code_col needs to be the first column, raise error if not
    if df.columns[0] != input_code_col:
        raise ValueError(f"input geography {input_code_col} must be first column")

    # fetch the
    overlap_df = get_overlap_df(input_geography, output_geography)

    original_columns = list(df.columns)[1:]
    df = df.merge(
        overlap_df, how="left", left_on=input_code_col, right_on=input_geography
    )

    if overlap_measure == "population":
        overlap_column = "overlap_pop"
    elif overlap_measure == "area":
        overlap_column = "overlap_area"
    else:
        raise ValueError("overlap_measure must be either 'population' or 'area'")

    if input_values_type == "absolute":
        # if we've been given raw people, convert them into percentage ([absolute]/[total pop])
        # similarly if this is an area based measure, this is now a number representing [absolute]/[square ms]
        for c in original_columns:
            df[c] = df[c] / df["original_pop"]

    # at this point we have the original_columns, which are

    for c in original_columns:
        # taking our fractional values for the original geography we need to express this
        # as an absolute of the current fragment we are in
        df[c] = df[c].astype(float) * df[overlap_column]
        # this is now a unit of [original absolute unit] expected in this fragment

    # now we need to aggregate by the output geography to get the total absolute unit for each output geography
    # at the same time, we're aggregating the overlap column to get the total overlap

    final = df.groupby(output_geography).agg("sum")

    # if we want percentages, reconvert based on the summed overlap value (which should roughly add up to the output geography area)
    if output_values_type == "percentage":
        for c in original_columns:
            # percentage of total
            final[c] = final[c] / final[overlap_column]

    final = (
        final[original_columns]
        .reset_index()
        .rename(columns={output_geography: output_code_col})
    )

    return final


def convert_parl_polling_to_la(
    polling_df: PollingDataFrame,
    *,
    overlap_measure: Literal["area", "population"] = "population",
) -> pd.DataFrame:
    """
    Convert the polling data from constituency (2010 boundaries) to local authority (2023).
    Includes generating the higher geographies.
    """

    councils_2023 = date(2023, 4, 2)

    df = convert_data_geographies(
        polling_df,
        input_geography="PARL10",
        input_code_col="PCON2010",
        output_geography="LAD23",
        output_code_col="gss-code",
        input_values_type="percentage",
        output_values_type="absolute",
        overlap_measure=overlap_measure,
    )

    original_cols = list(df.columns)[1:]

    final = GovLayers(df).create_code_column(
        from_type="gss", source_col="gss-code", drop_source=True
    )

    final = GovLayers(final).get_council_info(
        ["pop-2020"], include_historical=True, as_of_date=councils_2023
    )

    upper_layers = GovLayers(final).to_multiple_higher(aggfunc="sum")

    # recombine the upper layers with the lower layers
    final = pd.concat([upper_layers, final])

    # calculate the percentages
    for c in original_cols:
        final[c] = final[c] / final["pop-2020"]

    final = GovLayers(final).get_council_info(
        ["official-name"], include_historical=True, as_of_date=councils_2023
    )

    final = final[["local-authority-code", "official-name"] + original_cols]

    return final
