import pandas as pd
from pathlib import Path
from .convert_polling import convert_polling_from_con_to_la


def sort_name(s: str) -> str:
    """
    Rough function to convert constituency names to a standard format
    """

    s = s.replace("Na h-Eileanan An Iar (Western Isles)", "Na h-Eileanan an Iar")
    s = s.replace(" (Yorks)", "")
    s = s.replace("Môn", "Mon")
    s = s.replace("-", " ").lower()
    s = s.replace(" & ", " and ")
    s = s.replace(" of ", " ")
    s = s.replace(" the ", " ")
    s = s.replace(",", "")
    s = s.replace(" st ", " saint ")
    s = s.replace(" st.", " saint ")
    s = s.replace(" st,", " saint ")
    s = s.replace(" of", " ")
    s = s.replace("kingston upon hull", "hull")
    s = s.replace(")", "")
    s = s.replace("(", "")

    while "  " in s:
        s = s.replace("  ", " ")

    l = s.strip().split(" ")
    l.sort()
    return " ".join(l)


def convert_renewable_uk():
    """
    Convert RenewableUK polling to local authorities
    https://www.renewableuk.com/news/615931/Polling-in-every-constituency-in-Britain-shows-strong-support-for-wind-farms-to-drive-down-bills.htm

    """
    polling_df = (
        pd.read_excel(
            str(
                Path(
                    "data",
                    "raw",
                    "polling",
                    "RenewableUK-MRP-Constituency-Topline.xlsx",
                )
            )
        )
        .drop(columns=["Variable", "Name"])
        .rename(columns={"Group": "PCON2010"})
        .set_index("PCON2010")
    )

    polling_df = polling_df / 100
    polling_df = polling_df.reset_index()

    df = convert_polling_from_con_to_la(polling_df)

    melted_df = df.melt(
        id_vars=["local-authority-code", "official-name"],
        value_name="percentage",
        var_name="question",
    )
    melted_df["source"] = "RenewableUK2022"
    # move to first column
    cols = melted_df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    melted_df = melted_df[cols]

    questions = melted_df["question"].unique().tolist()
    short = ["Q" + x.split(")")[0] for x in questions]
    lookup = dict(zip(questions, short))
    melted_df["question"] = melted_df["question"].map(lookup)

    lookup_dataframe = pd.DataFrame.from_dict(lookup, orient="index")
    lookup_dataframe = lookup_dataframe.reset_index()
    lookup_dataframe.columns = ["question", "short"]
    lookup_dataframe["source"] = "RenewableUK2022"
    # move to first column
    cols = lookup_dataframe.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    lookup_dataframe = lookup_dataframe[cols]

    lookup_dataframe.to_csv(
        Path("data", "interim", "polling", "RenewableUK2022_lookup.csv"), index=False
    )

    melted_df.to_csv(
        Path("data", "interim", "polling", "RenewableUK2022.csv"), index=False
    )


def convert_onward_guide():
    """
    Make guide lookup correct
    """
    guide_df = pd.read_excel(
        str(
            Path(
                "data",
                "raw",
                "polling",
                "Public-First-Poll-for-Onward-MRP-Model.xlsx",
            )
        ),
        sheet_name="GUIDE",
    )

    party_qs = guide_df[["Key", "Question"]].head(11)

    party_as = guide_df[["Answer Key", "Answer Options"]].head(8)
    new_keys = {}
    for i, row in party_qs.iterrows():
        for ia, answer_row in party_as.iterrows():
            key = row["Key"] + "_" + answer_row["Answer Key"]
            answer = row["Question"] + " -- " + answer_row["Answer Options"]
            new_keys[key] = answer

    guide_df = pd.read_excel(
        str(
            Path(
                "data",
                "raw",
                "polling",
                "Public-First-Poll-for-Onward-MRP-Model.xlsx",
            )
        ),
        sheet_name="GUIDE",
        skiprows=15,
    )

    # in Question column, fill a nan value with the previous value in the column
    guide_df["Key"] = guide_df["Key"].fillna(method="ffill")
    guide_df["Question"] = guide_df["Question"].fillna(method="ffill")
    guide_df["Real answer"] = guide_df["Question"] + " -- " + guide_df["Answer Options"]
    new_dict = guide_df.set_index("Answer Key")["Real answer"].to_dict()
    new_keys.update(new_dict)
    # convert to a dataframe
    new_keys = pd.DataFrame.from_dict(new_keys, orient="index").reset_index()
    new_keys.columns = ["short", "question"]
    new_keys["source"] = "Onward2022"
    # rearrange to source,question,short
    new_keys = new_keys[["source", "question", "short"]]
    new_keys.to_csv(
        Path("data", "interim", "polling", "Onward2022_lookup.csv"), index=False
    )


def convert_onward():
    """
    Convert  onward 2022 polling
    """
    df = pd.read_excel(
        str(
            Path(
                "data",
                "raw",
                "polling",
                "Public-First-Poll-for-Onward-MRP-Model.xlsx",
            )
        ),
        sheet_name="MRP Results",
    )
    valid_columns = [
        c
        for c in df.columns
        if (c == "Constituency" or c.startswith("Q")) and not c.endswith("Winner")
    ]
    df = df[valid_columns]

    lookup = pd.read_csv(
        Path(
            "data",
            "raw",
            "Westminster_Parliamentary_Constituencies_(Dec_2020)_Names_and_Codes_in_the_United_Kingdom.csv",
        )
    )
    lookup["PCON20NM"] = lookup["PCON20NM"].apply(sort_name)

    lookup = lookup.set_index("PCON20NM")["PCON20CD"].to_dict()

    df["PCON2010"] = df["Constituency"].apply(sort_name).apply(lambda x: lookup[x])

    # move the constituency column to the front
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]

    # drop constituency name
    df = df.drop(columns=["Constituency"])

    df = convert_polling_from_con_to_la(df)

    melted_df = df.melt(
        id_vars=["local-authority-code", "official-name"],
        value_name="percentage",
        var_name="question",
    )
    melted_df["source"] = "Onward2022"
    # move to front
    cols = melted_df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    melted_df = melted_df[cols]

    melted_df.to_csv(Path("data", "interim", "polling", "Onward2022.csv"), index=False)


def join_files():
    """
    Convert all polling into a single file to be ingested elsewhere
    """
    polling_dir = Path("data", "interim", "polling")

    # read all polling files
    polling_files = [
        x
        for x in polling_dir.iterdir()
        if x.suffix == ".csv" and "lookup" not in x.name
    ]
    polling_dfs = [pd.read_csv(x) for x in polling_files]
    df = pd.concat(polling_dfs, ignore_index=True)
    df.to_csv(
        Path(
            "data",
            "packages",
            "local_authority_climate_polling",
            "local_authority_climate_polling.csv",
        ),
        index=False,
    )

    # convert guides
    guide_files = [
        x for x in polling_dir.iterdir() if x.suffix == ".csv" and "lookup" in x.name
    ]
    guide_dfs = [pd.read_csv(x) for x in guide_files]
    df = pd.concat(guide_dfs, ignore_index=True)
    df.to_csv(
        Path(
            "data",
            "packages",
            "local_authority_climate_polling",
            "local_authority_climate_polling_guide.csv",
        ),
        index=False,
    )


def convert_all():
    convert_renewable_uk()
    convert_onward()
    convert_onward_guide()
    join_files()


if __name__ == "__main__":
    convert_all()
