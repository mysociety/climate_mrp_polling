from pathlib import Path
import pandas as pd
from data_common.pandas import GovLayers


def create_unified_con_pop():
    """
    Create a new file with unified 2010 parliamentary constituency populations.
    The populations are for 2021.
    """

    pop_folder = Path("data", "raw", "con_pop")

    pop_file = pop_folder / "sape23dt7mid2020parliconsyoaestimatesunformatted.xlsx"

    print("pPocessing E&W 2020")
    df = (
        pd.read_excel(str(pop_file), sheet_name="Mid-2020 Persons", header=4)
        .drop(columns=["PCON11NM", "All Ages"])
        .set_index("PCON11CD")
    )
    ew_pop = (
        df.drop(columns=[x for x in df.columns if x == "90+" or int(x) < 18])
        .sum(axis=1)
        .to_frame("con_pop")
        .reset_index()
    )

    print("Processing Scot 2021")
    scot_pop_file = pop_folder / "ukpc-21-tabs.xlsx"
    df = (
        pd.read_excel(str(scot_pop_file), sheet_name="2021", header=3)
        .rename(columns={"UK Parliamentary Constituency 2005 Code": "PCON11CD"})
        .loc[lambda df: df["Sex"] == "Persons"]
        .drop(columns=["UK Parliamentary Constituency 2005 Name", "Sex", "Total"])
        .set_index("PCON11CD")
    )
    df = df.drop(
        columns=[x for x in df.columns if x in [f"Age {y}" for y in range(18)]]
    )
    scot_pop = df.sum(axis=1).to_frame("con_pop").reset_index()

    print("Processing NI 2020")
    ni_file = pop_folder / "MYE20-SYA.xlsx"
    df = pd.read_excel(str(ni_file), sheet_name="Flat")

    partial = df.loc[
        (df["area"] == "3. Parliamentary Constituencies (2008)")
        & (df["year"] == 2020)
        & (df["gender"] == "All persons")
        & (df["age"] > 17)
    ]
    df = partial.pivot_table("MYE", index="area_code", aggfunc="sum").reset_index()
    df.columns = ["PCON11CD", "con_pop"]
    ni_pop = df
    print("Joining all three")
    pops = [ew_pop, scot_pop, ni_pop]
    df_con = pd.concat(pops)
    df_con.to_csv(Path("data", "interim", "con_2010_pops.csv"), index=False)
    print("Done")


if __name__ == "__main__":
    create_unified_con_pop()
