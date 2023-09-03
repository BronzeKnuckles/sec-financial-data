import os
import json
import pandas as pd
import multiprocessing as mp

# Directory containing the JSON files
DATA_DIR = "./data/companyfacts/"

# List all JSON files in the directory
files = [f for f in os.listdir(DATA_DIR) if f.endswith(".json")]

columns = [
    "CompanyName",
    "cik",
    "fact",
    "units",
    "end",
    "val",
    "accn",
    "fy",
    "fp",
    "form",
    "filed",
]


def process_file(company):
    with open(os.path.join(DATA_DIR, company), "r") as data:
        df = json.load(data)

    try:
        company_name = df["entityName"]
        cik = df["cik"]
        facts = df["facts"][
            "us-gaap"
        ].keys()  # Not all Companies report in us-gaap, ignoring non us-gaap (For Now )
    except:
        print(f"error in file {company}")  # example: error in infosys!, WIPRO Ltd.
        return "Not"
    rows = []

    for fact in facts:
        dynamic_col = str(list(df["facts"]["us-gaap"][fact]["units"].keys())[0])
        for unit in df["facts"]["us-gaap"][fact]["units"][dynamic_col]:
            row = [
                company_name,
                cik,
                fact,
                dynamic_col,  # dynamic col is CURRENCY -> 'USD'
                unit["end"],
                unit["val"],
                unit["accn"],
                unit["fy"],
                unit["fp"],
                unit["form"],
                unit["filed"],
            ]
            rows.append(row)

    df_processed = pd.DataFrame(rows, columns=columns)
    output_file = os.path.join(DATA_DIR, f"{company_name}.csv")
    df_processed.to_csv(output_file, index=False)

    return f"Processed {company_name}"


if __name__ == "__main__":
    # Use multiprocessing to process the files in parallel
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(process_file, files)
        for result in results:
            print(result)
