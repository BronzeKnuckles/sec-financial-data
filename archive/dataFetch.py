import requests, zipfile, io, os
import pandas as pd
import psycopg2


def convertData(fname):
    """
    > Takes in foldername,
    > makes .csv files from .txt files,
    > removes .txt files,
    > returns nothing
    """
    f = ["num", "pre", "sub", "tag"]

    for i in f:
        df = pd.read_csv(f"{fname}/{i}.txt", sep="\t", header=None)
        df.to_csv(f"{fname}/{i}.csv", header=None)
        print(f"Converted-- {fname}/{i}.csv")
        os.remove(f"{fname}/{i}.txt")
        print(f"Converted to CSV in file : {fname}")

    print(f"Conversion complete in: {fname}")


def main(start_year, end_year):
    """
    > Takes in start_year, end_year,
    > downloads zip files of all quarters of the years,
    > extracts zip into specific folders with year and quarter name,
    > [Optional] call function: convertData(fname) converts txt to csv format,
    > returns nothing
    """
    for year in range(start_year, end_year + 1):
        for q in range(1, 5):
            url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{q}.zip"

            print(f"fetching from url {url}...")
            r = requests.get(url)

            if r.status_code == 404:
                print(f"CODE 404 -- {url}")
                continue

            z = zipfile.ZipFile(io.BytesIO(r.content))

            fname = f"./data/{year}q{q}"
            z.extractall(fname)
            os.remove(f"{fname}/readme.htm")

            print(f"extracted in {fname}")

            # use convertData(fname) if necessary
            # you can import the data from .txt as df = pd.read_csv(file_name, sep = "\t", header = None)

            # convertData(fname)


if __name__ == "__main__":
    # set the start and end year range
    start_year = 2009
    end_year = 2023

    main(start_year, end_year)
    print(f"\n\n\n DONE. \n\t From: {start_year}\n\t To: {end_year}\n\n\n")
