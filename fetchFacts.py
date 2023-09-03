def downloadZip(url):
    url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{q}.zip"

    print(f"fetching from url {url}...")
    r = requests.get(url)

    if r.status_code == 404:
        print(f"CODE 404 -- {url}")
        return

    z = zipfile.ZipFile(io.BytesIO(r.content))
    fname = f"./data/{year}q{q}"
    z.extractall(fname)
    os.remove(f"{fname}/readme.htm")

    print(f"extracted in {fname}")
