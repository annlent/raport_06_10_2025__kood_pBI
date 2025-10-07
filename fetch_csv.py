import os, io, re, csv
from pathlib import Path
import requests
import chardet
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# csv asukoht
CSV_PATH = Path("data/RAA0061.csv")
SCHEMA   = "stat_ee" 
TABLE    = "raa0061_raw"

def die(msg):
    raise SystemExit(f"[STOP] {msg}")

load_dotenv() #vajalikud andmed .env failist (host, port, andmebaas, kasutaja, salasõna)
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}"
    f"@{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDB')}",
    future=True
)
#create_engine loob sqlAlchemy ühendusmootori --> värav andmebaasi, st selle abil saab käivitada sql lauseid, laadida andmeid pandasest pstgresqli, lugeda sql tulemusi andmeraamis.
#postgresql+psycopg2:// on ühenduse draiver, mis ütleb sqlAlchemyle - kasuta postgrwsql andmebaasi ja psycopg2 python draiverit

# 0) kontrollime kas CSV olemas, nö varane kontroll (fail on v mitte)
if not CSV_PATH.exists():
    die(f"CSV puudub: {CSV_PATH.resolve()}")

# 1) kiire visuaalne kontroll, st kas fail on olemas ja mitte tühi (st kas esimene laadimine oli edukas)
raw = CSV_PATH.read_bytes()
print(f"[INFO] CSV suurus: {len(raw)} baiti, asukoht: {CSV_PATH.resolve()}")
print("[INFO] Faili algus (<=500b):")
print(raw[:500].decode(errors="ignore"))

# 2) Formaadi tuvastus (Sniffer tuvastab veergude eraldaja)
enc = chardet.detect(raw).get("encoding") or "utf-8"
sample = raw[:100000].decode(enc, errors="ignore")
try:
    dialect = csv.Sniffer().sniff(sample, delimiters=[",",";","\t","|"])
    sep = dialect.delimiter
except Exception:
    sep = ";" if sample.count(";") > sample.count(",") else ","
decimal = "," if sep == ";" else "."
print(f"[INFO] Tuvastatud encoding={enc}, sep='{sep}', decimal='{decimal}'")

# 3) Loeme andmeraami, esialgu kõik sõnedena, et vältida infokadu (hiljem muudame andmetüüpe, kui kõik veerunimed on paigas). decimal määrtleb kümnendeid erladava märgi
df = pd.read_csv(CSV_PATH, encoding=enc, sep=sep, decimal=decimal, dtype=str)
print(f"[INFO] DF kuju: {df.shape}, veerud: {list(df.columns)}")

# 4) Normaliseerime päised ja nimetame ümber.
orig_cols = df.columns.tolist()
df.columns = [re.sub(r"[^0-9A-Za-z]+","_", c).strip("_").lower() for c in df.columns]
rename_map = {
    "komponent":"component", "component":"component",
    "aasta":"year", "year":"year", "time":"year",
    "kvartal":"quarter", "quarter":"quarter",
    "näitaja":"indicator", "naitaja":"indicator", "indicator":"indicator",
    "mõõtühik":"unit", "mootuhik":"unit", "units":"unit", "unit":"unit",
    "väärtus":"value", "vaartus":"value", "value":"value"
} #kaardistame nii eesti, kui ingliskeelsed päised, standardiseerime (et etl ja sql oleksid stabiilsed)
df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})
print(f"[INFO] Normaliseeritud veerud: {list(df.columns)}")

# 5) Teisendame laiast kitsaks, st tuvastame mõõdikuveerud, mis pole dimensioonid
dim_candidates = ["component","year","quarter","indicator","unit"]
present_dims = [c for c in dim_candidates if c in df.columns]
measure_cols = [c for c in df.columns if c not in present_dims]

if "value" not in df.columns:
    if len(measure_cols) == 0:
        die("Ei leidnud ühtki mõõdikuveeru (value).")
    elif len(measure_cols) == 1:
        mcol = measure_cols[0]
        # Pane indicator'ks inimloetav nimi (algsest päisest vastav element)
        # Leia algse päise indeks -> inimloetav tekst
        human_indicator = None
        for oc, nc in zip(orig_cols, [re.sub(r"[^0-9A-Za-z]+","_", c).strip("_").lower() for c in orig_cols]):
            if nc == mcol:
                human_indicator = oc
                break
        if not human_indicator:
            human_indicator = mcol
        df["indicator"] = human_indicator
        if "unit" not in df.columns:
            # lihtne heuristik ühikule
            df["unit"] = "miljonit eurot" if "eurot" in human_indicator.lower() else None
        df["value"] = pd.to_numeric(df[mcol].astype(str).str.replace(" ", "", regex=False), errors="coerce")
        df = df[[c for c in df.columns if c != mcol]]
    else:
        id_vars = [c for c in ["component","year","quarter","indicator","unit"] if c in df.columns]
        df = df.melt(id_vars=id_vars, var_name="indicator", value_name="value")
        df["value"] = pd.to_numeric(df["value"].astype(str).str.replace(" ", "", regex=False), errors="coerce")

# 6) Tüübid ja kontrollid (aasta ja väärtus numbriteks)
if "year" in df.columns:
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
if "value" in df.columns:
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
required = ["component","year","quarter","indicator","value"]
missing = [c for c in required if c not in df.columns]
if missing:
    die(f"Pärast teisendust puuduvad veerud: {missing}.")
if len(df)==0:
    die("Andmeraam on tühi.")
if df["value"].isna().mean()==1.0:
    die("Kõik VALUE on NaN – kontrolli eraldajat.")

print("[INFO] DF näidis pärast teisendust:")
print(df.head(5).to_string())

# 7) Tabeli loomine ja TRUNCATE (puhastab sihttabeli enne iga laadimist)
with engine.begin() as con:
    con.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
    con.exec_driver_sql(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
            component  text,
            year       integer,
            quarter    text,
            indicator  text,
            unit       text,
            value      numeric
        );
    """)
    before = con.execute(text(f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE}")).scalar_one()
    print(f"[INFO] Enne TRUNCATE: {before}")
    con.exec_driver_sql(f"TRUNCATE {SCHEMA}.{TABLE};")

# 8) Laadimine postgressqli
cols = ["component","year","quarter","indicator","unit","value"] #võtame ainult need veerud õiges järjekorras
df[cols].to_sql(TABLE, con=engine, schema=SCHEMA, if_exists="append", index=False, method="multi", chunksize=5000)
#laeme andmeraami andmebaasi. Method="multi" ja chunksize on jõudluse jaoks mitmerealine INSERT.
#to_sql on pandase meetod, mis võtab andmeraami ja sisestab andmed sql tabelisse. ka võimalik luua ühendus, aga siin on see juba engine kaudu olemas
#if_existx="append" seepärst, et nne tegime TRUNCATE'ga tühjenduse, seega täidame tühja tabeli. Hilje uute andmete lisandumisel saab jätta samaks (uued andmed)
#index=False ütleb pandasele, et andmeraami indeksit ei salvestata tabelisse
#method="multi" määrab kuidas INSERT käsku genereeritakse. Vaikimisi ükshaaval. Multi paneb mitu kirjet ühte käsku. Multi on kiirem.
#chunksize määramine väldib mälu ületäitumist ja võrgu timeouti, eriti oluline suurte andmemahtude korral. Optimeerimise eesmärk

# 9) Kontroll andmebaasis
with engine.begin() as con:
    after = con.execute(text(f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE}")).scalar_one()
    print(f"[INFO] Pärast laadimist: {after}") #kinnitab et ridu on > 0
    sample_rows = con.execute(text(f"SELECT * FROM {SCHEMA}.{TABLE} LIMIT 5")).fetchall()
    print("[INFO] Anmdebaasi näidised:", sample_rows) #trükib paar näidisrida

print("[OK] CSV -> PostgreSQL laadimine õnnestus.")
