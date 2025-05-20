import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from rapidfuzz import process, fuzz
import os


output_dir = input("Enter the full path where output Excel files should be saved: ").strip()
os.makedirs(output_dir, exist_ok=True)


conn_str = "mssql+pyodbc://@DCRODBPRDDB6001/CurrentData?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(conn_str)

query = "SELECT Name FROM CompanyBasic WHERE TypeStatus = '0'"
df_sql = pd.read_sql(query, engine)
company_names_sql = df_sql["Name"].tolist()

# Count each company name's occurrences in SQL dataset
name_counts_sql = pd.Series(company_names_sql).value_counts().to_dict()

# Load company names from Excel
df_excel = pd.read_excel(r"C:\Users\pkhare\OneDrive - MORNINGSTAR INC\Desktop\Company OPS\Public Private Security Master Squad- Himanshu\Legal name list for fuzzy  T.xlsx")
company_names_excel = df_excel["PB CompanyLegalName"].tolist()

THRESHOLD = 75
batch_size = 500

# Progress tracking
progress_file = "progress_checkpoint.txt"
batch_file = "batch_checkpoint.txt"

last_index = 0
batch_count = 0

if os.path.exists(progress_file):
    with open(progress_file, "r") as f:
        last_index = int(f.read().strip())
        print(f"Resuming from index {last_index}")

if os.path.exists(batch_file):
    with open(batch_file, "r") as f:
        batch_count = int(f.read().strip())
        print(f"Resuming from batch {batch_count + 1}")

matches = []
for index, company in enumerate(company_names_excel[last_index:], start=last_index + 1):
    best_match = process.extractOne(company, company_names_sql, scorer=fuzz.WRatio)

    if best_match:
        match, score, _ = best_match
        if score >= THRESHOLD:
            is_duplicate = name_counts_sql.get(match, 0)
            matches.append((company, match, score, is_duplicate))
        else:
            matches.append((company, "No good match", score, 0))

    if index % 100 == 0:
        print(f"Processed {index} records out of {len(company_names_excel)}")

    if index % batch_size == 0 or index == len(company_names_excel):
        batch_count += 1
        batch_df = pd.DataFrame(matches, columns=["Excel_Company", "Matched_Company", "Score", "IsDuplicate"])
        batch_filename = os.path.join(output_dir, f"Fuzzy_Match_Results_Batch_{batch_count}.xlsx")
        batch_df.to_excel(batch_filename, index=False)
        print(f"Saved {os.path.abspath(batch_filename)} with {len(matches)} records.")

        matches.clear()

        with open(progress_file, "w") as f:
            f.write(str(index))

        with open(batch_file, "w") as f:
            f.write(str(batch_count))

engine.dispose()
print("Matching completed. All results saved in batches.")
