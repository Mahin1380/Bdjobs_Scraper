from curl_cffi import requests   
import time
import pandas as pd
from clean_csv import clean_jobs
from tqdm.auto import tqdm

base_url = "https://api.bdjobs.com/Jobs/api/JobSearch/GetJobSearch"

job_title = "financial analyst"   # change this to your desired job title, e.g. "Data Analyst", "Software Engineer", etc.

params = {
    "Icat": "",
    "industry": "",
    "category": "",
    "org": "",  # change this to your desired organization ID 1=govt, 2=semi-govt, 3=ngo, 4=private, 5=international_agencies, 6=others or leave blank for all
    "jobNature": "",
    "Fcat": "",
    "location": "",
    "Qot": "",
    "jobType": "",
    "jobLevel": "",
    "postedWithin": "",
    "deadline": "",
    "keyword": job_title,
    "pg": 1,
    "qAge": "",
    "Salary": "",
    "experience": "",
    "gender": "",
    "MExp": "",
    "genderB": "",
    "MPostings": "",
    "MCat": "",
    "version": "",
    "rpp": "50",
    "Newspaper": "",
    "armyp": "",
    "QDisablePerson": "",
    "pwd": "",
    "workplace": "",
    "facilitiesForPWD": "",
    "SaveFilterList": "",
    "UserFilterName": "",
    "HUserFilterName": "",
    "earlyJobAccess": "",
    "isPro": "1",
    "ToggleJobs": "true",
    "isFresher": "false"  # change this to "false" if you don't want to include fresher jobs
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://bdjobs.com/",
    "Accept-Language": "en-US,en;q=0.9",
}


# ────────────────────────────────────────────────
# Step 1: Get first page → discover real total pages
# ────────────────────────────────────────────────
session = requests.Session()
all_jobs = []

print("Fetching page 1 to determine total pages...")
response = session.get(base_url, params=params, headers=headers)

if response.status_code != 200:
    print(f"Request failed with status {response.status_code}")
    print(response.text[:400])
    raise SystemExit

data = response.json()

if data.get("statuscode") != "1":
    print("API error:", data.get("message", "No message"))
    raise SystemExit

total_pages = int(data["common"].get("totalpages", 1))
total_jobs_approx = data["common"].get("total_records_found", "unknown")

print(f"Total pages: {total_pages}  |  Approx jobs: {total_jobs_approx}")

# ────────────────────────────────────────────────
# Step 2: Fetch all pages
# ────────────────────────────────────────────────
for page in tqdm(range(1, total_pages + 1), desc="Fetching pages", unit="page", colour="cyan"):
    params["pg"] = page

    response = session.get(base_url, params=params, headers=headers)

    if response.status_code != 200:
        print(f"Failed (status {response.status_code})")
        break

    page_data = response.json()
    if page_data.get("statuscode") != "1":
        print("API error on page", page)
        break

    regular_jobs = page_data.get("data", [])
    premium_jobs = page_data.get("premiumData", [])

    all_jobs.extend(regular_jobs)
    all_jobs.extend(premium_jobs)

    time.sleep(1.1)   # polite delay – increase if you get blocked

# ────────────────────────────────────────────────
# Step 3: Clean & save
# ────────────────────────────────────────────────
if all_jobs:
    df = pd.DataFrame(all_jobs)
    print(f"\nRaw jobs collected: {len(df)}")

    df = clean_jobs(df)
    print(f"After cleaning: {len(df)} jobs")

    output_file = f"bdjobs_{job_title.replace(' ', '_')}.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"Saved to {output_file}")
    print("Columns:", ", ".join(df.columns[:8]), "...")
else:
    print("No jobs collected.")

# Optional: quick stats
print("\nQuick check:")
print(df["companyName"].value_counts().head(8))