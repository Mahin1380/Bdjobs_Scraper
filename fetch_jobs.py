from curl_cffi import requests   
import time
import pandas as pd
from clean_csv import clean_jobs
from tqdm.auto import tqdm



def get_data(job_title, pages):
   
    base_url = "https://api.bdjobs.com/Jobs/api/JobSearch/GetJobSearch"
    
    params = {
        "Icat": "",
        "industry": "",
        "category": "",
        "org": "",
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
        "isFresher": "false"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://bdjobs.com/",
        "Accept-Language": "en-US,en;q=0.9",
    }

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

    if pages == 'all':
        pages_to_fetch = total_pages
    else:
        pages_to_fetch = min(total_pages, int(pages))

    print(
        f"Total pages: {total_pages}  |  "
        f"Approx jobs: {total_jobs_approx} | "
        f"Fetching: {pages_to_fetch} pages"
    )

    for page in tqdm(range(1, pages_to_fetch + 1), desc="Fetching pages", unit="page", colour="cyan"):
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

        time.sleep(1.1)


    # ✅ FIX 1: create df AFTER loop
    df = pd.json_normalize(all_jobs)

    # optional but safe (prevents duplicate issues in cleaning)
    df = clean_jobs(df)

    return df
