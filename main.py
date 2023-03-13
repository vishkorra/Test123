import os
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import concurrent.futures
import time
import random
import numpy as np
import datetime
import multiprocessing


# get current path
def get_current_path():
    return os.path.dirname(os.path.realpath(__file__)) + "/"

def get_cpu_count():
    cpu_count = multiprocessing.cpu_count()
    if cpu_count < 1:
        cpu_count = 1
    return cpu_count

PATH = get_current_path()

WORKERS = get_cpu_count() * 2

today = datetime.datetime.now().date()
job_list = pd.read_csv("jobtitle - Data Extraction.csv")
data = []
data1 = []
count = 0
start_date = datetime.datetime.now().date()


def main(JobTitle, Location, State):
    statsCount = 0
    num = 1
    next_page = True
    while next_page:
        jobID = []
        main_page_url = f"https://www.simplyred.co.in/search?q={JobTitle}&l={Location}&pn={num}"
        main_response = requests.get(main_page_url).text  
        if "Next page" not in main_response:
            next_page = False
        else:
            num += 1
        soup = BeautifulSoup(main_response, "html.parser")
        jobkeys = soup.find_all(attrs={"data-jobkey": True})
        for jobkey in jobkeys:
            if jobkey['data-jobkey'] in jobID:
                continue
            jobID.append(jobkey['data-jobkey'])

        for id in jobID:
            job_info = requests.get(f'https://www.simplyred.co.in/api/job?key={id}').json()

            regex = re.compile(r"<br /?>", re.IGNORECASE)
            c_job = job_info["job"] if 'job' in job_info else None
            if not c_job:
                continue
            job_description = job_info["job"]["description"] if 'description' in job_info["job"] else None
            if not job_description:
                continue
            newtext = re.sub(regex, '\n', job_description)
            description = BeautifulSoup(newtext, "html.parser")

            full_description = description.get_text()

            # Check if the description contains an email
            email_match = re.search(r'[\w.-]+@[\w.-]+', full_description)
            if email_match:
                email = email_match.group()
            else:
                email = 'none'
            # Check if the description contains a phone number starting with +91
            phone_match = re.search(r'\d{10}', full_description)
            if phone_match:
                phone = phone_match.group()
            else:
                phone = 'none'

            # Check if email and phone exist
            if phone == 'none' and email == 'none':
                continue

            jobkey = job_info["jobKey"]
            job_title = job_info["job"]["title"]
            company_name = job_info["job"]["company"]
            location = job_info["job"]["location"]
            qualifications = ""
            benefits = ""
            job_type = ""
            if "educationEntities" in job_info:
                qualifications += ", ".join(job_info["educationEntities"])
            if "skillEntities" in job_info:
                qualifications += ", ".join(job_info["skillEntities"])
            if "jobType" in job_info["job"]:
                job_type = job_info["job"]["jobType"]
            if "benefitEntities" in job_info:
                benefits += ", ".join(job_info["benefitEntities"])
            state = State
            post_data = requests.post('https://testapi.ezjobs.io/admin/uploadJobData', data={
                'secretToken': 'KzjQQ5H4ZTSn7Efm4DFDcHcKUhvPNK7PRyWMyz7QPuwv3Djgvk',
                'countryCode': 91,
                'firstName': company_name,
                'phone': phone,
                'type': "M",
                'email': email,
                'address1': location,
                'city': 'none',
                'state': 'none',
                'latitude': 'none',
                'longitude': 'none',
                'zipCode': 'none',
                'country': 'IN',
                'jobTitle': job_title,
                'jobDescription': full_description,
                'categoryId': jobkey,
                'skills': qualifications,
                'jobType': job_type,
                'companyName': company_name,
                'source': 'SimplyHired',
                'classified': 'No',
                'validUpTo': 'none',
                'contactSource': 'w'
            })

            global count
            count += 1
            statsCount += 1

            data.append(
                [job_title, jobkey, company_name, location, full_description, qualifications, benefits, job_type, email,
                 phone, state])
    data1.append(['Simply Hired', Location.title(), JobTitle.title(), statsCount, datetime.datetime.now().date()])
    return "Success"


# Start threads
threaded_start = time.time()

current_data = []
chunk_iter = 1
chunk_size = WORKERS


def run_the_executor(current_rows):
    global chunk_iter
    print(f"running chunk # {chunk_iter}")
    chunk_iter += 1
    with concurrent.futures.ThreadPoolExecutor(WORKERS) as executor:
        futures = []
        for title, city, state in current_rows:
            futures.append(executor.submit(main, JobTitle=title, Location=city, State=state))
        for future in concurrent.futures.as_completed(futures):
            try:
                print(future.result())
            except Exception as e:
                time.sleep(10)
                print(f"{e} || TRYING AGAIN")
                print(future.result())


for index, row in job_list.iterrows():
    current_data.append((row["Job Title"], row["City"], row["State"]))
    if len(current_data) == chunk_size:
        run_the_executor(current_data)
        current_data.clear()

# latest chunk
if current_data:
    run_the_executor(current_data)

print("Threaded time:", time.time() - threaded_start)

# Build Pandas Dataframe
df = pd.DataFrame(data, columns=['Title', 'Key', 'Company', 'Location', 'Description', 'Qualifications', 'Benefits',
                                 'Job type', 'Email', 'Phone', 'State'])
df = df.drop_duplicates(subset=['Key'], keep='first')

# Add Count & Date
# df["Count"] = count
# df.iloc[1:, df.columns.get_loc("Count")] = np.nan
# df["Date Started"] = start_date
# df.iloc[1:, df.columns.get_loc("Date Started")] = np.nan
# df["Date Ended"] = datetime.datetime.now().date()
# df.iloc[1:, df.columns.get_loc("Date Ended")] = np.nan

# path = "/Users/vishkorra/Desktop/Simply Hired/"

#df.to_excel(f"Simply Hired All{time.strftime(' %m-%d-%Y')}.xlsx", index=False)
# Build Second Data

df1 = pd.DataFrame(data1, columns=['Source', 'Location', 'Job Title', 'Count', 'Date Ended'])

# df1["Count"] = count
# df1.iloc[1:, df1.columns.get_loc("Count")] = np.nan
# df1["Date Ended"] = datetime.datetime.now()
# df1.iloc[1:, df1.columns.get_loc("Date Ended")] = np.nan

# path = "/Users/vishkorra/Desktop/Simply Hired/"

formatting_data_start = time.time()

if os.path.exists(f"Statistics of Simply Hired All.xlsx"):
    df2 = pd.read_excel(f"Statistics of Simply Hired All.xlsx")
    df2 = pd.concat([df2, df1])
    df2.to_excel(f"Statistics of Simply Hired All.xlsx", index=False)
else:
    df1.to_excel(f"Statistics of Simply Hired All.xlsx", index=False)

new_df = pd.DataFrame(columns=df.columns)
non_matched_jobs = pd.DataFrame(columns=df.columns)
current_title = ""

for index, row in job_list.iterrows():
    JobTitle = row["Job Title"]
    Location = row["City"]
    State = row["State"]
    JobTitle = JobTitle.lower().strip()
    Location = Location.lower().strip()
    State = State.replace(" ", "").strip()
    JobTitlePossibleLocations = []

    for inner_index, inner_row in job_list.iterrows():
        inner_job_title = inner_row["Job Title"]
        inner_job_title = inner_job_title.lower().replace(" ", "").strip()
        if inner_job_title == JobTitle:
            JobTitlePossibleLocations.append(inner_row["City"])

    for inner_index, inner_row in df.iterrows():
        CurrentTitle = inner_row["Title"]
        CurrentLocation = inner_row["Location"]
        CurrentTitle = CurrentTitle.lower().replace(" ", "")
        CurrentLocation = CurrentLocation.lower().replace(" ", "")
        CurrentLocation = CurrentLocation.split(",")
        CurrentState = inner_row["State"]
        CurrentState = CurrentState.replace(" ", "").strip()
        for each in CurrentLocation:
            each = each.lower().strip()
        if CurrentTitle.__contains__(JobTitle) and (State == CurrentState):
            # add the inner_row with only the location that matches to the new data frame
            row_to_append = inner_row
            for location in CurrentLocation:
                row_to_append["Location"] = location
                new_df = pd.concat([new_df, pd.DataFrame([row_to_append])])

            df = df.drop(inner_index)


non_matched_jobs = pd.concat([non_matched_jobs, df])

# add non_matched_jobs to the new data frame
ordered_df = pd.concat([new_df, non_matched_jobs])

# add a column to the new data frame named 'Added Date' and set every value to the current date
ordered_df["Added Date"] = datetime.datetime.now().date()


# check if Statistics of Simply Hired All.xlsx exists
if os.path.exists(f"ORDERED Simply Hired All.xlsx"):
    df3 = pd.read_excel(f"ORDERED Simply Hired All.xlsx")
    df3 = pd.concat([df3, ordered_df])
    df3.to_excel(f"ORDERED Simply Hired All.xlsx", index=False)
else:
    # if it doesn't exist, create a new file
    ordered_df.to_excel(f"ORDERED Simply Hired All.xlsx", index=False)

print("Formatting time total:", time.time() - formatting_data_start)
