import mysql.connector
import time
import json
import requests
from datetime import date
import html2text


# Connects to database
def connect_to_sql():
    conn = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1', database='cne340')
    return conn


def create_tables(cursor):
    # Creates table
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (job_id INT PRIMARY KEY, 
    company varchar(300), created_at date, url varchar(30000), title varchar(100) CHARSET utf8, description LONGBLOB, 
    category varchar(50), job_type varchar(20), location varchar(50), salary varchar(200)); ''')


# Query the database.
def query_sql(cursor, query):
    cursor.execute(query)
    return cursor


# Add a new job
def add_new_job(cursor, jobdetails):
    description = html2text.html2text(jobdetails['description'])
    date = jobdetails['publication_date'][0:10]
    job_id = jobdetails['id']
    company = jobdetails['company_name']
    url = jobdetails['url']
    title = jobdetails['title']
    category = jobdetails['category']
    job_type = jobdetails['job_type']
    location = jobdetails['candidate_required_location']
    salary = jobdetails['salary']
    query = cursor.execute("INSERT INTO jobs( description, created_at, job_id, company, url, title, category, job_type, location, salary " ") "
               "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (  description, date, job_id, company, url, title, category, job_type, location, salary))
    return query_sql(cursor, query)


# Check if new job
def check_if_job_exists(cursor, jobdetails):
    job_id = jobdetails['id']
    query = "SELECT * FROM jobs WHERE job_id = \"%s\"" % job_id
    return query_sql(cursor, query)

# Deletes job
def delete_job(cursor, jobdetails):
    job_id = jobdetails['id']
    query = "DELETE FROM jobs WHERE job_id = \"%s\"" % job_id
    return query_sql(cursor, query)


# Grab new jobs from a website, Parses JSON code and inserts the data into a list of dictionaries do not need to edit
def fetch_new_jobs():
    query = requests.get("https://remotive.com/api/remote-jobs")
    datas = json.loads(query.text)
    return datas


def jobhunt(cursor):
    # Fetch jobs from website
    jobpage = fetch_new_jobs()  # Gets API website and holds the json data in it as a list
    add_or_delete_job(jobpage, cursor)


def add_or_delete_job(jobpage, cursor):
    for jobdetails in jobpage['jobs']:  # EXTRACTS EACH JOB FROM THE JOB LIST.
        check_if_job_exists(cursor, jobdetails)
        is_job_found = len(cursor.fetchall()) > 0
        if is_job_found:
            # Prints if job is already found
            print("Job already found in database: " + jobdetails["title"] + " from " + jobdetails["company_name"])
        else:
            # Prints if a new job is found
            print("New job found: " + jobdetails["title"] + " from " + jobdetails["company_name"])
            add_new_job(cursor, jobdetails)


# Setup portion of the program. Take arguments and set up the script
def main():
    # Connect to SQL and get cursor
    conn = connect_to_sql()
    cursor = conn.cursor()
    create_tables(cursor)

    while (1):  # Infinite Loops. Only way to kill it is to crash or manually crash it.
        jobhunt(cursor)
        time.sleep(14400)  # Sleeps for 4 hours


if __name__ == '__main__':
    main()

