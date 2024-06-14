[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/--SLCZVD)
# Streaming Pipelines
Homework for Week 5 Streaming Pipelines

## Homework Notes:

I have enhanced the original code to meet the assignment requirements for tracking Data Expert sessions. First, I created a DDL script (session_ddl.sql) to define a new table shashankkongara.kafka_streaming_user_sessions_homework, which includes columns for session details such as session ID, start and end times, event count, session date, geolocation (country, state, city), device and browser information, and login status. 

I have added a DDL statement in "session_ddl.sql" file to create the table which gets the data from Kafka session window.

In the Spark Streaming job (session_job.py), I added Kafka configurations, implemented a function to fetch geolocation details from the ip2location API, and defined the schema for Kafka message values. I updated the job to group events into sessions with a 5-minute inactivity window and generated unique session IDs using a hash of user ID, IP, and session start time. Additionally, I enriched the session data with geolocation and device/browser details and wrote the processed data to the output table in Iceberg format, partitioned by the session start date. These changes ensure effective tracking and processing of sessions, meeting the assignment requirements.


## Submission Guidelines

To ensure smooth processing of your submissions through GitHub Classroom automation, we cannot accommodate individual requests for changes. Therefore, please read all instructions carefully.

1. **Sync fork:** Ensure your fork is up-to-date with the upstream repository. You can do this through the GitHub UI or by running the following commands in your local forked repository:
    ```bash
    git pull upstream main --rebase
    git push origin <main/your_homework_repo> -f
    ```
2. **Open a PR in the upstream repository to submit your work:**
    - Open a Pull Request (PR) to merge changes from your `main` or custom `homework` branch in your forked repository into the main branch in the upstream repository, i.e., the repository your fork was created from.
    - Ensure your PR is opened correctly to trigger the GitHub workflow. Submitting to the wrong branch or repository will cause the GitHub action to fail.
    - See the [Steps to open a PR](#steps-to-open-a-pr) section below if you need help with this.
3. **Complete assignment prompts:** Write your query files corresponding to the prompt number in the **`submission`** folder. Do not change or rename these files!
4. **Lint your SQL code for readability.** Ensure your code is clean and easy to follow.
5. **Add comments to your queries.** Use the **`--`** syntax to explain each step and help the reviewer understand your thought process. 

A link to your PR will be shared with our TA team after the homework deadline.

### Steps to open a PR
  1. Go to the upstream [**`streaming-pipelines`**](https://github.com/DataExpert-ZachWilson-V4/streaming-pipelines) repository
  2. Click the [**Pull Requests**](https://github.com/DataExpert-ZachWilson-V4/streaming-pipelines/pulls) tab.
  3. Click the **"New pull request"** button on the top-right. This will take you to the [**"Compare changes"**](https://github.com/DataExpert-ZachWilson-V4/streaming-pipelines/compare) page.
  4. Click the **"compare across forks"** link in the text.
  5. Leave the base repository as is. For the **"head repository"**, select your forked repository and then the name of the branch you want to compare from your forked repo.
  6. Click the **"Create pull request"** button to open the PR

### Grading
  - Grades are pass or fail, used solely for certification.
  - Final grades will be submitted by a TA **after the deadline**.
  - An approved PR means a Pass grade. If changes are requested, the grade will be marked as Fail.
  - Reviewers may provide comments or suggestions with requested changes. These are optional and intended for your benefit. Any changes you make in response will not be re-reviewed.

Assignment
==================

Your task is to write a streaming pipeline to understand DataExpert.io user behavior using sessionization. 
Use the `glue_job_runner.py` connectors and examples in the [airflow-dbt-project](https://github.com/DataExpert-io/airflow-dbt-project) repo to get started. Copy and paste the DDL into `submission/session_ddl.sql` and copy the job into `submission/session_job.py`!

## Assignment Tasks

- Write a DDL query (`session_ddl.sql`) that creates a table that tracks Data Expert sessions. 
  - Make sure to have a unique identifier for each session
  - the start and end of the session
  - a unique identifier for the session
  - how many events happened in that session
  - the date of the beginning of the session
  - What city, country, and state is associated with this session
  - What operating system and browser are associated with this session
  - Whether this session is for logged in or logged out users

  
- Write a Spark Streaming Job (`session_job.py`) that reads from Kafka
  - It groups the Kafka events with a `session_window` with a 5-minute gap (sessions end after 5 minutes of inactivity)
    - It is also grouped by `user_id` and `ip` to track each user behavior
  - Make sure to increase the `timeout` of your job to `1 hour` so you can capture real sessions
  - Create a unique identifier for each session called `session_id` (you should think about using `hash` and the necessary columns to uniquely identify the session)
  - If `user_id` is not null, that means the event is from a logged in user
  - If a user logs in, that creates a new session (if you group by `ip` and `user_id` that solves the problem pretty elegantly)
