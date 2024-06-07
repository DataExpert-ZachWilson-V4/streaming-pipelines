import os
import re
import logging
import requests
import boto3
from util import get_logger, get_api_key, check_aws_creds, get_git_creds, get_assignment, \
    get_submission_dir  # , get_changed_files
from openai import OpenAI

logger = get_logger()
client = OpenAI(api_key=get_api_key())

submission_dir = get_submission_dir()
git_token, repo, pr_number = get_git_creds()
s3_bucket = check_aws_creds()


def get_submissions(submission_dir: str) -> dict:
    submissions = {}
    try:
        jobs_dir = os.path.join(submission_dir)
        jobs_files = [
            f for f in os.listdir(jobs_dir)
            if os.path.isfile(os.path.join(jobs_dir, f))
        ]

        files_found = jobs_files
    except FileNotFoundError:
        logger.error(f"Directory not found: {submission_dir}")
        return None

    logger.info(f"Files: {files_found}")
    for sub_file in files_found:
        file_path = os.path.join(submission_dir, sub_file)
        if os.path.isfile(file_path) and (
                re.match(r'.*\.py', sub_file)
                or re.match(r'.*\.sql', sub_file)):
            try:
                with open(file_path, "r") as file:
                    file_content = file.read()
                if re.search(r'\S', file_content):
                    submissions[file_path] = file_content
            except FileNotFoundError:
                logger.info(f"File not found: {file_path}")
                continue

    if not submissions:
        logger.warning('No submissions found')
        return None

    sorted_submissions = dict(sorted(submissions.items()))
    return sorted_submissions


def download_from_s3(s3_bucket: str, s3_path: str, local_path: str):
    s3 = boto3.client('s3')
    try:
        s3.download_file(s3_bucket, s3_path, local_path)
    except Exception as e:
        raise Exception(f"Failed to download from S3: {e}")


def get_prompts(assignment: str) -> dict:
    s3_solutions_dir = f"academy/2/homework-keys/streaming-pipelines"
    local_solutions_dir = os.path.join(os.getcwd(), 'prompts', assignment)
    os.makedirs(local_solutions_dir, exist_ok=True)
    prompts = [
        'session_ddl.md',
        'session_job.md',
        'session_ddl.sql',
        'session_job.py'
    ]
    prompt_contents = {}
    for prompt in prompts:
        s3_path = f"{s3_solutions_dir}/{prompt}"
        print(s3_path)
        local_path = os.path.join(local_solutions_dir, prompt)
        download_from_s3(s3_bucket, s3_path, local_path)
        if not os.path.exists(local_path):
            raise ValueError(f"File failed to download to {local_path}. Path does not exist.")
        with open(local_path, "r") as file:
            prompt_contents[prompt] = file.read()
    return prompt_contents


def generate_system_prompt(prompts: dict):
    system_prompt = ""
    system_prompt += "# Additional Information"
    system_prompt += f"\n\n{prompts['session_ddl.md']}"
    system_prompt += f"\n\n{prompts['session_job.md']}"
    system_prompt += f"\n\n{prompts['session_job.py']}"
    system_prompt += f"\n\n{prompts['session_ddl.sql']}"
    system_prompt += "\n\n"

    return system_prompt


def generate_feedback_prompt(prompts: dict, submissions: dict) -> str:
    user_prompt = prompts['session_ddl.md']
    user_prompt += "\n\n"
    for file_name, submission in submissions.items():
        user_prompt += "# Student's Solution"
        user_prompt += f"Please analyze the following code in `{file_name}`:\n\n```\n{submission}\n```\n\n"
    return user_prompt


def generate_grading_prompt(prompts: dict, submissions: dict) -> str:
    user_prompt = prompts['session_job.md']
    user_prompt += "\n\n"

    # user_prompt += "# Additional Information"
    # user_prompt += f"\n\n{prompts['week_1_queries.md']}"
    # user_prompt += f"\n\n{prompts['week_2_queries.md']}"
    # user_prompt += f"\n\n{prompts['example_solution.md']}"
    # user_prompt += "\n\n"

    for file_name, submission in submissions.items():
        user_prompt += "# Student's Solution"
        user_prompt += f"Please grade the following code in `{file_name}`:\n\n```\n{submission}\n```\n\n"
    return user_prompt


def get_response(system_prompt: str, user_prompt: str) -> str:
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1
    )
    comment = response.choices[0].message.content
    # text = f"This is a LLM-generated comment: \n{comment if comment else 'No feedback generated.'}"
    return comment


def post_github_comment(git_token, repo, pr_number, comment):
    url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {git_token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    data = {"body": comment}
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 201:
        logger.error(f"Failed to create comment. Status code: {response.status_code} \n{response.text}")
        raise Exception(f"Failed to create comment. Status code: {response.status_code} \n{response.text}")
    logger.info(f"✅ Added review comment at https://github.com/{repo}/pull/{pr_number}")


def main():
    submissions = get_submissions(submission_dir)
    if not submissions:
        logger.warning(
            f"No comments were generated because no files were found in the `{submission_dir}` directory. Please modify one or more of the files at `src/jobs/` or `src/tests/` to receive LLM-generated feedback.")
        return None

    assignment = get_assignment()
    prompts = get_prompts(assignment)

    system_prompt = generate_system_prompt(prompts)

    feedback_prompt = generate_feedback_prompt(prompts, submissions)
    feedback_comment = get_response(system_prompt, feedback_prompt)

    grading_prompt = generate_grading_prompt(prompts, submissions)
    grading_comment = get_response(system_prompt, grading_prompt)

    final_comment = f"### Feedback:\n{feedback_comment}\n\n### Grade:\n{grading_comment}"

    if git_token and repo and pr_number:
        post_github_comment(git_token, repo, pr_number, final_comment)

    return final_comment


if __name__ == "__main__":
    main()
