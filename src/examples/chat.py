#!/usr/bin/env python

import subprocess
import openai
import os
import shlex
from yaspin import yaspin

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

current_branch = subprocess.check_output(
    "git branch --show-current", shell=True, universal_newlines=True).strip()

if current_branch == "main" or current_branch == "master":
    print("Error: cannot create pull request from main or master branch. Checkout to some branch first.")
    exit(1)


def gitprlog():
    with yaspin(text="Collecting git log", color="yellow") as spinner:
        try:
            base_commit = subprocess.check_output(
                "git merge-base HEAD $(git show-ref --verify --quiet refs/heads/main && echo 'main' || echo 'master')",
                shell=True,
                universal_newlines=True
            ).strip()

            output = subprocess.check_output(
                f'git log --reverse --pretty=format:"* %s%n%w(0,4,4)%b" {base_commit}..HEAD',
                shell=True,
                universal_newlines=True
            )
            spinner.ok("✔")
            return output
        except subprocess.CalledProcessError as e:
            spinner.fail("✗")
            print(f"An error occurred while collecting git log: {str(e)}")
            return None


def generate_chat_response(prompt, log_text):
    openai.api_key = OPENAI_API_KEY

    with yaspin(text=log_text, color="yellow") as spinner:
        try:
            response = openai.ChatCompletion.create(
                model='gpt-3.5-turbo',
                temperature=0.2,
                n=1,
                messages=[
                    {'role': 'user', 'content': prompt}
                ]
            )
            output = response.choices[0].message.content.strip().strip('"')
            spinner.ok("✔")
            return output
        except Exception as e:
            spinner.fail("✗")
            print(
                f"An error occurred during chat response generation: {str(e)}")
            return None


def generate_title(text):
    prompt = f"You are expert software engineer preparing a title of PR. Summarize a key changes into a short (max 100 chars) PR title from git log:\n\n{text}\n\n"

    return generate_chat_response(prompt, "Summarizing title")


def generate_description(text):
    prompt = f"You are expert programmer preparing a description of a PR. Use git log to generate a concise description written in markdown. Answer only with description. Git log:\n\n{text}"

    return generate_chat_response(prompt, "Summarizing body")


def create_pull_request(title, body):
    print("Creating pull request...")
    title = shlex.quote(title)
    body = shlex.quote(body)
    command = f'gh pr create --assignee @me --title {title} --body {body}'
    exit_code = os.system(command)
    if exit_code == 0:
        print("Pull request created successfully.")
    else:
        print("An error occurred while creating the pull request.")


git_log = gitprlog()
if git_log:
    title = generate_title(git_log)
    description = generate_description(git_log)
    log = git_log.replace('\n\n', '\n')
    body = f"{description}\n\nCommit log:\n\n{log}"
    if title and body:
        create_pull_request(title, body)
    else:
        print("An error occurred while generating title or description.")
        print("Pull request will not be created.")