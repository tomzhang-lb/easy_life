import configparser
import os
from pathlib import Path
from git import Repo
from github import Github


class RepoNotExistsException(Exception):
    def __init__(self, value, message="repo does not exist"):
        self.value = value
        self.message = message
        super().__init__(f"{message}: {value}")


def get_github_token(config_file):
    config_file_exists = try_utf8(config_file)
    if not config_file_exists:
        print(f'{config_file} does not exist')
        raise FileNotFoundError

    # Initialize the parser
    github_token = ''
    github_path = ''
    try:
        config = configparser.ConfigParser()
        config.read(config_file)
        github_token = config['GIT']['github_token']
        github_path = config['GIT']['github_path']
    except Exception as e:
        print(e)
        print('unable to read [GIT][github_token]')

    return github_token, github_path


def check_repo_exists(github_token, git_repos):
    git_repos_all = []
    if isinstance(git_repos, str):
        git_repos_all.append(git_repos)
    elif isinstance(git_repos, list) and len(git_repos) > 0:
        git_repos_all.extend(git_repos)
    else:
        print(f'Invalid input of {git_repos}')

    # Get the authenticated user
    git = Github(github_token)
    git_user = git.get_user()
    print(f"Logged in as: {git_user.login}")

    all_repos = [repo.git_url for repo in git_user.get_repos()]
    # print(all_repos)

    # git_repos_url = ['git://github.com/' + git + '.git' for git in git_repos]
    # exists_flat = all(elem in all_repos for elem in git_repos_url)
    for repo in git_repos_all:
        repo_url = 'git://github.com/' + repo + '.git'
        if repo_url not in all_repos:
            print(f'{repo} does not exist, please check if the repository name is correct.')
            return False

    return True


def git_download(github_token, git_repo, local_path):
    # first check if the git repo exists
    if check_repo_exists(github_token, git_repo):
        repo_url = f'https://{github_token}@github.com/{git_repo}.git'
        git_repo_name = git_repo.split('/')[1]
        git_repo_local_path = f'{local_path}/{git_repo_name}'

        if os.path.exists(git_repo_local_path):
            print(f'Path {git_repo_local_path} already exists')
            print(f"Pulling updates in {git_repo_local_path}")
            repo = Repo(git_repo_local_path)
            origin = repo.remotes.origin
            origin.pull()
        else:
            Repo.clone_from(repo_url, git_repo_local_path)
            print(f'Cloned {repo_url} successfully to {git_repo_local_path}')
    else:
        raise RepoNotExistsException(git_repo)


def try_utf8(file):
    "Returns a Unicode object on success, or None on failure"
    try:
       return open(file, encoding='UTF-8').read()
    except UnicodeDecodeError:
       return None


def table_usage(table_name, location):
    all_files = list(Path(location).rglob('*.*'))
    # picture_type_list = ['png', 'jpg', 'jpeg', 'gif', 'woff']
    result_file_list = []

    for file in all_files:
        if file.is_file() and try_utf8(file) is not None:
            with open(file, encoding='utf-8') as f:
                if table_name in f.read().lower():
                    print(file)
                    result_file_list.append(file)

    return result_file_list


def table_column_usage(table_name, column_names, location):
    all_files = list(Path(location).rglob('*.*'))
    # picture_type_list = ['png', 'jpg', 'jpeg', 'gif', 'woff']
    result_file_list = []

    for file in all_files:
        if file.is_file() and try_utf8(file) is not None:
            with open(file, encoding='utf-8') as f:
                file_content = f.read().lower()
                if table_name.lower() in file_content and any(x.lower() in file_content for x in column_names):
                    # print(file)
                    result_file_list.append(file)
    return result_file_list


if __name__ == '__main__':
    token_config_file = 'github_config.ini'
    git_repo = 'lifebyte-systems/lb-data-mars-ui'
    git_repo_name = git_repo.split('/')[1]
    github_token, github_path = get_github_token(token_config_file)
    git_download(github_token, git_repo, github_path)

    # search
    table_name = 'report_server_trades_closed'
    column_names = ['usd_pnl_c', 'usd_pnl_d']
    result_file_list = table_column_usage(table_name, column_names, f'{github_path}/{git_repo_name}')
    print(f'Result files are:')
    for result_file in result_file_list:
        print(result_file)

