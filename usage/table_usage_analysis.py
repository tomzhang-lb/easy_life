from utils.table_usage import git_download, get_github_token, table_column_usage


def main():
    token_config_file = 'github_config.ini'
    git_repo = 'lifebyte-systems/lb-data-mars-ui'
    git_repo_name = git_repo.split('/')[1]
    github_token, github_path = get_github_token(token_config_file)
    git_download(github_token, git_repo, github_path)

    table_name = 'report_server_trades_closed'
    column_names = ['usd_pnl_c', 'usd_pnl_d']
    result_file_list = table_column_usage(table_name, column_names, f'{github_path}/{git_repo_name}')
    print(f'Result files are:')
    for result_file in result_file_list:
        print(result_file)


if __name__ == '__main__':
    main()
