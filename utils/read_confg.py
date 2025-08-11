import yaml


def read_repos_yaml(yaml_path="config/repos.yaml"):
    """Read the repositories configuration from YAML file"""
    try:
        with open(yaml_path, 'r') as file:
            config = yaml.safe_load(file)
            return {
                'ignore_files_regex': config.get('ignore_files_regex', []),
                'repositories': config.get('repositories', []),
                'data_directory': config.get('data_directory')
            }
    except Exception as e:
        print(f"❌ Error reading YAML file: {e}")