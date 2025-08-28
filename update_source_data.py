import os
import re
import shutil
import tempfile
import subprocess
import datetime
import io
import sys
import pandas as pd
from contextlib import redirect_stdout

from utils.read_confg import read_repos_yaml


def clone_and_extract_dirs(repo_url, dirs_to_copy, output_dir, ref='main', ref_type='branch'):
    """
    Clone a GitHub repo to a temp folder, copy specific directories, and clean up.

    Parameters:
        repo_url (str): GitHub repo URL (e.g., https://github.com/user/repo.git)
        dirs_to_copy (list of str): List of relative directory paths to copy
        output_dir (str): Where to save the copied directories
        ref (str): Branch name or tag to checkout (default: main)
        ref_type (str): Type of reference ('branch' or 'tag')
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"📥 Cloning {repo_url} ({ref_type}: {ref}) into temp folder...")
        if ref_type == 'tag':
            # For tags, we first do a shallow clone and then fetch the specific tag
            subprocess.run(["git", "clone", "--depth", "1", repo_url, tmpdir], check=True)
            # Navigate to the cloned directory
            current_dir = os.getcwd()
            os.chdir(tmpdir)
            # Fetch the specific tag
            subprocess.run(["git", "fetch", "--depth", "1", "origin", f"refs/tags/{ref}:refs/tags/{ref}"], check=True)
            # Checkout the tag
            subprocess.run(["git", "checkout", f"tags/{ref}"], check=True)
            # Return to original directory
            os.chdir(current_dir)
        else:
            # For branches, use the original approach
            subprocess.run(["git", "clone", "--depth", "1", "--branch", ref, repo_url, tmpdir], check=True)

        for relative_path in dirs_to_copy:
            src = os.path.join(tmpdir, relative_path)
            dst = os.path.join(output_dir, os.path.basename(relative_path))
            if os.path.exists(src):
                print(f"📁 Copying {relative_path} to {dst}")
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                print(f"⚠️ Directory not found: {relative_path}")

        print(f"✅ Done! Selected directories copied to {output_dir}")


def get_github_release_tags(repo_url, last_version=True):
    """
    Get a list of release tags from a GitHub repository without cloning.

    Parameters:
        repo_url (str): GitHub repo URL (e.g., https://github.com/user/repo.git)
        last_version (bool): If True, only returns last available version of each tag sharing the
            same date ids, for tags in a `"YYYY-MM-DD-vX"` format (`"-vX"` optional)

    Returns:
        list: A list of release tags
    """
    print(f"🏷️ Fetching tags from {repo_url}...")

    try:
        # Get all tags using ls-remote
        result = subprocess.run(
            ["git", "ls-remote", "--tags", repo_url],
            check=True,
            capture_output=True,
            text=True
        )

        # Parse output to extract tag names
        lines = result.stdout.strip().split('\n')
        tags = []
        for line in lines:
            if not line:
                continue
            # Format is: <commit-hash>\trefs/tags/<tag-name>
            parts = line.split('\t')
            if len(parts) == 2:
                # Extract tag name and remove potential ^{} suffix for annotated tags
                tag = parts[1].replace('refs/tags/', '')
                if not tag.endswith('^{}'):  # Skip the peeled tag references
                    tags.append(tag)

        # Select tag of interest
        if last_version:
            sel_tags = []
            for tag in tags:
                if re.findall("-v", tag):
                    sel_tag = [tag] + re.split("-v", tag)
                else:
                    sel_tag = [tag, tag, '0']
                sel_tags.append(sel_tag)
            tag_df = pd.DataFrame(sel_tags).rename(columns={0: 'tag', 1: 'round', 2: 'version'})
            tags = tag_df.loc[tag_df.groupby(['round'])["version"].idxmax()]["tag"].tolist()

        print(f"Found {len(tags)} tags in repository")
        return tags
    except subprocess.CalledProcessError as e:
        print(f"❌ Error fetching tags: {e}")
        return []

def delete_ignored_files_and_directories(directory, ignore_files_regex):
    """
    Delete files and directories in the specified directory that match the ignore regex.

    Parameters:
        directory (str): Directory to search for files and directories to delete
        ignore_files_regex (list of str): List of regex patterns to match items to delete
    """
    # First pass: Delete matching files
    for root, dirs, files in os.walk(directory):
        for file in files:
            for pattern in ignore_files_regex:
                if re.search(pattern, file):
                    file_path = os.path.join(root, file)
                    print(f"🗑️ Deleting file: {file_path}")
                    os.remove(file_path)
                    break  # No need to check other patterns

    # Second pass: Identify and delete matching directories (bottom-up)
    dirs_to_delete = []
    for root, dirs, _ in os.walk(directory, topdown=False):
        for dir_name in dirs:
            for pattern in ignore_files_regex:
                if re.search(pattern, dir_name):
                    dir_path = os.path.join(root, dir_name)
                    dirs_to_delete.append(dir_path)
                    break  # No need to check other patterns

    # Delete directories (from deepest to shallowest)
    for dir_path in sorted(dirs_to_delete, key=len, reverse=True):
        if os.path.exists(dir_path):  # Check if it still exists (might have been deleted as part of a parent)
            print(f"🗑️ Deleting directory: {dir_path}")
            shutil.rmtree(dir_path)


if __name__ == "__main__":
    # Capture stdout for logging
    output_capture = io.StringIO()

    # Get current date and time for the log
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with redirect_stdout(output_capture):
        print(f"Data update started at: {current_time}\n")

        # Read configuration from YAML
        config = read_repos_yaml()
        repositories = config['repositories']
        base_output_dir = config['data_directory']

        os.makedirs(base_output_dir, exist_ok=True)

        if not repositories:
            print("⚠️ No repositories found in configuration file")
            exit(1)

        print(f"Found {len(repositories)} repositories to process")
        print(f"Base output directory: {base_output_dir}")
        print(f"Ignore files regex: {config['ignore_files_regex']}")
        ignore_files_regex = config['ignore_files_regex']

        # Process each repository
        for repo_config in repositories:
            repo_url = repo_config.get('url')
            directories = list(repo_config["directories"].values())

            if not repo_url:
                print("⚠️ Skipping repository with missing URL")
                continue

            print(f"\n📦 Processing repository: {repo_url}")

            # Get tags for this repository
            tags = get_github_release_tags(repo_url)

            if not tags:
                print(f"⚠️ No tags found for {repo_url}, falling back to branch")
                # Fall back to using the specified branch if no tags found
                branch = repo_config.get('branch', 'main')
                try:
                    output_dir = base_output_dir
                    print(f"Using branch: {branch}")
                    clone_and_extract_dirs(repo_url, directories, output_dir, branch, 'branch')
                except Exception as e:
                    print(f"❌ Error processing repository {repo_url} with branch {branch}: {e}")
                continue

            # Process each tag
            print(f"Found {len(tags)} tags to process")
            for tag in tags:
                try:
                    # Create tag-specific output directory
                    tag_output_dir = os.path.join(base_output_dir, tag)
                    os.makedirs(tag_output_dir, exist_ok=True)

                    print(f"\n🏷️ Processing tag: {tag}")
                    clone_and_extract_dirs(repo_url, directories, tag_output_dir, tag, 'tag')
                except Exception as e:
                    print(f"❌ Error processing tag {tag} for repository {repo_url}: {e}")

        print("\n🎉 All repositories processed!")

        # Apply ignore patterns to the entire output directory structure
        delete_ignored_files_and_directories(base_output_dir, ignore_files_regex)

        print(f"\nData update completed at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Write the captured output to the log file
    log_content = output_capture.getvalue()
    log_file_path = os.path.join(base_output_dir, "last_update.log")

    # Also print to console
    print(log_content)

    # Write to file
    with open(log_file_path, 'w') as log_file:
        log_file.write(log_content)

    print(f"Log saved to {log_file_path}")

