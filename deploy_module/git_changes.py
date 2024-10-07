import subprocess
import os


def get_changed_files():
    subprocess.run(["git", "fetch", "--depth=2"], check=True)

    # Get the list of changed files in the latest commit
    result = subprocess.run(
        ["git", "diff", "--name-only", "HEAD", "HEAD~1"],
        capture_output=True,
        text=True,
        check=True
    )

    changed_items = result.stdout.strip().split('\n')
    current_directory = os.getcwd()
    full_paths = [os.path.join(current_directory, file) for file in changed_items]
    return full_paths


