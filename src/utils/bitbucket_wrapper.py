import os.path
from utils.config_wrapper import ConfigWrapper
from git import Repo, GitCommandError

def clone_fresh(username, repository_address, token, local_path):
    repository_url = f"https://{username}:{token}@{repository_address}"
    print(f"Cloning {repository_url} into {local_path}")
    print("BitBucket Repo Synchronization In Progress")
    try:
        Repo.clone_from(repository_url, local_path)
        print("\nCompleted Cloning")
        return 1, True, 'Success'
    except GitCommandError as e:
        print("Clone failed:", e)
        return 1, False, e

def pull_latest(local_path):
    try:
        print("Fetching updates from the remote repository...")
        repo = Repo(local_path)
        origin = repo.remotes.origin
        origin.pull('dev')
        repo.git.reset('--hard', 'origin/dev')
        print("Reset --hard origin/dev completed successfully.")
        return 2, True, 'Success'
    except GitCommandError as e:
        print("Git error:", e)
        return 2, False, e


class BitbucketWrapper(ConfigWrapper):
    def __init__(self):
        super().__init__()

    def parse_parameters(self, value, environment_key):
        if value is not None:
            return value
        else:
            return self.getenv(environment_key)

    def clone_repo(self, _username=None, _repository_address=None, _token=None, _local_path=None):
        username = self.parse_parameters(_username, 'Bitbucket_username')
        repository_address = self.parse_parameters(_repository_address, 'bit_bucket_repo_address')
        token = self.parse_parameters(_token, 'BitbucketToken')
        local_path = self.parse_parameters(_local_path, 'Bitbucket_Local_Path')

        if os.path.exists(local_path) and os.path.isdir(local_path):
            # fetch latest code
            print("Local Path is already existed. Checking for repository and fetch latest")
            return pull_latest(local_path)

        else:
            # Clone a fresh
            print("Cloning the repository fresh")
            return clone_fresh(username, repository_address, token, local_path)

if __name__ == "__main__":
    bucket = BitbucketWrapper()
    bucket.clone_repo()