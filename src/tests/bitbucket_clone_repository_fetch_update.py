import os

from utils.bitbucket_wrapper import BitbucketWrapper
from shutil import rmtree

def test_clone_repository_fetch_update():
    bucket = BitbucketWrapper()
    op_type, status, message = bucket.clone_repo() # op 1 for cloning, op 2 for fetching details
    assert op_type == 2 and status == True

