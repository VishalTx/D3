import pytest
from numpy.ma.testutils import assert_not_equal

from utils.jira_wrapper import JiraWrapper

def test_jira():
    j = JiraWrapper()
    print(j.jira_user)
    assert j.jira_user is not None

def test_create_issue():
    j = JiraWrapper()
    print(j.jira_manager)
    print(j.jira_user)
    issue_id = j.log_task(
        tsummary="VDI-Test-Vinay",
        tdescription="VDI-Test",
        issue_type='Bug'
    )
    print(issue_id)
    assert issue_id != ''