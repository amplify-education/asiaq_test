"""
Integration tests for disco_accounts.py
"""
from random import randint
from test.helpers.integration_helpers import IntegrationTest


TEST_ACCOUNT_NAME = "test_account"
TEST_ACCOUNT_S3_USER_KEY = "accounts/users/{account_name}"
TEST_ACCOUNT_S3_GROUP_KEY = "accounts/groups/{account_name}"
TEST_ACCOUNT_S3_KEYS = [TEST_ACCOUNT_S3_USER_KEY, TEST_ACCOUNT_S3_GROUP_KEY]
CREATE_CMD = "disco_accounts.py adduser --name {account_name} --password password"
EDIT_CMD = "disco_accounts.py edituser --name {account_name}"
DISABLE_CMD = "disco_accounts.py edituser --name {account_name} --active no"
REMOVE_CMDS = ["disco_creds.py delete --key %s" % _key for _key in TEST_ACCOUNT_S3_KEYS]


class DiscoAccountsTests(IntegrationTest):
    """
    Tests bin/disco_accounts.py
    """
    _multiprocess_shared_ = True

    def _gen_account_name(self, postfix):
        return TEST_ACCOUNT_NAME + str(postfix)

    def _create_test_account(self, postfix):
        account_name = self._gen_account_name(postfix=postfix)

        output = self.run_cmd(
            CREATE_CMD.format(account_name=account_name).split(),
            environ={"EDITOR": "true"}
        )

        return output

    def _remove_test_account(self, postfix):
        account_name = self._gen_account_name(postfix=postfix)

        formatted_remove_cmds = [
            cmd.format(account_name=account_name)
            for cmd in REMOVE_CMDS
        ]

        for cmd in formatted_remove_cmds:
            self.run_cmd(cmd.split())

    def _get_test_account_settings(self, postfix):
        account_name = self._gen_account_name(postfix=postfix)

        output = self.run_cmd(
            EDIT_CMD.format(account_name=account_name).split(),
            environ={"EDITOR": "cat"}
        )

        return output

    def _is_test_account_active(self, postfix):
        return "active = yes" in self._get_test_account_settings(postfix=postfix)

    def _disable_test_account(self, postfix):
        account_name = self._gen_account_name(postfix=postfix)

        self.run_cmd(DISABLE_CMD.format(account_name=account_name).split())

    def test_create_account(self):
        """
        we can create a new unix user account
        """
        postfix = randint(10000, 99999)
        account_name = self._gen_account_name(postfix=postfix)
        self._create_test_account(postfix=postfix)

        try:
            user_output = self.run_cmd("disco_accounts.py listusers".split())
            group_output = self.run_cmd("disco_accounts.py listgroups".split())
            self.assertIn(account_name, user_output)
            self.assertIn(account_name, group_output)
        finally:
            self._remove_test_account(postfix=postfix)

    def test_disable_account(self):
        """
        we can disable an existing active account
        """
        postfix = randint(10000, 99999)
        self._create_test_account(postfix=postfix)

        try:
            self.assertTrue(self._is_test_account_active(postfix=postfix))
            self._disable_test_account(postfix=postfix)
            self.assertFalse(self._is_test_account_active(postfix=postfix))
        finally:
            self._remove_test_account(postfix=postfix)
