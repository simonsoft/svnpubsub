import unittest
from svnauthzsub import generate, validate


class SvnAuthzSubTestCase(unittest.TestCase):

    def test_permissions_read_only(self):
        repo = "testrepo"
        input = [
            "[/qa/readonly]",
            "* =",
            "@CmsUser = r",
            "@CmsAdmin = r",
        ]
        expected = [
            "<Location \"/svn/{}/qa/readonly\" >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method GET PROPFIND OPTIONS REPORT",
            "<RequireAny>",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUser(,[^,]+)*$/",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsAdmin(,[^,]+)*$/",
            "</RequireAny>",
            "</RequireAll>",
            "</Location>",
        ]
        # Generate the output as a list of lines removing the indentation and empty lines.
        output = [line.strip() for line in generate(input, repo).readlines() if line.strip() != '']
        self.assertListEqual(output, expected)

    def test_permissions_read_write(self):
        repo = "testrepo"
        input = [
            "[/projectB]",
            "* =",
            "@CmsAdmin = rw",
            "@CmsUserSuper = rw",
            "@CmsUser = rw",
        ]
        expected = [
            "<Location \"/svn/{}/projectB\" >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "<RequireAny>",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsAdmin(,[^,]+)*$/",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUserSuper(,[^,]+)*$/",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUser(,[^,]+)*$/",
            "</RequireAny>",
            "</RequireAll>",
            "</Location>",
        ]
        # Generate the output as a list of lines removing the indentation and empty lines.
        output = [line.strip() for line in generate(input, repo).readlines() if line.strip() != '']
        self.assertListEqual(output, expected)

    def test_permissions_asterisk_only(self):
        repo = "testrepo"
        input = [
            "[/projectC]",
            "* =",
            "[/projectD]",
            "* = x",
        ]
        expected = [
            "<Location \"/svn/{}/projectC\" >".format(repo),
            "Require all denied",
            "</Location>",
            "<Location \"/svn/{}/projectD\" >".format(repo),
            "Require all denied",
            "</Location>",
        ]
        # Generate the output as a list of lines removing the indentation and empty lines.
        output = [line.strip() for line in generate(input, repo).readlines() if line.strip() != '']
        self.assertListEqual(output, expected)

    def test_permissions_mixed(self):
        repo = "testrepo"
        input = [
            "[/]",
            "@CmsAdmin = rw",
            "@CmsUserSuper = rw",
            "@CmsUser = r",
        ]
        expected = [
            "<Location \"/svn/{}\" >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method OPTIONS MERGE",
            "</RequireAll>",
            "<RequireAll>",
            "Require valid-user",
            "Require method GET PROPFIND OPTIONS REPORT",
            "<RequireAny>",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUser(,[^,]+)*$/",
            "</RequireAny>",
            "</RequireAll>",
            "<RequireAll>",
            "Require valid-user",
            "<RequireAny>",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsAdmin(,[^,]+)*$/",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUserSuper(,[^,]+)*$/",
            "</RequireAny>",
            "</RequireAll>",
            "</Location>",
        ]
        # Generate the output as a list of lines removing the indentation and empty lines.
        output = [line.strip() for line in generate(input, repo).readlines() if line.strip() != '']
        self.assertListEqual(output, expected)

    def test_multiple_sections(self):
        repo = "testrepo"
        input = [
            "[/projectA]",
            "* =",
            "@CmsAdmin = rw",
            "@CmsUserSuper = rw",
            "@CmsUser = r",
            "[/projectB]",
            "* =",
            "@CmsAdmin = rw",
            "@CmsUserSuper = rw",
            "@CmsUser = rw",
        ]
        expected = [
            "<Location \"/svn/{}/projectA\" >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method GET PROPFIND OPTIONS REPORT",
            "<RequireAny>",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUser(,[^,]+)*$/",
            "</RequireAny>",
            "</RequireAll>",
            "<RequireAll>",
            "Require valid-user",
            "<RequireAny>",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsAdmin(,[^,]+)*$/",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUserSuper(,[^,]+)*$/",
            "</RequireAny>",
            "</RequireAll>",
            "</Location>",
            "<Location \"/svn/{}/projectB\" >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "<RequireAny>",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsAdmin(,[^,]+)*$/",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUserSuper(,[^,]+)*$/",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUser(,[^,]+)*$/",
            "</RequireAny>",
            "</RequireAll>",
            "</Location>",
        ]
        # Generate the output as a list of lines removing the indentation and empty lines.
        output = [line.strip() for line in generate(input, repo).readlines() if line.strip() != '']
        self.assertListEqual(output, expected)

    def test_parent_child_order(self):
        repo = "testrepo"
        input = [
            "[/qa]",
            "* =",
            "@CmsUser = r",
            "[/qa/b/a]",
            "* =",
            "@CmsUser = r",
            "[/qa/b]",
            "* =",
            "@CmsUser = r",
            "[/qa/a]",
            "* =",
            "@CmsUser = r",
        ]
        expected = [
            "<Location \"/svn/{}/qa\" >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method GET PROPFIND OPTIONS REPORT",
            "<RequireAny>",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUser(,[^,]+)*$/",
            "</RequireAny>",
            "</RequireAll>",
            "</Location>",
            "<Location \"/svn/{}/qa/a\" >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method GET PROPFIND OPTIONS REPORT",
            "<RequireAny>",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUser(,[^,]+)*$/",
            "</RequireAny>",
            "</RequireAll>",
            "</Location>",
            "<Location \"/svn/{}/qa/b\" >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method GET PROPFIND OPTIONS REPORT",
            "<RequireAny>",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUser(,[^,]+)*$/",
            "</RequireAny>",
            "</RequireAll>",
            "</Location>",
            "<Location \"/svn/{}/qa/b/a\" >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method GET PROPFIND OPTIONS REPORT",
            "<RequireAny>",
            "Require expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*CmsUser(,[^,]+)*$/",
            "</RequireAny>",
            "</RequireAll>",
            "</Location>",
        ]
        # Generate the output as a list of lines removing the indentation and empty lines.
        output = [line.strip() for line in generate(input, repo).readlines() if line.strip() != '']
        self.assertListEqual(output, expected)

    def test_validate_paths(self):
        # A valid section with valid path and roles
        self.assertTrue(validate([
            "[/projectA/B_C-D.E F/0]",
            "* =",
            "@CmsAdmin = rw",
            "@CmsUserSuper = rw"
        ]))
        # A section with an invalid path
        self.assertFalse(validate([
            "[/projectÄ]",
            "* =",
            "@CmsAdmin = rw",
            "@CmsUserSuper = rw"
        ]))
        # A section with an invalid role
        self.assertFalse(validate([
            "[/projectA]",
            "* =",
            "@CmsÄdmin = rw",
            "@CmsUserSuper = rw"
        ]))


if __name__ == '__main__':
    unittest.main()
