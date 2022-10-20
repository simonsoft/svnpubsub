import unittest
from svnauthzsub import generate


class SvnAuthzSubTestCase(unittest.TestCase):

    def test_read_only(self):
        repo = "testrepo"
        input = [
            "[/qa/readonly]",
            "* =",
            "@CmsUser = r",
            "@CmsAdmin = r",
        ]
        expected = [
            "<Location /svn/{}/qa/readonly >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method OPTIONS",
            "</RequireAll>",
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
            "<Location /svn/{}/projectB >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method OPTIONS",
            "</RequireAll>",
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

    def test_permissions_mixed(self):
        repo = "testrepo"
        input = [
            "[/]",
            "@CmsAdmin = rw",
            "@CmsUserSuper = rw",
            "@CmsUser = r",
        ]
        expected = [
            "<Location /svn/{} >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method OPTIONS",
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
            "<Location /svn/{}/projectA >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method OPTIONS",
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
            "<Location /svn/{}/projectB >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method OPTIONS",
            "</RequireAll>",
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
            "[/qa/b/a]",
            "* =",
            "[/qa/b]",
            "* =",
            "[/qa/a]",
            "* =",
        ]
        expected = [
            "<Location /svn/{}/qa >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method OPTIONS",
            "</RequireAll>",
            "</Location>",
            "<Location /svn/{}/qa/a >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method OPTIONS",
            "</RequireAll>",
            "</Location>",
            "<Location /svn/{}/qa/b >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method OPTIONS",
            "</RequireAll>",
            "</Location>",
            "<Location /svn/{}/qa/b/a >".format(repo),
            "<RequireAll>",
            "Require valid-user",
            "Require method OPTIONS",
            "</RequireAll>",
            "</Location>",
        ]
        # Generate the output as a list of lines removing the indentation and empty lines.
        output = [line.strip() for line in generate(input, repo).readlines() if line.strip() != '']
        self.assertListEqual(output, expected)


if __name__ == '__main__':
    unittest.main()
