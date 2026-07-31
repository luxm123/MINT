from scripts.audit_cloudwatch_truth import _log_group_name


def test_log_group_name_ignores_version_or_alias_qualifier():
    assert _log_group_name("mint-nowarm-f1:12") == "/aws/lambda/mint-nowarm-f1"
    assert (
        _log_group_name("arn:aws:lambda:us-east-1:123456789012:function:mint-full-f7:4")
        == "/aws/lambda/mint-full-f7"
    )
