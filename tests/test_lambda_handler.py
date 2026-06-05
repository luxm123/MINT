from lambda_function import handler as lambda_module


def test_lambda_handler_entrypoint_and_type_alias():
    lambda_module.IS_COLD_START = True
    result = lambda_module.lambda_handler({"function_name": "f1", "run_id": "r1", "type": "warmup"}, None)
    assert result["function_name"] == "f1"
    assert result["run_id"] == "r1"
    assert result["invocation_type"] == "warmup"
    assert result["cold_start"] is True
