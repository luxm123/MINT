from scripts.run_controlled_cold_pilot import reset_function_pool, validate_controlled_cold_run


class FakeWaiter:
    def wait(self, **kwargs):
        return None


class FakeLambdaClient:
    def __init__(self):
        self.variables = {}
        self.updates = []

    def get_function_configuration(self, FunctionName):
        return {
            "Environment": {"Variables": dict(self.variables.get(FunctionName, {}))},
            "LastUpdateStatus": "Successful",
        }

    def update_function_configuration(self, FunctionName, Environment):
        self.variables[FunctionName] = dict(Environment["Variables"])
        self.updates.append(FunctionName)

    def get_waiter(self, name):
        assert name == "function_updated_v2"
        return FakeWaiter()


def _event(logical, invocation_type, cold, env):
    return {
        "event_type": "warmup" if invocation_type == "warmup" else "invocation",
        "logical_name": logical,
        "invocation_type": invocation_type,
        "cold_start": cold,
        "execution_environment_id": env,
        "status": "ok",
    }


def test_reset_function_pool_preserves_environment_and_adds_unique_token():
    client = FakeLambdaClient()
    client.variables["pool-f1"] = {"EXISTING": "value"}
    rows = reset_function_pool(
        client,
        {"f1": "pool-f1", "f2": "pool-f2"},
        ["f1", "f2"],
        "token-1",
        dry_run=False,
    )

    assert client.updates == ["pool-f1", "pool-f2"]
    assert client.variables["pool-f1"] == {"EXISTING": "value", "MINT_COLD_RESET_TOKEN": "token-1"}
    assert client.variables["pool-f2"] == {"MINT_COLD_RESET_TOKEN": "token-1"}
    assert all(row["last_update_status"] == "Successful" for row in rows)


def test_validate_controlled_cold_accepts_cold_warmup_then_hot_real_same_environment():
    events = [
        _event("f6", "warmup", True, "env-new-6"),
        _event("f6", "real", False, "env-new-6"),
        _event("f1", "real", True, "env-new-1"),
    ]

    valid, reason, observed = validate_controlled_cold_run(
        events,
        {"f6": "env-old-6", "f1": "env-old-1"},
    )

    assert valid is True
    assert reason == "valid"
    assert observed == {"f6": "env-new-6", "f1": "env-new-1"}


def test_validate_controlled_cold_rejects_reused_or_mismatched_environment():
    reused = [_event("f1", "real", True, "env-old")]
    valid, reason, _ = validate_controlled_cold_run(reused, {"f1": "env-old"})
    assert valid is False
    assert reason == "environment_not_replaced:f1"

    mismatch = [
        _event("f6", "warmup", True, "env-warmup"),
        _event("f6", "real", False, "env-real"),
    ]
    valid, reason, _ = validate_controlled_cold_run(mismatch, {})
    assert valid is False
    assert reason == "warmup_real_environment_mismatch:f6"
