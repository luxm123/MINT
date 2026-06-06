from mint.controller import MintController
from mint.workloads import get_workload


def _config(tmp_path, baseline):
    return {
        "aws": {"lambda_functions": {f"f{i}": f"mint-f{i}" for i in range(1, 6)}},
        "experiment": {"baseline": baseline, "warmup_budget": 2, "output_dir": str(tmp_path / baseline)},
        "platform": {"default_retention_sec": 300, "default_cold_start_ms": 800, "default_warm_duration_ms": 100},
        "scheduler": {"enable_delay": True, "enable_cancel": True, "enable_replace": True, "gain_threshold": 0.0},
    }


def test_static_dag_and_mint_offline_obey_warmup_budget(tmp_path):
    for baseline in ("static_dag", "mint_offline"):
        controller = MintController(_config(tmp_path, baseline), dag=get_workload("chain"), baseline=baseline, dry_run=True)
        summary = controller.run(3)
        assert summary["total_warmup"] == 6
        assert summary["execute_count"] == 6
        assert summary["replace_count"] == 3


def test_unlimited_static_baseline_can_warm_all_nodes(tmp_path):
    controller = MintController(
        _config(tmp_path, "static_dag_unlimited"),
        dag=get_workload("chain"),
        baseline="static_dag_unlimited",
        dry_run=True,
    )
    summary = controller.run(3)
    assert summary["total_warmup"] == 9
    assert summary["replace_count"] == 0


def test_periodic_keepwarm_and_orion_like_obey_warmup_budget(tmp_path):
    for baseline in ("periodic_keepwarm", "orion_like"):
        controller = MintController(_config(tmp_path, baseline), dag=get_workload("chain"), baseline=baseline, dry_run=True)
        summary = controller.run(2)
        assert summary["total_warmup"] == 4
        assert summary["execute_count"] == 4
        assert summary["replace_count"] == 2
