"""Static checks of the Asset Bundle, so CI catches broken paths before `bundle validate`."""

from pathlib import Path

import yaml


def load_resources(root: Path) -> dict:
    merged: dict = {}
    for f in sorted((root / "resources").glob("*.yml")):
        for kind, items in (yaml.safe_load(f.read_text(encoding="utf-8")).get("resources") or {}).items():
            merged.setdefault(kind, {}).update(items)
    return merged


def test_bundle_has_the_three_targets(root):
    cfg = yaml.safe_load((root / "databricks.yml").read_text(encoding="utf-8"))
    assert set(cfg["targets"]) == {"dev", "test", "prod"}
    assert cfg["targets"]["dev"]["mode"] == "development"
    assert cfg["targets"]["prod"]["mode"] == "production"
    assert "docs/**" in cfg["sync"]["exclude"], "docs/ holds IDs and emails and must never be uploaded"


def test_job_dependencies_point_at_existing_tasks(root):
    job = load_resources(root)["jobs"]["shopsphere_daily"]
    keys = {t["task_key"] for t in job["tasks"]}
    assert len(keys) == len(job["tasks"]), "task keys are unique"
    for t in job["tasks"]:
        for dep in t.get("depends_on", []):
            assert dep["task_key"] in keys, f"{t['task_key']} depends on unknown {dep['task_key']}"


def test_notebook_and_pipeline_paths_exist(root):
    res = load_resources(root)
    paths = [t["notebook_task"]["notebook_path"] for t in res["jobs"]["shopsphere_daily"]["tasks"] if "notebook_task" in t]
    paths += [lib["notebook"]["path"] for lib in res["pipelines"]["medallion"]["libraries"]]
    paths.append(res["dashboards"]["executive"]["file_path"])
    for p in paths:
        assert (root / "resources" / p).resolve().exists(), p


def test_every_notebook_task_shares_the_single_job_cluster(root):
    job = load_resources(root)["jobs"]["shopsphere_daily"]
    assert len(job["job_clusters"]) == 1, "the 4-core regional quota fits one cluster"
    key = job["job_clusters"][0]["job_cluster_key"]
    assert all(t["job_cluster_key"] == key for t in job["tasks"] if "notebook_task" in t)


def test_no_account_emails_in_committed_config(root):
    for f in [root / "databricks.yml", *(root / "resources").glob("*.yml")]:
        assert "@gmail.com" not in f.read_text(encoding="utf-8"), f.name
