# tests/test_proofs.py
import json
import os
import time
from pathlib import Path
import shutil

import pytest

PROOF_DIR = Path(os.environ.get("AIRFLOW_HOME", "/workspace/airflow")) / "proof"


@pytest.fixture(autouse=True)
def ensure_proof_dir(tmp_path, monkeypatch):
    # Redirect AIRFLOW_HOME to a temp workspace so tests are hermetic
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path / "airflow"))
    # Copy in your checked-in proof files
    src = Path.cwd() / "airflow" / "proof"
    dst = Path(os.environ["AIRFLOW_HOME"]) / "proof"
    if src.exists():
        shutil.copytree(src, dst)
    yield


def test_vector_proof():
    f = PROOF_DIR / "noaa_proof.json"
    assert f.exists(), f"Missing proof file: {f}"
    data = json.loads(f.read_text())
    assert data["dag"] == "noaa_storms_to_pmtiles"
    assert data["exists"] is True
    # allow absolute or relative, just check the filename
    assert data["pmtiles"].endswith("noaa_storms.pmtiles")


def test_raster_proof():
    f = PROOF_DIR / "snodas_proof.json"
    assert f.exists(), f"Missing proof file: {f}"
    data = json.loads(f.read_text())
    assert data["dag"] == "snodas_to_pmtiles"
    assert data["exists"] is True
    assert data["pmtiles"].endswith("dummy_diff.pmtiles")


def test_print_badge():
    # this line will register the badge in GitHub Actions logs
    code = f"brick_e3_automation_{int(time.time())}"
    print(f"::notice title=Badge Unlocked::{code}")
