# tests/test_proofs.py
import json
import os
import time
from pathlib import Path
import shutil

import pytest

@pytest.fixture(autouse=True)

def test_vector_proof():
    proof_dir =  "airflow/proof"
    f = proof_dir / "noaa_proof.json"
    assert f.exists(), f"Missing proof file: {f}"
    data = json.loads(f.read_text())
    assert data["dag"] == "noaa_storms_to_pmtiles"
    assert data["exists"] is True
    # allow absolute or relative, just check the filename
    assert data["pmtiles"].endswith("noaa_storms.pmtiles")


def test_raster_proof():
    proof_dir = "airflow/proof"
    f = proof_dir / "snodas_proof.json"
    assert f.exists(), f"Missing proof file: {f}"
    data = json.loads(f.read_text())
    assert data["dag"] == "snodas_to_pmtiles"
    assert data["exists"] is True
    assert data["pmtiles"].endswith("dummy_diff.pmtiles")


def test_print_badge():
    # this line will register the badge in GitHub Actions logs
    code = f"brick_e3_automation_{int(time.time())}"
    print(f"::notice title=Badge Unlocked::{code}")
