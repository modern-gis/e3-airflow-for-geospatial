# tests/test_proofs.py

import json
from pathlib import Path
import time

def test_vector_proof_exists_and_valid():
    proof_file = Path("airflow/proof") / "noaa_proof.json"
    assert proof_file.exists(), f"Missing proof file: {proof_file}"
    data = json.loads(proof_file.read_text())
    assert data["dag"] == "noaa_storms_to_pmtiles"
    assert data["exists"] is True
    assert data["pmtiles"].endswith("noaa_storms.pmtiles")

def test_raster_proof_exists_and_valid():
    proof_file = Path("airflow/proof") / "snodas_proof.json"
    assert proof_file.exists(), f"Missing proof file: {proof_file}"
    data = json.loads(proof_file.read_text())
    assert data["dag"] == "snodas_to_pmtiles"
    assert data["exists"] is True
    assert data["pmtiles"].endswith("diff.pmtiles")

def test_print_badge():
    # This line will show up in GitHub Actions logs as the badge proof.
    code = f"brick_e3_automation_{int(time.time())}"
    print(f"::notice title=Badge Unlocked::{code}")
