import json
import os
import tempfile
from pathlib import Path

import pytest

# import the underlying Python functions behind the @task decorators
from dags.noaa_storms_to_pmtiles import noaa_storms_to_pmtiles
from dags.snodas_to_pmtiles      import snodas_to_pmtiles

@pytest.fixture(autouse=True)
def clean_dirs(tmp_path, monkeypatch):
    # redirect AIRFLOW_HOME to a temp directory for isolation
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path / "airflow"))
    # ensure DAGs see the env var
    from importlib import reload
    import dags.noaa_storms_to_pmtiles, dags.snodas_to_pmtiles
    reload(dags.noaa_storms_to_pmtiles)
    reload(dags.snodas_to_pmtiles)
    yield

def test_vector_dag_writes_proof():
    dag = noaa_storms_to_pmtiles()
    # access the unwrapped task
    write_proof = dag.write_proof.__wrapped__
    # create a fake pmtiles file
    base = Path(os.environ["AIRFLOW_HOME"]) / "tiles"
    base.mkdir(parents=True)
    fake = base / "noaa_storms.pmtiles"
    fake.write_text("dummy")
    # run the proof task
    proof_path = write_proof(str(fake))
    assert os.path.exists(proof_path)
    data = json.loads(open(proof_path).read())
    assert data["dag"] == "noaa_storms_to_pmtiles"
    assert data["pmtiles"].endswith("noaa_storms.pmtiles")
    assert data["exists"] is True

def test_raster_dag_writes_proof():
    dag = snodas_to_pmtiles()
    write_proof = dag.write_raster_proof.__wrapped__
    base = Path(os.environ["AIRFLOW_HOME"]) / "tiles" / "raster"
    base.mkdir(parents=True, exist_ok=True)
    fake = base / "dummy_diff.pmtiles"
    fake.write_text("dummy")
    proof_path = write_proof(str(fake))
    assert os.path.exists(proof_path)
    data = json.loads(open(proof_path).read())
    assert data["dag"] == "snodas_to_pmtiles"
    assert data["pmtiles"].endswith("dummy_diff.pmtiles")
    assert data["exists"] is True
