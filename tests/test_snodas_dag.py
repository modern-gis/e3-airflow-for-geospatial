# tests/test_snodas_dag.py
import os
import pytest
from airflow.models import DagBag

# set env vars so the DAG can import without KeyError
os.environ["MODERN_GIS_S3_BUCKET"] = "modern-gis-test-bucket"
os.environ["AIRFLOW_HOME"] = "/tmp/airflow"

@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(include_examples=False)

def test_snodas_dag_loaded(dag_bag):
    dag = dag_bag.get_dag("snodas_to_pmtiles")
    assert dag is not None, "DAG 'snodas_to_pmtiles' failed to load"

def test_snodas_tasks(dag_bag):
    dag = dag_bag.get_dag("snodas_to_pmtiles")
    expected = {
        "fetch_snodas_dat",
        "convert_dat_to_geotiff",
        "compute_diff",
        "to_pmtiles",
        "upload_to_s3",
    }
    actual = {t.task_id for t in dag.tasks}
    assert actual == expected, f"Expected tasks {expected}, got {actual}"
