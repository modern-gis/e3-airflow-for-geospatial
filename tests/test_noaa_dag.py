import os
import pytest
from airflow.models import DagBag

# ensure env var for DAG import
os.environ["MODERN_GIS_S3_BUCKET"] = "modern-gis-test-bucket"
os.environ["AIRFLOW_HOME"] = "/tmp/airflow"

@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(include_examples=False)

def test_dag_and_tasks_exist(dag_bag):
    dag = dag_bag.get_dag("noaa_storms_to_pmtiles")
    assert dag is not None, "DAG 'noaa_storms_to_pmtiles' failed to load"
    expected = {"fetch_shapefile", "to_geoparquet", "to_pmtiles", "upload"}
    assert {t.task_id for t in dag.tasks} == expected