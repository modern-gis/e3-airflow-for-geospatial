from airflow.models import DagBag

def test_noaa_dag_loads():
    dagbag = DagBag()
    assert "noaa_storms_to_pmtiles" in dagbag.dags
    dag = dagbag.get_dag("noaa_storms_to_pmtiles")
    assert dag is not None
    assert len(dag.tasks) >= 4  # fetch, convert, tile, upload

def test_task_ids():
    dagbag = DagBag()
    dag = dagbag.get_dag("noaa_storms_to_pmtiles")
    expected_tasks = {
        "fetch_shapefile",
        "to_geoparquet",
        "to_pmtiles",
        "upload",
    }
    actual = set(task.task_id for task in dag.tasks)
    assert expected_tasks.issubset(actual)
