from airflow.models import DagBag

def test_snodas_dag_loads():
    dagbag = DagBag()
    assert "snodas_to_pmtiles" in dagbag.dags
    dag = dagbag.get_dag("snodas_to_pmtiles")
    assert dag is not None
    assert len(dag.tasks) >= 5  # fetch, convert, diff, tile, upload

def test_task_ids():
    dagbag = DagBag()
    dag = dagbag.get_dag("snodas_to_pmtiles")
    expected_tasks = {
        "fetch_today_snodas_dat",
        "fetch_yesterday_snodas_dat",
        "convert_dat_to_geotiff",
        "compute_difference",
        "generate_pmtiles",
        "upload_to_s3",
    }
    actual = set(task.task_id for task in dag.tasks)
    assert expected_tasks.issubset(actual)
