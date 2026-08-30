"""DAG principal SoundPulse — orchestration quotidienne du pipeline (ENF-01, ENF-04).

extract_spotify → glue_staging → glue_curated → data_quality_checks → notify_on_failure
"""
from __future__ import annotations

import datetime as dt

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    "owner": "soundpulse",
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
}


@dag(
    dag_id="soundpulse_pipeline",
    schedule="0 6 * * *",  # quotidien, dashboard prêt avant 8h (objectif §2.2 du CDC)
    start_date=dt.datetime(2026, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["soundpulse", "aws"],
)
def soundpulse_pipeline():
    @task
    def extract_spotify():
        from src.extract.spotify_extractor import main

        main()

    glue_staging = GlueJobOperator(
        task_id="glue_staging",
        job_name="soundpulse-staging-job",
    )

    glue_curated = GlueJobOperator(
        task_id="glue_curated",
        job_name="soundpulse-curated-job",
    )

    @task
    def data_quality_checks():
        # TODO phase 5 : lancer les contrôles Great Expectations (section 7 du CDC),
        # bloquer la suite si un seuil critique (complétude, unicité) est franchi.
        raise NotImplementedError

    extract_spotify() >> glue_staging >> glue_curated >> data_quality_checks()


soundpulse_pipeline()
