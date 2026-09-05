"""DAG principal SoundPulse — orchestration quotidienne du pipeline (ENF-01, ENF-04).

extract_deezer → staging_job → curated_job → check_freshness

Ne dépend d'aucune ressource AWS Glue Job réelle (ENF-05 : coût DPU différé
tant que la logique n'est pas validée en dehors de Glue). `staging_job` et
`curated_job` exécutent directement les scripts PySpark portables du
Prompt 2 (`glue_jobs/staging_job.py`/`curated_job.py`, argparse +
SparkSession standard, pas `awsglue`) dans le conteneur Airflow, qui
embarque désormais un JRE et PySpark (voir `Dockerfile`) — d'où l'usage de
`BashOperator` plutôt que l'ancien (et obsolète) `GlueJobOperator`.
"""
from __future__ import annotations

import datetime as dt
import logging

import boto3
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator

from src.common.config import AWS_REGION, S3_BUCKET_NAME

logger = logging.getLogger(__name__)


def _log_task_failure(context: dict) -> None:
    """Notifie l'échec d'une tâche dans les logs Airflow/UI (EF-08/ENF-03).

    Aucun canal de notification externe (email/Slack) n'est branché ici :
    ce projet portfolio n'a ni credential ni canal configuré pour ça, et en
    simuler un serait une promesse de fonctionnalité non tenue. Cette
    fonction structure l'échec dans les logs pour qu'il soit repérable
    immédiatement dans l'UI Airflow ; un vrai canal (email/Slack) reste à
    ajouter dans un prompt futur, si demandé.
    """
    ti = context["task_instance"]
    logger.error(
        "ÉCHEC PIPELINE SOUNDPULSE — dag=%s task=%s run_id=%s tentative=%d/%d exception=%s",
        ti.dag_id,
        ti.task_id,
        context["run_id"],
        ti.try_number,
        ti.max_tries + 1,
        context.get("exception"),
    )


default_args = {
    "owner": "soundpulse",
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
    "on_failure_callback": _log_task_failure,
}


@dag(
    dag_id="soundpulse_pipeline",
    schedule="0 6 * * *",  # quotidien, dashboard prêt avant 8h (objectif §2.2 du CDC)
    start_date=dt.datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,  # deux extractions Deezer concurrentes accélèrent le rate limiting (voir deezer_extractor._get_json)
    default_args=default_args,
    tags=["soundpulse", "aws"],
)
def soundpulse_pipeline():
    @task
    def extract_deezer() -> None:
        # Import direct plutôt que BashOperator : l'extracteur expose déjà
        # une fonction `main()` réutilisable, pas besoin de repasser par un
        # sous-processus juste pour l'invoquer depuis Airflow.
        from src.extract.deezer_extractor import main

        main()

    # `WORKDIR` isolé par date d'exécution (`{{ ds }}`, pas la date du jour) :
    # évite qu'un retry ou une relance manuelle sur une autre date ne mélange
    # ses fichiers locaux avec ceux d'un autre run. Chaque tâche retélécharge
    # ses entrées depuis S3 plutôt que de réutiliser les fichiers locaux
    # laissés par la tâche précédente : ça reste correct même si les deux
    # tâches finissent par tourner sur des workers différents.
    staging_job = BashOperator(
        task_id="staging_job",
        # BashOperator exécute la commande dans un répertoire temporaire
        # qu'il crée lui-même, pas dans le `working_dir` du conteneur : il
        # faut donc lui fixer explicitement le cwd pour que les chemins
        # relatifs (`glue_jobs/...`) résolvent.
        cwd="/opt/airflow/dags",
        bash_command="""
set -euo pipefail
WORKDIR="/tmp/soundpulse/{{ ds }}"
rm -rf "$WORKDIR/raw" "$WORKDIR/staging"
mkdir -p "$WORKDIR/raw" "$WORKDIR/staging"
python -m glue_jobs._s3_local_io download \
    --bucket "$S3_BUCKET_NAME" --prefix "raw/date={{ ds }}" \
    --local_dir "$WORKDIR/raw/date={{ ds }}"
python glue_jobs/staging_job.py \
    --raw_path "$WORKDIR/raw/date={{ ds }}/deezer_snapshot.json" \
    --staging_path "$WORKDIR/staging"
python -m glue_jobs._s3_local_io upload \
    --bucket "$S3_BUCKET_NAME" --prefix "staging" --local_dir "$WORKDIR/staging"
""",
    )

    curated_job = BashOperator(
        task_id="curated_job",
        cwd="/opt/airflow/dags",
        bash_command="""
set -euo pipefail
WORKDIR="/tmp/soundpulse/{{ ds }}"
rm -rf "$WORKDIR/staging" "$WORKDIR/curated"
mkdir -p "$WORKDIR/staging" "$WORKDIR/curated"
python -m glue_jobs._s3_local_io download \
    --bucket "$S3_BUCKET_NAME" --prefix "staging" --local_dir "$WORKDIR/staging"
python glue_jobs/curated_job.py \
    --staging_path "$WORKDIR/staging" --curated_path "$WORKDIR/curated"
python -m glue_jobs._s3_local_io upload \
    --bucket "$S3_BUCKET_NAME" --prefix "curated" --local_dir "$WORKDIR/curated"
""",
    )

    @task
    def check_freshness(ds: str | None = None) -> None:
        """Vérifie la fraîcheur (section 7 du CDC) : la partition curated du jour existe.

        `curated_job.py` ne connaît que les données qu'il vient d'écrire, pas
        la notion de "jour d'exécution Airflow" : cette vérification de bout
        en bout a donc sa place dans le DAG plutôt que dans le job lui-même.
        """
        prefix = f"curated/fait_popularite/date={ds}/"
        s3 = boto3.client("s3", region_name=AWS_REGION)
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix, MaxKeys=1)
        if response.get("KeyCount", 0) == 0:
            raise AirflowException(
                f"Fraîcheur KO : aucune partition trouvée sous s3://{S3_BUCKET_NAME}/{prefix}"
            )
        logger.info("Fraîcheur OK : s3://%s/%s présente", S3_BUCKET_NAME, prefix)

    extract_deezer() >> staging_job >> curated_job >> check_freshness()


soundpulse_pipeline()
