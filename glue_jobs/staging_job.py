"""Job AWS Glue (Spark) — raw (JSON) → staging (Parquet nettoyé/typé).

Couvre EF-03 du CDC : nettoyage, dédoublonnage, typage. Destiné à être déployé
comme job Glue (arguments --JOB_NAME, --raw_path, --staging_path fournis par
Terraform/Airflow) ; squelette à compléter en phase 3.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_path", "staging_path"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

raw_df = spark.read.json(args["raw_path"])

# TODO phase 3 : aplatir tracks/audio_features/artists, dédoublonner sur
# (id_titre, date_snapshot), typer les colonnes (tempo, énergie, dansabilité...),
# rejeter/quarantiner selon les règles de la section 7 du CDC.
staging_df = raw_df.withColumn("date_ingestion", F.current_date())

staging_df.write.mode("overwrite").parquet(args["staging_path"])

job.commit()
