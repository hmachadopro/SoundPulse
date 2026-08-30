"""Job AWS Glue (Spark) — staging (Parquet) → curated (modèle en étoile).

Couvre EF-04/EF-05 du CDC : construction de fait_popularite, dim_titre,
dim_artiste, dim_date, et calcul des indicateurs (popularité moyenne par genre,
croissance d'artiste, profils audio). Squelette à compléter en phase 3.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "staging_path", "curated_path"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

staging_df = spark.read.parquet(args["staging_path"])

# TODO phase 3 : construire les 4 tables du modèle en étoile (section 5.3 du CDC)
# et les écrire sous curated/<table>/ en Parquet partitionné par date_snapshot.

job.commit()
