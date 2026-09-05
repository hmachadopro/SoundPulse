"""Job raw → staging (EF-03 du CDC) : aplatissement, dédoublonnage et typage.

Écrit en PySpark **portable**, sans dépendance à `awsglue` (bibliothèque
disponible uniquement dans l'environnement managé AWS Glue, non installable
en local) : arguments via `argparse`, `SparkSession` standard. Les chemins
`--raw_path`/`--staging_path` sont traités comme s'ils étaient des chemins S3
(le job ne sait pas s'il tourne en local ou sur un vrai cluster Glue) ; pour
ce prompt, le pont S3 ↔ disque local est assuré en amont/aval par
`_s3_local_io.py`, pas par ce module — voir son docstring pour le détail de
ce choix (éviter la configuration du connecteur `s3a://` en local).

Usage :
    python glue_jobs/staging_job.py --raw_path <chemin>/deezer_snapshot.json --staging_path <chemin>
"""
from __future__ import annotations

import argparse
import logging
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schéma explicite plutôt qu'une inférence Spark : `rank` dépasse largement
# l'amplitude d'un int32 (valeurs constatées ~1e6 sur le snapshot réel), et
# bpm/gain sont fréquemment absents (null) sur Deezer — les figer évite qu'une
# inférence sur un échantillon partiel ne se trompe de type.
_TRACK_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("title", T.StringType()),
        T.StructField("duration", T.IntegerType()),
        T.StructField("rank", T.LongType()),
        T.StructField("bpm", T.DoubleType()),
        T.StructField("gain", T.DoubleType()),
        T.StructField("explicit_lyrics", T.BooleanType()),
        T.StructField("artist_id", T.LongType()),
        T.StructField("genre_id", T.LongType()),
    ]
)

_ARTIST_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("name", T.StringType()),
        T.StructField("nb_fan", T.LongType()),
        T.StructField("nb_album", T.LongType()),
    ]
)

_RAW_SCHEMA = T.StructType(
    [
        T.StructField("tracks", T.ArrayType(_TRACK_SCHEMA)),
        T.StructField("artists", T.ArrayType(_ARTIST_SCHEMA)),
    ]
)

# Partition Hive `date=YYYY-MM-DD` du chemin raw : la date de snapshot doit
# venir de là (date réelle de collecte), pas de la date d'exécution du job,
# qui peut différer en cas de rattrapage/relance.
_DATE_PARTITION_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw_path", required=True, help="Chemin du snapshot JSON raw")
    parser.add_argument("--staging_path", required=True, help="Répertoire racine de la zone staging")
    return parser.parse_args()


def extract_snapshot_date(raw_path: str) -> str:
    """Extrait la date `YYYY-MM-DD` de la partition Hive présente dans le chemin raw."""
    match = _DATE_PARTITION_RE.search(raw_path)
    if not match:
        raise ValueError(f"Impossible d'extraire la partition date= du chemin raw : {raw_path}")
    return match.group(1)


def build_titres(spark: SparkSession, raw_path: str, snapshot_date: str):
    """Aplatit `tracks`, déduplique sur `id` et type les colonnes."""
    raw_df = spark.read.schema(_RAW_SCHEMA).option("multiLine", True).json(raw_path)
    tracks_df = raw_df.select(F.explode("tracks").alias("t")).select("t.*")

    # Un même titre peut apparaître au chart de plusieurs genres : on ne
    # garde qu'une occurrence par id (le genre d'origine conservé est alors
    # arbitraire parmi ceux où il était classé — acceptable pour ce run de
    # test, une règle de choix explicite pourra être ajoutée si besoin).
    return tracks_df.dropDuplicates(["id"]).withColumn("date_snapshot", F.lit(snapshot_date))


def build_artistes(spark: SparkSession, raw_path: str):
    """Aplatit `artists` et déduplique sur `id` (un artiste peut couvrir plusieurs genres)."""
    raw_df = spark.read.schema(_RAW_SCHEMA).option("multiLine", True).json(raw_path)
    artists_df = raw_df.select(F.explode("artists").alias("a")).select("a.*")
    return artists_df.dropDuplicates(["id"])


def main() -> None:
    args = parse_args()
    snapshot_date = extract_snapshot_date(args.raw_path)

    spark = SparkSession.builder.appName("soundpulse-staging").getOrCreate()

    titres_df = build_titres(spark, args.raw_path, snapshot_date)
    artistes_df = build_artistes(spark, args.raw_path)

    titres_count = titres_df.count()
    artistes_count = artistes_df.count()

    titres_df.write.mode("overwrite").parquet(f"{args.staging_path}/titres")
    artistes_df.write.mode("overwrite").parquet(f"{args.staging_path}/artistes")

    logger.info(
        "Staging écrit pour date_snapshot=%s : %d titres, %d artistes → %s",
        snapshot_date,
        titres_count,
        artistes_count,
        args.staging_path,
    )

    spark.stop()


if __name__ == "__main__":
    main()
