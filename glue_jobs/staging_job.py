"""Job de transformation raw (JSON) → staging (Parquet nettoyé/typé). Couvre
EF-03 du CDC : aplatissement, dédoublonnage, typage des titres et artistes.

Écrit en PySpark portable (argparse + SparkSession standard) plutôt qu'en job
AWS Glue (GlueContext/getResolvedOptions) : au stade actuel, la logique de
transformation n'est pas encore validée et les DPU Glue sont facturés à
l'usage (ENF-05 du CDC, budget < 10 €/mois) — on évite ce coût tant qu'on
teste en local. `transform()` ci-dessous ne fait que lire/écrire des chemins
passés en argument (comportement identique à un vrai job Glue) ; seule
l'orchestration dans `main()` diffère : elle rapatrie le JSON raw depuis S3
vers un répertoire local via boto3 avant Spark, et renvoie les Parquot
produits vers S3 après (voir _s3_local_io.py — Spark local n'a pas le
connecteur S3A configuré). Quand l'orchestration Airflow déploiera un vrai
job Glue, seule cette couche d'orchestration sera à remplacer par
GlueContext/getResolvedOptions ; `transform()` restera inchangé.

Usage :
    python glue_jobs/staging_job.py \
        --raw_path s3://bucket/raw/date=2026-09-05/spotify_snapshot.json \
        --staging_path s3://bucket/staging
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import tempfile

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from _s3_local_io import download_object, upload_dir

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SNAPSHOT_DATE_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})")


def parse_args() -> argparse.Namespace:
    """Parse les arguments CLI (équivalent portable de getResolvedOptions)."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw_path", required=True, help="Chemin S3 du snapshot JSON raw")
    parser.add_argument("--staging_path", required=True, help="Préfixe S3 racine du staging")
    return parser.parse_args()


def _extract_snapshot_date(raw_path: str) -> str:
    """Dérive la date de snapshot depuis la partition `date=YYYY-MM-DD` du
    chemin raw — pas la date du jour d'exécution, pour que la valeur soit
    correcte même en cas de rattrapage (rejeu d'un job en retard sur un
    snapshot d'une date passée).
    """
    match = SNAPSHOT_DATE_RE.search(raw_path)
    if not match:
        raise ValueError(f"Impossible d'extraire date=YYYY-MM-DD de {raw_path}")
    return match.group(1)


def _ensure_column(df: DataFrame, name: str, dtype: T.DataType) -> DataFrame:
    """Ajoute `name` en colonne null typée si absente du DataFrame.

    Certains champs Spotify (popularity, followers, genres) sont absents des
    réponses API pour cette app depuis les restrictions de nov. 2024 (voir
    rapport Prompt 2/3) : Spark, en inférant le schéma JSON, n'ouvre alors pas
    la colonne du tout. On la recrée à null pour garder un schéma stable dans
    le temps, y compris le jour où Spotify réexposerait ces champs.
    """
    if name in df.columns:
        return df
    return df.withColumn(name, F.lit(None).cast(dtype))


def _flatten_titres(raw_df: DataFrame, date_snapshot: str) -> DataFrame:
    """Aplatit `tracks` en un DataFrame de titres nettoyé et dédoublonné."""
    tracks = raw_df.select(F.explode("tracks").alias("t")).select("t.*")
    tracks = _ensure_column(tracks, "popularity", T.IntegerType())

    titres = tracks.select(
        F.col("id").alias("id_titre"),
        F.col("name").alias("nom"),
        F.col("duration_ms").cast(T.IntegerType()).alias("duree_ms"),
        F.col("popularity").cast(T.IntegerType()).alias("popularite"),
        F.col("explicit").cast(T.BooleanType()),
        F.col("artists")[0]["id"].alias("id_artiste_principal"),
        F.transform("artists", lambda a: a["id"]).alias("ids_artistes"),
        F.lit(date_snapshot).alias("date_snapshot"),
    )
    # Dédoublonnage sur id_titre : les 12 genres échantillonnés en recherche
    # côté extraction (search()) se recoupent (un même titre peut ressortir
    # pour plusieurs tags de genre).
    return titres.dropDuplicates(["id_titre"])


def _flatten_artistes(raw_df: DataFrame, date_snapshot: str) -> DataFrame:
    """Aplatit `artists` en un DataFrame d'artistes nettoyé et dédoublonné."""
    artists = raw_df.select(F.explode("artists").alias("a")).select("a.*")
    artists = _ensure_column(artists, "genres", T.ArrayType(T.StringType()))
    artists = _ensure_column(artists, "popularity", T.IntegerType())
    if "followers" in artists.columns:
        followers_total = F.col("followers")["total"]
    else:
        followers_total = F.lit(None)

    artistes = artists.select(
        F.col("id").alias("id_artiste"),
        F.col("name").alias("nom"),
        F.col("genres"),
        followers_total.cast(T.IntegerType()).alias("nb_followers"),
        F.col("popularity").cast(T.IntegerType()).alias("popularite"),
        F.lit(date_snapshot).alias("date_snapshot"),
    )
    return artistes.dropDuplicates(["id_artiste"])


def transform(spark: SparkSession, raw_path: str, staging_path: str, date_snapshot: str) -> None:
    """Lit le snapshot JSON à `raw_path` et écrit titres/artistes sous
    `staging_path`/titres et `staging_path`/artistes.

    Ne référence que des chemins reçus en argument (aucun accès direct à S3
    codé en dur) : que `raw_path`/`staging_path` soient des chemins locaux
    (run de test, voir main()) ou de vrais chemins s3:// (job Glue réel),
    cette fonction n'a pas à changer.
    """
    raw_df = spark.read.option("multiLine", "true").json(raw_path)

    titres = _flatten_titres(raw_df, date_snapshot)
    artistes = _flatten_artistes(raw_df, date_snapshot)

    titres.write.mode("overwrite").parquet(f"{staging_path}/titres")
    artistes.write.mode("overwrite").parquet(f"{staging_path}/artistes")

    logger.info(
        "Staging écrit — %d titres uniques, %d artistes uniques (date_snapshot=%s)",
        titres.count(),
        artistes.count(),
        date_snapshot,
    )


def main() -> None:
    args = parse_args()
    date_snapshot = _extract_snapshot_date(args.raw_path)

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_raw = os.path.join(tmp_dir, "raw_snapshot.json")
        local_staging = os.path.join(tmp_dir, "staging")

        download_object(args.raw_path, local_raw)

        spark = SparkSession.builder.appName("soundpulse-staging").getOrCreate()
        try:
            transform(spark, local_raw, local_staging, date_snapshot)
        finally:
            spark.stop()

        upload_dir(os.path.join(local_staging, "titres"), f"{args.staging_path}/titres")
        upload_dir(os.path.join(local_staging, "artistes"), f"{args.staging_path}/artistes")


if __name__ == "__main__":
    main()
