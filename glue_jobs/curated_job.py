"""Job staging → curated (EF-04/EF-05 du CDC) : modélisation en étoile.

Écrit en PySpark **portable**, sans dépendance à `awsglue` — voir le
docstring de `staging_job.py` et de `_s3_local_io.py` pour le détail de ce
choix (compatibilité avec un vrai job AWS Glue en phase orchestration, sans
configurer le connecteur `s3a://` pour ce run local).

Schéma Deezer (pas l'ancien schéma Spotify de `archive/spotify-attempt`) :
- `fait_popularite` : `score_popularite` = `rank` Deezer brut (échelle propre
  à Deezer, non normalisée 0-100 — pas de `nb_streams_estimes`, aucune
  plateforme grand public n'expose de compteur d'écoutes réel).
- `dim_titre` : le `genre` est connu directement depuis l'extraction (id du
  genre de chart d'origine), pas dérivé par une heuristique fragile comme
  côté Spotify.
- `dim_artiste` : pas de colonne `genres`, Deezer n'expose pas de genre au
  niveau artiste (le genre vit uniquement dans `dim_titre`).

Les contrôles qualité (section 7 du CDC) sont ici des `assert` bloquants :
contrairement à la version Spotify, `rank` est normalement bien peuplé côté
Deezer — un échec de complétude doit stopper le job plutôt qu'être assoupli
silencieusement.

Usage (depuis la racine du repo, `src/` doit être sur le PYTHONPATH — déjà
le cas dans le conteneur Airflow via `PYTHONPATH=/opt/airflow/dags`, voir
`docker-compose.yml`) :
    PYTHONPATH=. python glue_jobs/curated_job.py --staging_path <chemin> --curated_path <chemin>
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.quality_checks import check_completude, check_coherence, check_unicite, check_validite

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staging_path", required=True, help="Répertoire racine de la zone staging")
    parser.add_argument("--curated_path", required=True, help="Répertoire racine de la zone curated")
    return parser.parse_args()


def build_dim_titre(titres_df: DataFrame) -> DataFrame:
    return titres_df.select(
        F.col("id").alias("id_titre"),
        F.col("title").alias("nom"),
        F.col("duration").alias("duree"),
        F.col("bpm"),
        F.col("gain"),
        F.col("genre_id").alias("genre"),
    )


def build_dim_artiste(artistes_df: DataFrame) -> DataFrame:
    return artistes_df.select(
        F.col("id").alias("id_artiste"),
        F.col("name").alias("nom"),
        F.col("nb_fan"),
        F.col("nb_album"),
    )


def build_fait_popularite(titres_df: DataFrame) -> DataFrame:
    return titres_df.select(
        F.col("date_snapshot"),
        F.col("id").alias("id_titre"),
        F.col("artist_id").alias("id_artiste"),
        F.col("rank").alias("score_popularite"),
    )


def build_dim_date(fait_df: DataFrame) -> DataFrame:
    """Génère une ligne de calendrier par date distincte présente dans le fait."""
    dates_df = fait_df.select(F.to_date("date_snapshot").alias("date")).distinct()
    return dates_df.select(
        F.col("date"),
        F.dayofmonth("date").alias("jour"),
        F.weekofyear("date").alias("semaine"),
        F.month("date").alias("mois"),
        F.quarter("date").alias("trimestre"),
    )


def run_quality_checks(fait_df: DataFrame, dim_titre_df: DataFrame, dim_artiste_df: DataFrame) -> None:
    """Contrôles qualité section 7 du CDC (version Deezer) — bloquants.

    Délègue les règles à `src/common/quality_checks.py` (fonctions pures
    pandas, partagées avec les tests unitaires) plutôt que de les
    dupliquer ici : `.toPandas()` matérialise les DataFrames Spark, ce qui
    reste acceptable au volume de ce projet portfolio (quelques milliers de
    lignes). Contrairement à la version Spotify, où `rank`/équivalent était
    souvent absent et avait imposé d'assouplir le seuil de complétude,
    Deezer peuple systématiquement `rank` sur les titres de chart : un
    échec ici doit être traité comme une régression réelle, pas comme un
    cas attendu.
    """
    fait_pdf = fait_df.toPandas()
    dim_titre_pdf = dim_titre_df.toPandas()
    dim_artiste_pdf = dim_artiste_df.toPandas()

    check_unicite(fait_pdf)
    completude = check_completude(fait_pdf)
    check_validite(fait_pdf, dim_titre_pdf)
    check_coherence(fait_pdf, dim_artiste_pdf)

    logger.info(
        "Contrôles qualité OK — %d lignes, complétude score_popularite %.2f%%",
        len(fait_pdf),
        completude * 100,
    )


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("soundpulse-curated").getOrCreate()

    titres_df = spark.read.parquet(f"{args.staging_path}/titres")
    artistes_df = spark.read.parquet(f"{args.staging_path}/artistes")

    dim_titre_df = build_dim_titre(titres_df)
    dim_artiste_df = build_dim_artiste(artistes_df)
    fait_df = build_fait_popularite(titres_df)
    dim_date_df = build_dim_date(fait_df)

    run_quality_checks(fait_df, dim_titre_df, dim_artiste_df)

    # Un run par date de snapshot : chaque table est écrite sous sa propre
    # partition `date=...`, sans écraser l'historique des runs précédents.
    snapshot_dates = [row["date_snapshot"] for row in fait_df.select("date_snapshot").distinct().collect()]
    if len(snapshot_dates) != 1:
        raise AssertionError(f"Un seul date_snapshot attendu par run, trouvé : {snapshot_dates}")
    snapshot_date = snapshot_dates[0]

    tables = {
        "fait_popularite": fait_df,
        "dim_titre": dim_titre_df,
        "dim_artiste": dim_artiste_df,
        "dim_date": dim_date_df,
    }
    for name, df in tables.items():
        path = f"{args.curated_path}/{name}/date={snapshot_date}"
        df.write.mode("overwrite").parquet(path)
        logger.info("%s écrit : %d lignes → %s", name, df.count(), path)

    spark.stop()


if __name__ == "__main__":
    main()
