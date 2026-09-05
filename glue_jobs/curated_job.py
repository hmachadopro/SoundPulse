"""Job de transformation staging (Parquet) → curated (modèle en étoile ajusté).
Couvre EF-04/EF-05 du CDC — modèle révisé le 2026-09-05 (ne pas se fier au
schéma du CDC brut) : fait_popularite, dim_titre, dim_artiste, dim_date.

Écrit en PySpark portable (argparse + SparkSession standard), pour la même
raison que staging_job.py : valider la logique en local avant de payer des
DPU Glue sur une transformation pas encore éprouvée. Voir le docstring de
staging_job.py pour le détail du compromis portabilité/Glue et de
l'aller-retour boto3 (`_s3_local_io.py`) qui contourne l'absence de connecteur
S3A en local.

Écarts connus par rapport au CDC d'origine (voir rapport de session) :
  - Pas de colonne `nb_streams_estimes` dans fait_popularite : Spotify n'a
    jamais exposé de compteur d'écoutes public via son API (comme aucune
    plateforme de streaming grand public ne le fait). Champ non réalisable,
    au même titre que les audio features du Prompt 2 — à documenter dans le
    wiki, pas à combler par une valeur inventée.
  - `score_popularite` (tracks) et `genres`/`nb_followers`/`popularite_globale`
    (artistes) sont nuls sur les données actuelles : le champ `popularity` des
    titres et `genres`/`followers`/`popularity` des artistes ont disparu des
    réponses de l'API Spotify pour cette app depuis nov. 2024 (confirmé en
    direct, indépendamment du endpoint ou du flow d'auth — voir rapport de
    session). Les colonnes existent quand même (schéma stable si Spotify
    réexpose ces champs un jour) ; le contrôle qualité de complétude sur
    `score_popularite` est donc assoupli en avertissement plutôt qu'en assert
    bloquant (voir run_quality_checks ci-dessous).

Usage :
    python glue_jobs/curated_job.py \
        --staging_path s3://bucket/staging \
        --curated_path s3://bucket/curated
"""
from __future__ import annotations

import argparse
import logging
import os
import tempfile

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from _s3_local_io import download_prefix, upload_dir

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Seuil de complétude en-dessous duquel on alerte (section 7 du CDC, version
# ajustée) — pour score_popularite, actuellement toujours sous ce seuil pour
# une raison connue (voir docstring de module), donc juste loggé, pas bloquant.
COMPLETUDE_MIN_RATIO = 0.98


def parse_args() -> argparse.Namespace:
    """Parse les arguments CLI (équivalent portable de getResolvedOptions)."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging_path", required=True, help="Préfixe S3 racine du staging")
    parser.add_argument("--curated_path", required=True, help="Préfixe S3 racine du curated")
    return parser.parse_args()


def _single_snapshot_date(titres: DataFrame) -> str:
    """Renvoie l'unique date_snapshot présente dans le staging.

    Ce job traite un snapshot à la fois (comme staging_job.py) : on s'assure
    qu'il n'y a bien qu'une seule date avant de construire le chemin de
    partition curated `date=...`, plutôt que d'en choisir une arbitrairement
    en cas d'anomalie amont.
    """
    dates = [row["date_snapshot"] for row in titres.select("date_snapshot").distinct().collect()]
    if len(dates) != 1:
        raise ValueError(f"Attendu une seule date_snapshot en staging, trouvé : {dates}")
    return dates[0]


def build_fait_popularite(titres: DataFrame) -> DataFrame:
    """Construit fait_popularite : une ligne par titre (pas par contribution
    d'artiste) — l'artiste retenu est l'artiste principal (`ids_artistes[0]`),
    pour que (id_titre, date_snapshot) reste unique (cf. contrôle qualité
    d'unicité). Les featurings additionnels ne sont pas représentés ici.
    """
    return titres.select(
        "date_snapshot",
        "id_titre",
        F.col("id_artiste_principal").alias("id_artiste"),
        F.col("popularite").alias("score_popularite"),
    )


def build_dim_titre(titres: DataFrame, artistes: DataFrame) -> DataFrame:
    """Construit dim_titre. Le `genre` n'existe pas nativement sur un titre
    Spotify (seuls les artistes ont des genres) : on le dérive du premier
    genre du premier artiste principal du titre — une approximation
    documentée, pas une donnée Spotify directe. `null` si l'artiste n'a aucun
    genre renseigné (cas systématique actuellement, voir docstring de module).
    """
    artiste_genres = artistes.select(
        F.col("id_artiste").alias("id_artiste_principal"),
        F.col("genres"),
    )
    return (
        titres.join(artiste_genres, on="id_artiste_principal", how="left")
        .select(
            "id_titre",
            F.col("nom"),
            F.col("duree_ms").alias("duree"),
            F.element_at(F.col("genres"), 1).alias("genre"),
        )
    )


def build_dim_artiste(artistes: DataFrame) -> DataFrame:
    """Construit dim_artiste par mapping direct depuis le staging."""
    return artistes.select(
        "id_artiste",
        F.col("nom"),
        F.col("genres"),
        F.col("nb_followers"),
        F.col("popularite").alias("popularite_globale"),
    )


def build_dim_date(fait_popularite: DataFrame) -> DataFrame:
    """Construit dim_date à partir des seules dates réellement observées
    dans fait_popularite — pas une table calendaire complète pré-générée
    (viendra si besoin en phase ultérieure).
    """
    dates = fait_popularite.select(F.to_date("date_snapshot").alias("date")).distinct()
    return dates.select(
        "date",
        F.dayofmonth("date").alias("jour"),
        F.weekofyear("date").alias("semaine"),
        F.month("date").alias("mois"),
        F.quarter("date").alias("trimestre"),
    )


def run_quality_checks(
    fait_popularite: DataFrame, dim_titre: DataFrame, dim_artiste: DataFrame
) -> None:
    """Garde-fous qualité minimaux (section 7 du CDC, version ajustée) —
    bloquants par défaut, sauf la complétude de score_popularite (voir
    docstring de module) qui reste un avertissement tant que l'API Spotify
    n'expose pas ce champ pour cette app.
    """
    total = fait_popularite.count()

    # Unicité : aucun doublon sur (id_titre, date_snapshot).
    doublons = (
        fait_popularite.groupBy("id_titre", "date_snapshot").count().filter("count > 1").count()
    )
    assert doublons == 0, f"Unicité violée : {doublons} doublons sur (id_titre, date_snapshot)"

    # Complétude : score_popularite non nul sur > 98% des lignes — assoupli en
    # avertissement, ce champ étant systématiquement absent de l'API pour
    # cette app (voir docstring de module), pas un défaut de la transformation.
    non_nuls = fait_popularite.filter(F.col("score_popularite").isNotNull()).count()
    ratio_completude = non_nuls / total if total else 1.0
    if ratio_completude < COMPLETUDE_MIN_RATIO:
        logger.warning(
            "Complétude score_popularite = %.1f%% (< %.0f%% attendu) — non bloquant : "
            "champ non exposé par l'API Spotify pour cette app depuis nov. 2024, "
            "à documenter dans le wiki plutôt qu'à corriger ici.",
            ratio_completude * 100,
            COMPLETUDE_MIN_RATIO * 100,
        )
    else:
        logger.info("Complétude score_popularite = %.1f%%", ratio_completude * 100)

    # Validité : score_popularite entre 0 et 100 (sur les valeurs non nulles
    # uniquement — un score absent n'est pas une valeur hors plage).
    hors_plage = fait_popularite.filter(
        F.col("score_popularite").isNotNull()
        & ((F.col("score_popularite") < 0) | (F.col("score_popularite") > 100))
    ).count()
    assert hors_plage == 0, f"Validité violée : {hors_plage} score_popularite hors [0, 100]"

    # Validité : durée > 0.
    duree_invalide = dim_titre.filter(F.col("duree").isNull() | (F.col("duree") <= 0)).count()
    assert duree_invalide == 0, f"Validité violée : {duree_invalide} titres avec durée <= 0 ou nulle"

    # Cohérence : tout id_artiste de fait_popularite existe dans dim_artiste.
    orphelins = (
        fait_popularite.select("id_artiste")
        .distinct()
        .join(dim_artiste.select("id_artiste"), on="id_artiste", how="left_anti")
        .count()
    )
    assert orphelins == 0, f"Cohérence violée : {orphelins} id_artiste absents de dim_artiste"


def transform(spark: SparkSession, staging_path: str, curated_path: str) -> None:
    """Lit les titres/artistes staging sous `staging_path` et écrit les 4
    tables du modèle en étoile sous `curated_path`/<table>/date=.../.

    Ne référence que des chemins reçus en argument (aucun accès direct à S3
    codé en dur) : que `staging_path`/`curated_path` soient des chemins
    locaux (run de test, voir main()) ou de vrais chemins s3:// (job Glue
    réel), cette fonction n'a pas à changer.
    """
    titres = spark.read.parquet(f"{staging_path}/titres")
    artistes = spark.read.parquet(f"{staging_path}/artistes")
    date_snapshot = _single_snapshot_date(titres)

    fait_popularite = build_fait_popularite(titres)
    dim_titre = build_dim_titre(titres, artistes)
    dim_artiste = build_dim_artiste(artistes)
    dim_date = build_dim_date(fait_popularite)

    run_quality_checks(fait_popularite, dim_titre, dim_artiste)

    tables = {
        "fait_popularite": fait_popularite,
        "dim_titre": dim_titre,
        "dim_artiste": dim_artiste,
        "dim_date": dim_date,
    }
    for name, df in tables.items():
        df.write.mode("overwrite").parquet(f"{curated_path}/{name}/date={date_snapshot}")
        logger.info("Curated écrit — %s : %d lignes", name, df.count())


def main() -> None:
    args = parse_args()

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_staging = os.path.join(tmp_dir, "staging")
        local_curated = os.path.join(tmp_dir, "curated")

        download_prefix(args.staging_path, local_staging)

        spark = SparkSession.builder.appName("soundpulse-curated").getOrCreate()
        try:
            transform(spark, local_staging, local_curated)
        finally:
            spark.stop()

        upload_dir(local_curated, args.curated_path)


if __name__ == "__main__":
    main()
