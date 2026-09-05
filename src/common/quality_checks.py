"""Contrôles qualité du modèle en étoile curated (section 7 du CDC, schéma Deezer).

Fonctions pures sur des DataFrames **pandas**, pas Spark : elles sont
appelées à la fois par `glue_jobs/curated_job.py` (garde-fou bloquant à
chaque run, via `.toPandas()` — le volume de ce projet portfolio le permet)
et par `tests/test_data_quality.py` (tests unitaires sur de petits
DataFrames construits à la main, sans démarrer de SparkSession). Une seule
source de vérité pour les règles, exécutée dans les deux contextes.

Le CDC (§7) cite Great Expectations comme modèle d'inspiration pour ces
contrôles, pas comme obligation ("sur le modèle de ce que permet un outil
comme..."). L'intégrer proprement (contexte GE, suites, checkpoints)
serait disproportionné pour ce volume de données et cette taille de
projet : de simples fonctions `assert` restent cohérentes avec le reste du
pipeline (déjà en `assert` bloquant côté `curated_job.py`) et sont
directement testables avec `pytest`, sans dépendance supplémentaire.
"""
from __future__ import annotations

import pandas as pd


def check_unicite(fait_popularite: pd.DataFrame) -> None:
    """Vérifie l'absence de doublon sur (id_titre, date_snapshot).

    Raises:
        AssertionError: si au moins un doublon est détecté.
    """
    doublons = fait_popularite.duplicated(subset=["id_titre", "date_snapshot"]).sum()
    assert doublons == 0, f"{doublons} doublon(s) détecté(s) sur (id_titre, date_snapshot)"


def check_completude(fait_popularite: pd.DataFrame, seuil: float = 0.98) -> float:
    """Vérifie que `score_popularite` est peuplé sur plus de `seuil` des lignes.

    Returns:
        Le taux de complétude observé.

    Raises:
        AssertionError: si le taux de complétude est sous le seuil, ou si
            `fait_popularite` est vide (rien à valider).
    """
    total = len(fait_popularite)
    assert total > 0, "fait_popularite est vide — rien à valider"

    completude = fait_popularite["score_popularite"].notna().mean()
    assert completude > seuil, f"Complétude score_popularite insuffisante : {completude:.2%} (seuil {seuil:.0%})"
    return completude


def check_validite(fait_popularite: pd.DataFrame, dim_titre: pd.DataFrame) -> None:
    """Vérifie les bornes de valeur : score ≥ 0, durée > 0, bpm dans [0, 300] si renseigné.

    Raises:
        AssertionError: si une valeur hors plage est détectée.
    """
    scores_invalides = (fait_popularite["score_popularite"].notna() & (fait_popularite["score_popularite"] < 0)).sum()
    assert scores_invalides == 0, f"{scores_invalides} score_popularite négatif(s)"

    durees_invalides = (dim_titre["duree"] <= 0).sum()
    assert durees_invalides == 0, f"{durees_invalides} durée(s) invalide(s) (<= 0)"

    bpm_invalides = (dim_titre["bpm"].notna() & ((dim_titre["bpm"] < 0) | (dim_titre["bpm"] > 300))).sum()
    assert bpm_invalides == 0, f"{bpm_invalides} bpm hors plage [0, 300]"


def check_coherence(fait_popularite: pd.DataFrame, dim_artiste: pd.DataFrame) -> None:
    """Vérifie que tout `id_artiste` de `fait_popularite` existe dans `dim_artiste`.

    Raises:
        AssertionError: si au moins un `id_artiste` est orphelin.
    """
    artistes_orphelins = ~fait_popularite["id_artiste"].isin(dim_artiste["id_artiste"])
    nb_orphelins = artistes_orphelins.sum()
    assert nb_orphelins == 0, f"{nb_orphelins} id_artiste de fait_popularite absent(s) de dim_artiste"


def check_fraicheur(fait_popularite: pd.DataFrame, date_attendue: str) -> None:
    """Vérifie qu'au moins une ligne porte `date_snapshot == date_attendue`.

    Contrairement aux autres contrôles, la fraîcheur n'a de sens que par
    rapport à une date d'exécution externe (le job Spark, lui, ne connaît
    que les données qu'il vient d'écrire) — c'est pour ça qu'elle est
    vérifiée séparément dans le DAG Airflow (`check_freshness`, Prompt 3)
    plutôt que dans `curated_job.py`. Exposée ici pour être testable au
    même titre que les autres règles.

    Raises:
        AssertionError: si aucune ligne ne correspond à `date_attendue`.
    """
    present = (fait_popularite["date_snapshot"] == date_attendue).any()
    assert present, f"Fraîcheur KO : aucune ligne avec date_snapshot={date_attendue}"
