"""Tests de qualité des données — section 7 du CDC (schéma Deezer).

Deux familles, séparées par le marqueur `integration` (voir `pytest.ini`) :
- **Tests unitaires** (par défaut, rapides, sans réseau) : petits
  DataFrames pandas construits à la main, couvrant le cas nominal et le cas
  d'échec de chaque fonction de `src/common/quality_checks.py`.
- **Tests d'intégration** (`@pytest.mark.integration`, exclus du run par
  défaut) : téléchargent le vrai export curated depuis S3 et appliquent les
  mêmes règles aux données réelles. Nécessitent des credentials AWS locaux
  — jamais exécutés en CI (voir `.github/workflows/ci.yml`, qui n'en a pas).
"""
from __future__ import annotations

import io

import boto3
import pandas as pd
import pytest

from src.common.quality_checks import (
    check_completude,
    check_coherence,
    check_fraicheur,
    check_unicite,
    check_validite,
)

# --- Fixtures (petits DataFrames construits à la main) ----------------------


def _fait_ok() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date_snapshot": ["2026-09-05", "2026-09-05", "2026-09-05"],
            "id_titre": [1, 2, 3],
            "id_artiste": [10, 20, 10],
            "score_popularite": [500, 600, 700],
        }
    )


def _dim_titre_ok() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id_titre": [1, 2, 3],
            "nom": ["A", "B", "C"],
            "duree": [180, 200, 220],
            "bpm": [120.0, None, 90.0],
            "gain": [-5.0, -6.0, -7.0],
            "genre": [132, 116, 152],
        }
    )


def _dim_artiste_ok() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id_artiste": [10, 20],
            "nom": ["Artiste 1", "Artiste 2"],
            "nb_fan": [1000, 2000],
            "nb_album": [5, 3],
        }
    )


# --- Tests unitaires : check_unicite -----------------------------------------


def test_check_unicite_nominal():
    check_unicite(_fait_ok())  # ne doit pas lever


def test_check_unicite_doublon():
    fait = pd.concat([_fait_ok(), _fait_ok().iloc[[0]]], ignore_index=True)
    with pytest.raises(AssertionError):
        check_unicite(fait)


# --- Tests unitaires : check_completude --------------------------------------


def test_check_completude_nominal():
    assert check_completude(_fait_ok()) == 1.0


def test_check_completude_sous_seuil():
    fait = _fait_ok()
    fait.loc[0, "score_popularite"] = None
    with pytest.raises(AssertionError):
        check_completude(fait, seuil=0.98)


# --- Tests unitaires : check_validite -----------------------------------------


def test_check_validite_nominal():
    check_validite(_fait_ok(), _dim_titre_ok())  # ne doit pas lever


def test_check_validite_score_negatif():
    fait = _fait_ok()
    fait.loc[0, "score_popularite"] = -1
    with pytest.raises(AssertionError):
        check_validite(fait, _dim_titre_ok())


def test_check_validite_duree_invalide():
    dim_titre = _dim_titre_ok()
    dim_titre.loc[0, "duree"] = 0
    with pytest.raises(AssertionError):
        check_validite(_fait_ok(), dim_titre)


def test_check_validite_bpm_hors_plage():
    dim_titre = _dim_titre_ok()
    dim_titre.loc[0, "bpm"] = 350.0
    with pytest.raises(AssertionError):
        check_validite(_fait_ok(), dim_titre)


# --- Tests unitaires : check_coherence ---------------------------------------


def test_check_coherence_nominal():
    check_coherence(_fait_ok(), _dim_artiste_ok())  # ne doit pas lever


def test_check_coherence_artiste_orphelin():
    fait = _fait_ok()
    fait.loc[0, "id_artiste"] = 999
    with pytest.raises(AssertionError):
        check_coherence(fait, _dim_artiste_ok())


# --- Tests unitaires : check_fraicheur ---------------------------------------


def test_check_fraicheur_nominal():
    check_fraicheur(_fait_ok(), "2026-09-05")  # ne doit pas lever


def test_check_fraicheur_absente():
    with pytest.raises(AssertionError):
        check_fraicheur(_fait_ok(), "2026-09-06")


# --- Tests d'intégration : vrai export curated sur S3 ------------------------

_BUCKET = "soundpulse-datalake-618875"
_DATE = "2026-09-05"


def _read_curated_table(table: str) -> pd.DataFrame:
    """Télécharge une table curated depuis S3 et la charge en DataFrame pandas.

    Une table peut être répartie sur plusieurs fichiers Parquet (un par
    exécution Spark ayant réécrit la partition) : on les concatène tous.
    Lu en mémoire (`io.BytesIO`) plutôt que via un fichier temporaire, pour
    rester simple et portable entre systèmes de fichiers.
    """
    s3 = boto3.client("s3")
    prefix = f"curated/{table}/date={_DATE}/"
    frames = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            buffer = io.BytesIO()
            s3.download_fileobj(_BUCKET, key, buffer)
            buffer.seek(0)
            frames.append(pd.read_parquet(buffer))
    assert frames, f"Aucun fichier Parquet trouvé sous s3://{_BUCKET}/{prefix}"
    return pd.concat(frames, ignore_index=True)


@pytest.mark.integration
def test_quality_checks_sur_export_curated_reel():
    """Rejoue toutes les règles qualité sur le vrai export curated du 2026-09-05."""
    fait = _read_curated_table("fait_popularite")
    dim_titre = _read_curated_table("dim_titre")
    dim_artiste = _read_curated_table("dim_artiste")

    check_unicite(fait)
    completude = check_completude(fait)
    check_validite(fait, dim_titre)
    check_coherence(fait, dim_artiste)
    check_fraicheur(fait, _DATE)

    assert completude == 1.0
