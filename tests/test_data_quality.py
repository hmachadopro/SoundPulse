"""Tests de qualité des données — traduction pytest des règles de la section 7 du CDC.

À exécuter contre un DataFrame curated (pandas) une fois le pipeline en place.
Squelette : la fixture `curated_titres` reste à brancher sur un vrai run
(local ou S3) en phase 5.
"""
import pandas as pd
import pytest


@pytest.fixture
def curated_titres() -> pd.DataFrame:
    pytest.skip("Brancher sur un export réel de fait_popularite (phase 5)")


def test_unicite_titre_date(curated_titres):
    doublons = curated_titres.duplicated(subset=["id_titre", "date_snapshot"])
    assert not doublons.any(), "Doublon détecté sur (id_titre, date_snapshot)"


def test_completude_score_popularite(curated_titres):
    taux_non_nul = curated_titres["score_popularite"].notna().mean()
    assert taux_non_nul > 0.98, f"Complétude insuffisante : {taux_non_nul:.2%}"


def test_validite_bpm(curated_titres):
    assert curated_titres["bpm"].between(0, 250).all()
