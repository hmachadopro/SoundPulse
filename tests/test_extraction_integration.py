"""Test d'intégration léger de l'extraction Spotify.

Contrairement aux tests unitaires, celui-ci appelle la vraie API Spotify (pas
de mock) pour vérifier que le pipeline d'extraction fonctionne bout en bout
avec de vrais credentials. Marqué `integration` pour rester exclu du run
pytest par défaut (voir pytest.ini) : il dépend d'un accès réseau et d'un
`.env` valide, ce qui ne convient pas à une CI qui doit rester déterministe.

Exécution manuelle : pytest -m integration tests/test_extraction_integration.py
"""
import pytest

from src.extract.spotify_extractor import (
    SEARCH_GENRES,
    build_spotify_client,
    extract_tracks_and_artists,
)

pytestmark = pytest.mark.integration


def test_extraction_sample_payload_structure():
    """Extrait un échantillon (un seul genre) et vérifie la structure du payload."""
    sp = build_spotify_client()

    # Un seul genre suffit pour un test d'intégration : on veut vérifier la
    # connectivité et la forme du payload, pas la volumétrie complète.
    sample_genre = SEARCH_GENRES[0]
    original_genres = list(SEARCH_GENRES)
    SEARCH_GENRES[:] = [sample_genre]
    try:
        payload = extract_tracks_and_artists(sp)
    finally:
        SEARCH_GENRES[:] = original_genres

    assert set(payload.keys()) == {"tracks", "artists"}
    assert len(payload["tracks"]) > 0
    assert len(payload["artists"]) > 0

    first_track = payload["tracks"][0]
    assert "id" in first_track and "artists" in first_track

    first_artist = payload["artists"][0]
    assert "id" in first_artist and "name" in first_artist
