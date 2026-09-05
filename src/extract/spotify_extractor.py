"""Extraction quotidienne des données Spotify (titres, artistes) vers la zone
raw de S3, partitionnée par date — couvre EF-01/EF-02 du CDC.

Nota : l'endpoint audio-features (danceability, energy, tempo...) a été
supprimé par Spotify en nov. 2024 (403 pour toutes les apps, quel que soit le
flow d'auth) — voir https://developer.spotify.com/blog/2024-11-27-changes-to-the-web-api.
Le payload ne contient donc plus que tracks/artists ; une source de
remplacement pour les features audio est hors périmètre de ce module (à
traiter en phase 3/4).

Usage :
    python -m src.extract.spotify_extractor
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import time

import boto3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from src.common.config import AWS_REGION, RAW_PREFIX, S3_BUCKET_NAME, spotify_credentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Genres de recherche pour échantillonner le catalogue — remplace l'ancienne
# approche par playlists (Top 50, RapCaviar...) : Spotify a rendu les
# playlists éditoriales/partenaires définitivement inaccessibles via l'API
# depuis nov. 2024 (404/403, quel que soit le flow d'auth), voir
# https://developer.spotify.com/blog/2024-11-27-changes-to-the-web-api.
# L'endpoint search(), lui, reste ouvert et couvre une plus grande diversité
# de genres que 3-5 playlists ne l'auraient permis.
SEARCH_GENRES = [
    "pop",
    "hip-hop",
    "rock",
    "electronic",
    "r&b",
    "indie",
    "latin",
    "k-pop",
    "metal",
    "country",
    "jazz",
    "afrobeats",
]

# Nombre de pages de résultats à parcourir par genre. La taille de page
# maximale documentée par Spotify pour search() est 50, mais cette app (mode
# développeur, quota non étendu) plafonne en pratique à 10 (au-delà : 400
# Invalid limit) — constaté empiriquement, pas dans la doc officielle.
# 5 pages/genre × 12 genres vise un ordre de grandeur de quelques centaines de
# titres uniques pour un run de test, chaque artiste distinct nécessitant
# ensuite un appel individuel (cf. extract_tracks_and_artists).
SEARCH_PAGES_PER_GENRE = 5
SEARCH_PAGE_SIZE = 10


def build_spotify_client() -> spotipy.Spotify:
    """Construit un client Spotify authentifié en Client Credentials Flow.

    Ce flow (app-only, sans login utilisateur) suffit ici : search() et
    artist() restent ouverts en Client Credentials. Les endpoints qui exigent
    désormais une authentification utilisateur (playlists) ou qui sont
    devenus inaccessibles (audio-features, get-several-artists) ne sont plus
    utilisés par ce module — voir le docstring en tête de fichier.

    Les paramètres retries/status_retries/backoff_factor sont fournis
    nativement par spotipy (urllib3.Retry sous le capot) : on les active plutôt
    que d'écrire une boucle de retry maison, pour absorber le rate limiting
    (429) et les erreurs serveur transitoires (5xx) rencontrés sur un run qui
    enchaîne plusieurs dizaines/centaines d'appels API.
    """
    client_id, client_secret = spotify_credentials()
    auth = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    return spotipy.Spotify(
        client_credentials_manager=auth,
        retries=3,
        status_retries=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        requests_timeout=10,
    )


def _fetch_genre_tracks(sp: spotipy.Spotify, genre: str) -> list[dict]:
    """Récupère jusqu'à `SEARCH_PAGES_PER_GENRE` pages de titres pour un genre.

    On pagine via `offset` plutôt que `sp.next()` (non disponible pour
    search()) ; on arrête dès qu'une page renvoie moins d'items que demandé
    (fin du catalogue disponible pour cette requête).
    """
    tracks: list[dict] = []
    for page in range(SEARCH_PAGES_PER_GENRE):
        result = sp.search(
            q=f'genre:"{genre}"',
            type="track",
            limit=SEARCH_PAGE_SIZE,
            offset=page * SEARCH_PAGE_SIZE,
        )
        items = result["tracks"]["items"]
        tracks.extend(t for t in items if t and t.get("id"))
        if len(items) < SEARCH_PAGE_SIZE:
            break
    return tracks


def extract_tracks_and_artists(sp: spotipy.Spotify) -> dict:
    """Extrait titres et artistes en échantillonnant le catalogue par genre.

    Les titres sont dédoublonnés entre genres (un morceau peut être renvoyé
    pour plusieurs tags de genre), de même que les artistes, dédoublonnés
    avant l'appel API pour éviter d'interroger plusieurs fois un même artiste
    présent sur plusieurs titres.
    """
    start = time.monotonic()
    tracks_by_id: dict[str, dict] = {}

    for genre in SEARCH_GENRES:
        genre_tracks = _fetch_genre_tracks(sp, genre)
        new_count = 0
        for track in genre_tracks:
            if track["id"] not in tracks_by_id:
                tracks_by_id[track["id"]] = track
                new_count += 1
        logger.info(
            "Genre %s : %d titres reçus, %d nouveaux (hors doublons)",
            genre,
            len(genre_tracks),
            new_count,
        )

    tracks = list(tracks_by_id.values())
    artist_ids: set[str] = set()
    for track in tracks:
        artist_ids.update(a["id"] for a in track["artists"] if a.get("id"))

    # Un appel par artiste unique : l'endpoint batch "get several artists" est
    # devenu inaccessible (403) depuis nov. 2024, contrairement à l'endpoint
    # unitaire sp.artist(). Le dédoublonnage ci-dessus limite le nombre
    # d'appels au nombre d'artistes distincts, pas au nombre de titres.
    artists: list[dict] = [sp.artist(artist_id) for artist_id in artist_ids]

    duration = time.monotonic() - start
    logger.info(
        "Extraction terminée en %.1fs — %d titres uniques, %d artistes uniques",
        duration,
        len(tracks),
        len(artists),
    )

    return {"tracks": tracks, "artists": artists}


def write_to_s3_raw(payload: dict, snapshot_date: dt.date) -> None:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    key = f"{RAW_PREFIX}/date={snapshot_date.isoformat()}/spotify_snapshot.json"
    body = json.dumps(payload).encode("utf-8")
    start = time.monotonic()
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    duration = time.monotonic() - start
    logger.info(
        "Snapshot écrit en %.1fs : s3://%s/%s (%d Ko)",
        duration,
        S3_BUCKET_NAME,
        key,
        len(body) // 1024,
    )


def main() -> None:
    sp = build_spotify_client()
    payload = extract_tracks_and_artists(sp)
    write_to_s3_raw(payload, dt.date.today())


if __name__ == "__main__":
    main()
