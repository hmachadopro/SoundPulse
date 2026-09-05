"""Extraction quotidienne des données Deezer (titres, artistes) vers la zone
raw de S3, partitionnée par date — couvre EF-01/EF-02 du CDC.

L'API Deezer utilisée ici (`/genre`, `/chart/{genre_id}/tracks`, `/artist/{id}`)
est publique : aucune clé API ni flow OAuth n'est nécessaire, contrairement à
la tentative précédente basée sur l'API Spotify (voir la branche
`archive/spotify-attempt`).

L'échantillonnage se fait par charts de genre (`/chart/{genre_id}/tracks`),
qui remplacent directement les playlists éditoriales utilisées côté Spotify :
Deezer expose ses 28 genres officiels via `/genre`, chacun avec son propre
classement des titres les mieux notés.

Usage :
    python -m src.extract.deezer_extractor
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import time

import boto3
import requests

from src.common.config import AWS_REGION, RAW_PREFIX, S3_BUCKET_NAME

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEEZER_API_BASE = "https://api.deezer.com"

# Le rate limit Deezer (~50 requêtes/5s, source communautaire — non garanti
# officiellement) invite à une marge de sécurité confortable : on vise ~6
# requêtes/s plutôt que le maximum théorique de 10/s.
MIN_REQUEST_INTERVAL_S = 1 / 6

# Genre id=0 = "Tous les genres", agrégat non représentatif d'un genre réel
# (voir la doc /genre) : on l'exclut de l'échantillonnage.
ALL_GENRES_ID = 0

# Pagination du chart par genre : 100 titres/page, jusqu'à 2 pages visées.
# Cible un ordre de grandeur de quelques centaines de titres par genre pour ce
# premier run de test (pas les 5000 titres/1000 artistes cibles du CDC).
CHART_PAGE_SIZE = 100
MAX_CHART_PAGES_PER_GENRE = 2

# Tentatives de retry sur erreurs transitoires (429 / 5xx).
MAX_RETRIES = 5
RETRY_BACKOFF_BASE_S = 1.0

_last_request_ts = 0.0


def _throttle() -> None:
    """Impose un intervalle minimal entre deux requêtes sortantes."""
    global _last_request_ts
    elapsed = time.monotonic() - _last_request_ts
    wait = MIN_REQUEST_INTERVAL_S - elapsed
    if wait > 0:
        time.sleep(wait)
    _last_request_ts = time.monotonic()


def _get_json(url: str, params: dict | None = None) -> dict:
    """GET avec throttling et retry basique sur 429/5xx.

    Sur 429, respecte l'en-tête `Retry-After` quand Deezer le fournit ;
    sinon, comme pour les 5xx, applique un backoff exponentiel simple.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        _throttle()
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 429:
            wait = float(response.headers.get("Retry-After", RETRY_BACKOFF_BASE_S * attempt))
            logger.warning("429 reçu sur %s — nouvelle tentative dans %.1fs", url, wait)
            time.sleep(wait)
            continue

        if response.status_code >= 500:
            wait = RETRY_BACKOFF_BASE_S * attempt
            logger.warning(
                "Erreur serveur %d sur %s — nouvelle tentative dans %.1fs",
                response.status_code,
                url,
                wait,
            )
            time.sleep(wait)
            continue

        response.raise_for_status()
        body = response.json()

        # Deezer signale aussi le rate limiting (et d'autres erreurs) par un
        # corps `{"error": {...}}` avec un statut HTTP 200 — invisible pour
        # les branches 429/5xx ci-dessus. Constaté en pratique sous charge
        # concurrente (deux extractions simultanées) : sans ce contrôle, un
        # appel `/artist/{id}` en erreur renvoie un dict sans clé `id` et
        # fait planter l'appelant avec un KeyError bien plus loin.
        if isinstance(body, dict) and "error" in body:
            wait = RETRY_BACKOFF_BASE_S * attempt
            logger.warning(
                "Erreur API Deezer sur %s : %s — nouvelle tentative dans %.1fs",
                url,
                body["error"],
                wait,
            )
            time.sleep(wait)
            continue

        return body

    raise RuntimeError(f"Échec après {MAX_RETRIES} tentatives : {url}")


def fetch_genres() -> list[dict]:
    """Récupère les genres officiels Deezer, hors id=0 ("Tous les genres")."""
    payload = _get_json(f"{DEEZER_API_BASE}/genre")
    return [g for g in payload.get("data", []) if g["id"] != ALL_GENRES_ID]


def fetch_chart_tracks(genre_id: int) -> list[dict]:
    """Pagine sur le chart d'un genre jusqu'à `MAX_CHART_PAGES_PER_GENRE` pages.

    S'arrête plus tôt si Deezer ne renvoie pas de lien `next` (fin du chart
    disponible pour ce genre — certains genres de niche en ont moins).
    """
    tracks: list[dict] = []
    url = f"{DEEZER_API_BASE}/chart/{genre_id}/tracks"
    params = {"limit": CHART_PAGE_SIZE}

    for _ in range(MAX_CHART_PAGES_PER_GENRE):
        payload = _get_json(url, params=params)
        tracks.extend(payload.get("data", []))
        next_url = payload.get("next")
        if not next_url:
            break
        url, params = next_url, None  # l'URL "next" embarque déjà index/limit

    return tracks


def fetch_artist(artist_id: int) -> dict:
    """Récupère les attributs complets d'un artiste (nb_fan, nb_album...)."""
    return _get_json(f"{DEEZER_API_BASE}/artist/{artist_id}")


def extract() -> dict:
    """Échantillonne le catalogue Deezer par genre et déduplique titres/artistes.

    L'id du genre d'origine est conservé sur chaque titre pour peupler de
    façon fiable `dim_titre.genre` (voir wiki) — contrairement à Spotify, un
    même titre Deezer ne peut apparaître ici qu'au chart des genres où il est
    effectivement classé, donc pas de conflit à trancher en aval.
    """
    start = time.monotonic()

    genres = fetch_genres()
    logger.info("%d genres récupérés", len(genres))

    tracks_by_id: dict[int, dict] = {}
    for genre in genres:
        genre_id = genre["id"]
        chart_tracks = fetch_chart_tracks(genre_id)
        new_count = 0
        for track in chart_tracks:
            if track["id"] not in tracks_by_id:
                track_record = {
                    "id": track["id"],
                    "title": track["title"],
                    "duration": track["duration"],
                    "rank": track["rank"],
                    "bpm": track.get("bpm"),
                    "gain": track.get("gain"),
                    "explicit_lyrics": track.get("explicit_lyrics"),
                    "artist_id": track["artist"]["id"],
                    "genre_id": genre_id,
                }
                tracks_by_id[track["id"]] = track_record
                new_count += 1
        logger.info(
            "Genre %s (id=%d) : %d titres reçus, %d nouveaux (hors doublons)",
            genre["name"],
            genre_id,
            len(chart_tracks),
            new_count,
        )

    tracks = list(tracks_by_id.values())
    artist_ids = {t["artist_id"] for t in tracks}

    # Un appel par artiste unique : le dédoublonnage en amont limite le
    # nombre de requêtes au nombre d'artistes distincts, pas au nombre de
    # titres (un artiste peut apparaître dans plusieurs genres).
    artists: list[dict] = []
    for artist_id in artist_ids:
        artist = fetch_artist(artist_id)
        artists.append(
            {
                "id": artist["id"],
                "name": artist["name"],
                "nb_fan": artist.get("nb_fan"),
                "nb_album": artist.get("nb_album"),
            }
        )

    duration = time.monotonic() - start
    logger.info(
        "Extraction terminée en %.1fs — %d genres, %d titres uniques, %d artistes uniques",
        duration,
        len(genres),
        len(tracks),
        len(artists),
    )

    return {"tracks": tracks, "artists": artists}


def write_to_s3_raw(payload: dict, snapshot_date: dt.date) -> None:
    """Écrit le snapshot JSON dans la zone raw, partitionnée par date."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    key = f"{RAW_PREFIX}/date={snapshot_date.isoformat()}/deezer_snapshot.json"
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
    payload = extract()
    write_to_s3_raw(payload, dt.date.today())


if __name__ == "__main__":
    main()
