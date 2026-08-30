"""Extraction quotidienne des données Spotify (playlists, titres, artistes, audio
features) vers la zone raw de S3, partitionnée par date — couvre EF-01/EF-02 du CDC.

Usage :
    python -m src.extract.spotify_extractor
"""
from __future__ import annotations

import datetime as dt
import json
import logging

import boto3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from src.common.config import AWS_REGION, RAW_PREFIX, S3_BUCKET_NAME, spotify_credentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Playlists de référence pour échantillonner le marché (à ajuster/étendre).
SEED_PLAYLIST_IDS = [
    "37i9dQZEVXbMDoHDwVN2tF",  # Global Top 50
]


def build_spotify_client() -> spotipy.Spotify:
    client_id, client_secret = spotify_credentials()
    auth = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    return spotipy.Spotify(client_credentials_manager=auth)


def extract_tracks_and_artists(sp: spotipy.Spotify) -> dict:
    tracks: list[dict] = []
    artist_ids: set[str] = set()

    for playlist_id in SEED_PLAYLIST_IDS:
        results = sp.playlist_items(playlist_id, additional_types=["track"])
        for item in results["items"]:
            track = item.get("track")
            if not track:
                continue
            tracks.append(track)
            artist_ids.update(a["id"] for a in track["artists"] if a.get("id"))

    audio_features = sp.audio_features([t["id"] for t in tracks if t.get("id")])
    artists = [sp.artist(a) for a in artist_ids]

    return {"tracks": tracks, "audio_features": audio_features, "artists": artists}


def write_to_s3_raw(payload: dict, snapshot_date: dt.date) -> None:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    key = f"{RAW_PREFIX}/date={snapshot_date.isoformat()}/spotify_snapshot.json"
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Snapshot écrit : s3://%s/%s", S3_BUCKET_NAME, key)


def main() -> None:
    sp = build_spotify_client()
    payload = extract_tracks_and_artists(sp)
    write_to_s3_raw(payload, dt.date.today())


if __name__ == "__main__":
    main()
