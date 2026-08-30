"""Configuration partagée : lecture des variables d'environnement et des secrets."""
import os

from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.environ.get("AWS_REGION", "eu-west-3")
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

RAW_PREFIX = "raw"
STAGING_PREFIX = "staging"
CURATED_PREFIX = "curated"


def spotify_credentials() -> tuple[str, str]:
    """Renvoie (client_id, client_secret).

    En local : lus depuis .env. En production (Airflow/Glue) : à remplacer par un
    appel à AWS Secrets Manager (`secretsmanager:GetSecretValue` sur le secret
    provisionné par terraform/secrets.tf) plutôt que des variables d'environnement.
    """
    client_id = os.environ.get("SPOTIFY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError(
            "SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET manquants — voir .env.example"
        )
    return client_id, client_secret
