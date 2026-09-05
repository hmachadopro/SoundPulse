"""Configuration partagée : lecture des variables d'environnement."""
import os

from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.environ.get("AWS_REGION", "eu-west-3")
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

RAW_PREFIX = "raw"
STAGING_PREFIX = "staging"
CURATED_PREFIX = "curated"
