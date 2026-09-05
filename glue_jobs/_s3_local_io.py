"""Aller-retour S3 <-> disque local pour exécuter des jobs Spark en local.

Spark en local (sans les JARs hadoop-aws) ne sait pas lire/écrire directement
sur s3://, et configurer ce connecteur ajoute une complexité (JARs, versions
hadoop-aws/aws-java-sdk-bundle à faire correspondre) disproportionnée pour ce
qui reste un run de test. On télécharge donc les entrées en local via boto3
avant Spark et on upload les sorties après — le corps des jobs continue de
raisonner en chemins S3 (voir staging_job.py/curated_job.py) pour rester
portable vers un vrai job Glue (qui lira/écrira nativement sur s3://).
"""
from __future__ import annotations

import os

import boto3

# Lu directement depuis l'environnement plutôt qu'importé de src.common.config :
# ces modules doivent rester autonomes (pas de dépendance au package `src`) pour
# être déployables tels quels comme jobs Glue, où seul ce dossier serait livré.
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-3")


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Découpe `s3://bucket/clé/...` en (bucket, clé)."""
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"URI S3 invalide (attendu s3://...) : {s3_uri}")
    bucket, _, key = s3_uri.removeprefix("s3://").partition("/")
    return bucket, key


def download_object(s3_uri: str, local_path: str) -> None:
    """Télécharge un objet S3 unique (ex. le snapshot JSON raw) vers `local_path`."""
    bucket, key = parse_s3_uri(s3_uri)
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    boto3.client("s3", region_name=AWS_REGION).download_file(bucket, key, local_path)


def download_prefix(s3_uri_prefix: str, local_dir: str) -> None:
    """Télécharge récursivement tous les objets sous un préfixe S3 (ex. un
    dossier Parquet multi-fichiers écrit par un run Spark précédent) vers
    `local_dir`, en conservant l'arborescence relative.
    """
    bucket, prefix = parse_s3_uri(s3_uri_prefix)
    s3 = boto3.client("s3", region_name=AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):  # marqueur de "dossier" S3, pas un fichier réel
                continue
            relative_path = key[len(prefix):].lstrip("/")
            local_path = os.path.join(local_dir, relative_path)
            os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
            s3.download_file(bucket, key, local_path)


def upload_dir(local_dir: str, s3_uri_prefix: str) -> None:
    """Upload récursivement le contenu de `local_dir` (ex. les fichiers
    part-*.parquet écrits par Spark) sous un préfixe S3.
    """
    bucket, prefix = parse_s3_uri(s3_uri_prefix)
    s3 = boto3.client("s3", region_name=AWS_REGION)
    for root, _dirs, files in os.walk(local_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_dir).replace(os.sep, "/")
            key = f"{prefix.rstrip('/')}/{relative_path}"
            s3.upload_file(local_path, bucket, key)
