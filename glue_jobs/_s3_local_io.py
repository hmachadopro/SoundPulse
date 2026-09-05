"""Pont S3 ↔ disque local pour faire tourner les jobs Glue en local (phase 3).

Spark en local n'a pas le connecteur Hadoop-AWS (`s3a://`) configuré, et
l'installer proprement (JARs `hadoop-aws`/`aws-java-sdk-bundle` alignés avec
la version de Hadoop embarquée par PySpark) ajoute une complexité
disproportionnée tant qu'on ne fait que valider la logique de transformation.
On contourne ici via boto3 : téléchargement du snapshot raw vers un
répertoire local avant l'exécution Spark, upload des Parquet produits après.
Le corps des jobs Spark (`staging_job.py`, `curated_job.py`), lui, reste
écrit comme s'il lisait/écrivait directement des chemins S3, pour rester
portable vers un vrai job AWS Glue en phase orchestration (il suffira alors
de lui passer des chemins `s3://...` et de retirer cette étape de pont).
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path, PurePosixPath

import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_prefix(bucket: str, s3_prefix: str, local_dir: Path) -> int:
    """Télécharge tous les objets sous `s3_prefix`, en conservant la structure relative des clés.

    Returns:
        Le nombre d'objets téléchargés.
    """
    s3 = boto3.client("s3")
    prefix = s3_prefix.rstrip("/")
    count = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            relative = PurePosixPath(key).relative_to(prefix)
            dest = local_dir / Path(*relative.parts)
            dest.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(dest))
            count += 1
    logger.info("%d objet(s) téléchargé(s) : s3://%s/%s → %s", count, bucket, prefix, local_dir)
    return count


def delete_prefix(bucket: str, s3_prefix: str) -> int:
    """Supprime tous les objets sous `s3_prefix`.

    Returns:
        Le nombre d'objets supprimés.
    """
    s3 = boto3.client("s3")
    prefix = s3_prefix.rstrip("/")
    count = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            count += len(keys)
    if count:
        logger.info("%d objet(s) supprimé(s) sous s3://%s/%s", count, bucket, prefix)
    return count


def upload_dir(local_dir: Path, bucket: str, s3_prefix: str, clean: bool = True) -> int:
    """Upload récursivement le contenu de `local_dir` vers `s3_prefix`, en conservant la structure relative.

    Spark nomme ses fichiers de sortie avec un UUID par run (`part-00000-<uuid>...`) :
    un simple upload par-dessus une exécution précédente laisserait les anciens
    fichiers à côté des nouveaux, et Spark les fusionnerait silencieusement à la
    prochaine lecture du dossier. Le pipeline étant destiné à tourner
    quotidiennement (le même `s3_prefix` est réécrit à chaque run), `clean=True`
    purge la destination avant upload pour éviter cette accumulation.

    Returns:
        Le nombre de fichiers uploadés.
    """
    if clean:
        delete_prefix(bucket, s3_prefix)

    s3 = boto3.client("s3")
    prefix = s3_prefix.rstrip("/")
    count = 0
    for path in sorted(local_dir.rglob("*")):
        if path.is_file():
            relative = path.relative_to(local_dir).as_posix()
            key = f"{prefix}/{relative}"
            s3.upload_file(str(path), bucket, key)
            count += 1
    logger.info("%d fichier(s) uploadé(s) : %s → s3://%s/%s", count, local_dir, bucket, prefix)
    return count


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    download = subparsers.add_parser("download", help="Télécharge un préfixe S3 vers un répertoire local")
    download.add_argument("--bucket", required=True)
    download.add_argument("--prefix", required=True)
    download.add_argument("--local_dir", required=True)

    upload = subparsers.add_parser("upload", help="Upload un répertoire local vers un préfixe S3")
    upload.add_argument("--bucket", required=True)
    upload.add_argument("--prefix", required=True)
    upload.add_argument("--local_dir", required=True)
    upload.add_argument(
        "--no-clean",
        action="store_true",
        help="Ne pas purger le préfixe S3 avant upload (par défaut : purgé, voir docstring upload_dir)",
    )

    args = parser.parse_args()
    if args.command == "download":
        download_prefix(args.bucket, args.prefix, Path(args.local_dir))
    else:
        upload_dir(Path(args.local_dir), args.bucket, args.prefix, clean=not args.no_clean)


if __name__ == "__main__":
    _main()
