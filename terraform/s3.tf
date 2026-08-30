# Data lake unique avec préfixes par couche (raw/staging/curated) — voir wiki Outils/Amazon_S3.md

resource "aws_s3_bucket" "datalake" {
  bucket = "${var.project_name}-datalake-${var.bucket_suffix}"
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle : bascule automatique vers Intelligent-Tiering, purge au-delà de la
# profondeur d'historique visée par le projet (3 mois glissants, cf. EF-06 du CDC)
resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "raw-intelligent-tiering"
    status = "Enabled"
    filter { prefix = "raw/" }

    transition {
      days          = 0
      storage_class = "INTELLIGENT_TIERING"
    }
  }

  rule {
    id     = "expire-old-snapshots"
    status = "Enabled"
    filter { prefix = "raw/" }

    expiration {
      days = 100 # ~3 mois glissants
    }
  }
}
