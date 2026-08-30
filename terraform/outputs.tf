output "datalake_bucket_name" {
  value = aws_s3_bucket.datalake.bucket
}

output "pipeline_role_arn" {
  value = aws_iam_role.pipeline_role.arn
}

output "spotify_secret_arn" {
  value = aws_secretsmanager_secret.spotify_api.arn
}
