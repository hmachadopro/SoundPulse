# Rôle applicatif minimal pour l'extraction et les jobs Glue — accès restreint
# au bucket du projet uniquement (principe du moindre privilège). L'API Deezer
# utilisée ici ne nécessite aucune authentification : pas de Secrets Manager.

resource "aws_iam_role" "pipeline_role" {
  name = "${var.project_name}-pipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = ["glue.amazonaws.com", "ecs-tasks.amazonaws.com"]
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "pipeline_s3_access" {
  name = "${var.project_name}-s3-access"
  role = aws_iam_role.pipeline_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.datalake.arn, "${aws_s3_bucket.datalake.arn}/*"]
      }
    ]
  })
}
