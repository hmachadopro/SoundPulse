# Workgroup dédié avec emplacement de résultats explicite plutôt que le
# bucket Athena par défaut (géré par le compte, hors contrôle IAM/lifecycle
# du projet) — bonne pratique Athena, évite les surprises de facturation et
# de permissions sur un emplacement qu'on ne maîtrise pas.
resource "aws_athena_workgroup" "soundpulse" {
  name = "soundpulse"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.datalake.bucket}/athena-query-results/"
    }
  }
}
