variable "aws_region" {
  description = "Région AWS de déploiement"
  type        = string
  default     = "eu-west-3"
}

variable "project_name" {
  description = "Nom du projet, utilisé comme préfixe des ressources"
  type        = string
  default     = "soundpulse"
}

variable "bucket_suffix" {
  description = "Suffixe unique pour le nom du bucket S3 (les noms S3 sont globalement uniques)"
  type        = string
}
