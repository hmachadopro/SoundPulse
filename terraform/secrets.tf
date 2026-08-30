# Credentials Spotify — jamais en clair dans le code (ENF-06 du CDC).
# La valeur du secret est renseignée hors Terraform (console AWS ou `aws secretsmanager put-secret-value`)
# pour ne jamais faire transiter le client secret par le state Terraform en clair.

resource "aws_secretsmanager_secret" "spotify_api" {
  name        = "${var.project_name}/spotify-api-credentials"
  description = "Client ID / Client Secret de l'API Spotify pour l'extraction SoundPulse"
}
