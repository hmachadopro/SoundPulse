# SoundPulse Analytics

Pipeline de données end-to-end sur **AWS** pour l'analyse du catalogue musical et des tendances d'écoute.

Projet portfolio Data Engineering — cahier des charges complet dans le wiki personnel (`WikiLlm/wiki/Perso/Portfolio-DataEngineering/SoundPulse-AWS/`).

`S3 · AWS Glue · Athena · Apache Airflow · Snowflake · Power BI · Terraform · GitHub Actions`

---

## Architecture

```
                        API Spotify
                            │  extraction Python (batch quotidien)
                            ▼
        Amazon S3 — zone RAW (JSON brut, partitionné par date)
                            │  AWS Glue (jobs Spark : nettoyage, typage)
                            ▼
        Amazon S3 — zone STAGING (Parquet nettoyé)
                            │  AWS Glue + Glue Data Catalog (modélisation étoile)
                            ▼
        Amazon S3 — zone CURATED  →  Amazon Athena / Snowflake
                            │  requêtes SQL
                            ▼
        Power BI — Tableaux de bord décisionnels

Orchestration transversale : Apache Airflow (dans Docker)
IaC : Terraform  ·  CI/CD : GitHub Actions
```

Modèle en étoile : `fait_popularite`, `dim_titre`, `dim_artiste`, `dim_date`.

## Structure du repo

```
SoundPulse/
├── terraform/          # IaC : bucket S3, IAM, Glue Data Catalog, Secrets Manager
├── airflow/dags/        # DAG d'orchestration du pipeline
├── src/
│   ├── extract/          # Extraction API Spotify → S3 raw
│   └── common/           # Config partagée (lecture des secrets, clients AWS)
├── glue_jobs/            # Jobs PySpark : raw → staging → curated
├── tests/                 # Tests de qualité de données
├── .github/workflows/     # CI/CD (lint, tests, terraform plan/apply)
└── docker-compose.yml     # Airflow local pour le développement
```

## Prérequis

- Compte AWS (free tier) + credentials configurés (`aws configure` ou variables d'environnement)
- Compte développeur Spotify (Client ID / Secret) sur https://developer.spotify.com/dashboard
- Terraform ≥ 1.9
- Docker + Docker Compose (pour Airflow en local)
- Python ≥ 3.11

## Démarrage rapide

```bash
cp .env.example .env   # renseigner les credentials Spotify + AWS
pip install -r requirements.txt

# Provisionner l'infrastructure AWS
cd terraform && terraform init && terraform plan && terraform apply

# Lancer Airflow en local
cd .. && docker compose up -d
# UI Airflow : http://localhost:8080
```

## État d'avancement

Voir le planning prévisionnel du cahier des charges (section 9) et le suivi dans le wiki. Ce repo est un squelette de départ (phase 1 — cadrage) : structure posée, infra Terraform de base, DAG et jobs Glue en stub à compléter au fil des phases 2 à 7.

- [x] Phase 1 — Cadrage : repo, structure, Terraform de base
- [x] Phase 2 — Ingestion : script d'extraction Spotify + zone raw S3
- [ ] Phase 3 — Transformation : jobs Glue staging & curated
- [ ] Phase 4 — Orchestration : DAG Airflow complet, retry, alertes
- [ ] Phase 5 — Qualité & tests : contrôles automatisés
- [ ] Phase 6 — Restitution : modèle Power BI + dashboards
- [ ] Phase 7 — Documentation : schéma, démo
