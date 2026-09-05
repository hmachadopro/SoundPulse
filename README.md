# SoundPulse Analytics

Pipeline de données end-to-end sur **AWS** pour l'analyse du catalogue musical et des tendances d'écoute.

Projet portfolio Data Engineering — cahier des charges complet dans le wiki personnel (`WikiLlm/wiki/Perso/Portfolio-DataEngineering/SoundPulse-AWS/`).

`S3 · AWS Glue · Athena · Apache Airflow · Snowflake · Power BI · Terraform · GitHub Actions`

---

## Architecture

```
                        API Deezer (publique, sans authentification)
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
├── terraform/          # IaC : bucket S3, IAM (pas de Secrets Manager, Deezer n'en a pas besoin)
├── airflow/dags/        # DAG d'orchestration du pipeline
├── src/
│   ├── extract/          # Extraction API Deezer → S3 raw
│   └── common/           # Config partagée (variables d'environnement, clients AWS)
├── glue_jobs/            # Jobs PySpark : raw → staging → curated
├── tests/                 # Tests de qualité de données
└── docker-compose.yml     # Airflow local pour le développement
```

## Prérequis

- Compte AWS (free tier) + credentials configurés (`aws configure`)
- Aucun compte développeur externe requis : l'API Deezer utilisée (`/genre`,
  `/chart/{genre_id}/tracks`, `/track/{id}`, `/artist/{id}`) est publique et
  ne nécessite ni clé API ni OAuth.
- Terraform ≥ 1.9
- Docker + Docker Compose (pour Airflow en local)
- Python ≥ 3.12

## Démarrage rapide

```bash
cp .env.example .env   # renseigner AWS_REGION / S3_BUCKET_NAME
pip install -r requirements.txt

# Provisionner l'infrastructure AWS
cd terraform && terraform init && terraform plan && terraform apply

# Lancer l'extraction Deezer → S3 raw
cd .. && python -m src.extract.deezer_extractor

# Lancer Airflow en local
docker compose up -d
# UI Airflow : http://localhost:8080
```

## État d'avancement

Voir le planning prévisionnel du cahier des charges (section 9) et le suivi dans le wiki.

- [x] Phase 1 — Cadrage : repo, structure, Terraform de base
- [x] Phase 2 — Ingestion : script d'extraction Deezer + zone raw S3
- [ ] Phase 3 — Transformation : jobs Glue staging & curated
- [ ] Phase 4 — Orchestration : DAG Airflow complet, retry, alertes
- [ ] Phase 5 — Qualité & tests : contrôles automatisés
- [ ] Phase 6 — Restitution : modèle Power BI + dashboards
- [ ] Phase 7 — Documentation : schéma, démo
