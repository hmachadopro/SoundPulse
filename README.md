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
├── sql/                   # Requêtes SQL prêtes à l'emploi (Athena / Power BI)
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
- **Pour exécuter les jobs PySpark (`glue_jobs/`) en local sous Windows** :
  Spark s'appuie sur des binaires Hadoop natifs (`winutils.exe`,
  `hadoop.dll`) même en mode local, sans quoi l'écriture Parquet échoue.
  Télécharger une distribution `winutils` correspondant à la version de
  Hadoop embarquée par PySpark (ex. https://github.com/kontext-tech/winutils),
  la placer dans un dossier type `C:\hadoop\bin\`, puis définir la variable
  d'environnement `HADOOP_HOME` (ex. `C:\hadoop`) au niveau utilisateur ou
  système et rouvrir le terminal pour qu'elle soit prise en compte.

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

## Connexion Power BI

Les 4 tables curated sont référencées dans Glue Data Catalog (base
`soundpulse`, voir `terraform/glue_catalog.tf`) et requêtables en SQL via
Amazon Athena (workgroup dédié `soundpulse`, voir `terraform/athena.tf`).
Des requêtes prêtes à l'emploi sont dans `sql/dashboard_queries.sql`.

Power BI Desktop propose un connecteur Athena natif certifié Microsoft
(disponible depuis juillet 2021), mais il s'appuie sur le driver ODBC Amazon
Athena — les deux sont nécessaires :

1. **Installer le driver ODBC Amazon Athena** (Simba) : télécharger depuis
   [la documentation officielle AWS](https://docs.aws.amazon.com/athena/latest/ug/connect-with-odbc-and-power-bi.html)
   et l'installer sur le poste Windows (version 64 bits, assortie à Power BI
   Desktop).
2. **Power BI Desktop** → onglet *Accueil* → *Obtenir les données* →
   rechercher "Athena" → sélectionner **Amazon Athena** → *Se connecter*.
3. Renseigner :
   - **Région AWS** : `eu-west-3`
   - **Emplacement des résultats de requête (S3 Output Location)** :
     `s3://soundpulse-datalake-618875/athena-query-results/` (celui du
     workgroup `soundpulse` créé par Terraform)
   - **Credentials** : les mêmes que pour la CLI locale (clé/secret du
     profil AWS configuré, ou un profil dédié en lecture seule — jamais les
     credentials du rôle IAM `soundpulse-pipeline-role`, réservé aux jobs).
4. Choisir le mode **Import** (volume de données de ce projet portfolio :
   quelques milliers de lignes, largement dans les limites d'un import) ou
   *DirectQuery* si des rafraîchissements très fréquents sont voulus.
5. Dans le navigateur de schéma, sélectionner la base `soundpulse` puis les
   4 tables (`fait_popularite`, `dim_titre`, `dim_artiste`, `dim_date`) et
   charger.
6. Construire les visuels à partir de ces 4 tables — modèle en étoile,
   relations à créer manuellement dans Power BI entre `fait_popularite` et
   les 3 dimensions sur leurs clés (`id_titre`, `id_artiste`, `date`).

⚠️ **La construction du rapport Power BI lui-même (visuels, mise en page,
publication) est une étape manuelle, interface graphique uniquement — hors
périmètre d'un agent codeur** (voir "Phase 6b" ci-dessous).

## État d'avancement

Voir le planning prévisionnel du cahier des charges (section 9) et le suivi dans le wiki.

- [x] Phase 1 — Cadrage : repo, structure, Terraform de base
- [x] Phase 2 — Ingestion : script d'extraction Deezer + zone raw S3
- [x] Phase 3 — Transformation : jobs Glue staging & curated
- [x] Phase 4 — Orchestration : DAG Airflow complet, retry, alertes
- [x] Phase 5 — Qualité & tests : contrôles automatisés
- [x] Phase 6a — Infra Athena/Glue Catalog + requêtes SQL : Glue Data
      Catalog déployé (base + 4 tables), workgroup Athena `soundpulse`,
      partitions chargées (`MSCK REPAIR TABLE`), `COUNT(*)` vérifiés via CLI
      Athena (2372/2372/1235/1) et les 5 requêtes de
      `sql/dashboard_queries.sql` exécutées avec succès sur les vraies
      données.
- [ ] Phase 6b — Rapport Power BI construit : étape manuelle utilisateur
      (interface graphique Power BI Desktop), hors périmètre agent.
- [ ] Phase 7 — Documentation : schéma, démo
