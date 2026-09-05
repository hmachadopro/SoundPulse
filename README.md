# SoundPulse Analytics

Pipeline de données perso sur AWS qui ingère chaque jour le catalogue Deezer (titres, artistes, popularité, genres) pour repérer des tendances et des artistes émergents. Projet portfolio Data Engineering, cahier des charges complet dans mon wiki personnel (`WikiLlm/wiki/Perso/Portfolio-DataEngineering/SoundPulse-AWS/`).

Stack : API Deezer, S3, PySpark, Airflow (Docker), Glue Data Catalog, Athena, Terraform, GitHub Actions.

## Architecture

```
API Deezer (publique, sans authentification)
    │  extraction Python throttlée, retry sur 429/5xx (src/extract/deezer_extractor.py)
    ▼
S3, zone raw (JSON brut, un fichier par jour, partitionné date=YYYY-MM-DD)
    │  job PySpark portable (glue_jobs/staging_job.py)
    ▼
S3, zone staging (Parquet typé et dédupliqué)
    │  job PySpark portable (glue_jobs/curated_job.py) : modélisation en étoile + contrôles qualité bloquants
    ▼
S3, zone curated (fait_popularite, dim_titre, dim_artiste, dim_date)
    │  Glue Data Catalog référence ces 4 tables (terraform/glue_catalog.tf)
    ▼
Amazon Athena (workgroup dédié) → requêtes SQL (sql/dashboard_queries.sql) → Power BI Desktop
```

Les jobs `staging_job.py` et `curated_job.py` sont volontairement écrits en PySpark portable (argparse + SparkSession standard, pas de dépendance à `awsglue`) et tournent en local ou dans le conteneur Airflow, pas comme de vrais jobs AWS Glue managés : ça évite de payer des DPU tant que la logique n'est pas stabilisée, voir la section Limites connues.

L'orchestration passe par un DAG Airflow (`airflow/dags/soundpulse_pipeline.py`) dans Docker Compose. L'image Airflow est une image custom (`Dockerfile`) qui ajoute un JRE 17 et les dépendances Python du projet à l'image officielle `apache/airflow`, qui n'a ni Java ni PySpark de base. Le DAG enchaîne extraction, staging, curated et une vérification de fraîcheur (la partition du jour existe bien dans le curated), avec retry automatique sur chaque tâche et un log d'échec structuré (pas de notification externe pour l'instant).

## Structure du repo

```
SoundPulse/
├── terraform/           IaC : bucket S3, IAM, Glue Data Catalog, workgroup Athena
├── airflow/dags/         DAG d'orchestration
├── Dockerfile            Image Airflow + JRE + dépendances Python
├── docker-compose.yml    Airflow en local (Postgres + LocalExecutor)
├── src/
│   ├── extract/           Extraction API Deezer → S3 raw
│   └── common/            Config partagée et contrôles qualité (quality_checks.py)
├── glue_jobs/            Jobs PySpark raw → staging → curated
├── sql/                   Requêtes prêtes à l'emploi (Athena / Power BI)
├── tests/                 Tests unitaires + un test d'intégration marqué (exclu de la CI)
├── .github/workflows/     CI : tests et terraform plan
└── DEMO_CHECKLIST.md     Guide pour enregistrer la démo (étape manuelle)
```

## Méthode de travail

Ce projet est construit avec Claude Code, sur un mode que j'appellerais "prompteur" : je conçois l'architecture, je prends les décisions (choix des technos, arbitrages quand une API ne fait pas ce qu'on espérait, changement de direction si besoin), et l'agent écrit le code à partir d'un prompt détaillé par étape (scaffold, extraction, transformation, orchestration, qualité, restitution, documentation). Chaque prompt part de l'état réel du repo plutôt que d'une supposition, et l'agent vérifie ce qu'il avance : il relit le code existant avant de le modifier, teste contre de vraies ressources AWS plutôt que de simuler, et demande confirmation avant toute action à conséquence réelle (créer des ressources AWS, attacher une permission IAM).

Deux décisions illustrent bien ce fonctionnement, pas comme des ratés à cacher mais comme des choix de méthode assumés. La première version du repo visait l'API Spotify, mais Spotify a fermé l'accès à `audio-features` et aux playlists éditoriales fin 2024, ce qui cassait une bonne partie du plan de collecte (tempo, énergie, dansabilité, échantillonnage par playlist). Plutôt que de bricoler un contournement fragile, j'ai choisi de repartir sur l'API Deezer, publique et sans authentification, quitte à perdre certains champs (bpm quasi jamais renseigné, voir Limites connues) et à réécrire toute la logique métier. Le repo a été réinitialisé pour ce pivot (`git checkout --orphan`), l'ancienne tentative reste consultable sur la branche `archive/spotify-attempt`.

La seconde, plus anecdotique : le scaffold initial (structure des dossiers, Terraform de base, stubs) a été fait dans le même prompt que la première extraction fonctionnelle, alors que la version Spotify avait séparé les deux. L'absence totale d'authentification côté Deezer simplifiait assez le travail pour regrouper les deux étapes sans surcharger le prompt.

## Reproduire ce projet depuis zéro

1. Cloner le repo. Prérequis : Python 3.12, Docker + Docker Compose, Terraform 1.9 ou plus récent, un compte AWS (le volume de ce projet reste dans le free tier).
2. `aws configure` avec un utilisateur IAM qui a de quoi gérer S3, IAM (roles), Glue et Athena. Le rôle applicatif `soundpulse-pipeline-role` créé par Terraform, lui, reste à privilège minimal (accès S3 seulement) ; c'est l'utilisateur humain qui a besoin de permissions plus larges pour provisionner l'infra.
3. `cp .env.example .env`, renseigner `AWS_REGION` et `S3_BUCKET_NAME`.
4. `pip install -r requirements.txt`, idéalement dans un environnement virtuel.
5. Sous Windows, si on veut lancer les jobs PySpark en dehors du conteneur Airflow (pour du debug local), il faut `winutils.exe`/`hadoop.dll` et la variable `HADOOP_HOME` pointant dessus. Pas nécessaire si on se contente de faire tourner le DAG dans Docker.
6. Dans `terraform/`, créer un `terraform.tfvars` avec un `bucket_suffix` unique (les noms de bucket S3 sont globaux à AWS). Puis `terraform init`, `terraform plan`, relire le plan, et seulement ensuite `terraform apply`. Ce sont de vraies ressources AWS facturées à l'usage, même si tout reste dans le free tier pour ce volume : à valider soi-même avant d'appliquer, jamais en automatique.
7. Vérifier l'extraction seule : `python -m src.extract.deezer_extractor` doit écrire un snapshot dans `s3://<bucket>/raw/date=.../deezer_snapshot.json`.
8. `docker compose build` (le premier build installe Java et PySpark dans l'image, ça prend une à deux minutes) puis `docker compose up -d`.
9. Dans l'UI Airflow (http://localhost:8080, admin/admin), déclencher `soundpulse_pipeline` manuellement et attendre les 4 tâches : `extract_deezer`, `staging_job`, `curated_job`, `check_freshness`. Le DAG est aussi planifié tous les jours à 6h UTC, donc le laisser tourner suffit à terme.
10. Une fois le run terminé, charger les partitions dans Athena avec `MSCK REPAIR TABLE <table>;` (une fois par nouvelle date de snapshot), puis lancer les requêtes de `sql/dashboard_queries.sql` depuis la console Athena ou la CLI.
11. Connecter Power BI Desktop à Athena pour construire le rapport (connecteur natif, driver ODBC requis), voir la section suivante.

## Connexion Power BI

Les 4 tables curated sont référencées dans Glue Data Catalog (base `soundpulse`, voir `terraform/glue_catalog.tf`) et requêtables en SQL via Athena (workgroup dédié `soundpulse`, voir `terraform/athena.tf`). Des requêtes prêtes à l'emploi sont dans `sql/dashboard_queries.sql`.

Power BI Desktop a un connecteur Athena natif, certifié Microsoft, disponible depuis 2021, mais il s'appuie sur le driver ODBC Amazon Athena : les deux sont nécessaires.

1. Installer le driver ODBC Amazon Athena (Simba), téléchargeable depuis la [documentation AWS](https://docs.aws.amazon.com/athena/latest/ug/connect-with-odbc-and-power-bi.html), version 64 bits assortie à Power BI Desktop.
2. Dans Power BI Desktop : onglet Accueil, Obtenir les données, rechercher "Athena", sélectionner Amazon Athena, Se connecter.
3. Renseigner la région AWS (`eu-west-3`), l'emplacement des résultats de requête (`s3://soundpulse-datalake-618875/athena-query-results/`, celui du workgroup `soundpulse`), et des credentials AWS en lecture seule (jamais ceux du rôle `soundpulse-pipeline-role`, réservé aux jobs).
4. Choisir le mode Import : le volume de ce projet (quelques milliers de lignes) reste largement dans les limites d'un import, DirectQuery n'apporte rien ici.
5. Charger la base `soundpulse` et ses 4 tables (`fait_popularite`, `dim_titre`, `dim_artiste`, `dim_date`).
6. Recréer les relations du modèle en étoile dans Power BI, entre `fait_popularite` et les 3 dimensions, sur `id_titre`, `id_artiste` et `date`.

La construction du rapport lui-même (visuels, mise en page, publication) reste une étape manuelle, interface graphique uniquement, hors de portée d'un agent codeur (Phase 6b ci-dessous).

## Limites connues

Le champ `bpm` n'est quasiment jamais renseigné par Deezer sur les endpoints de chart utilisés pour l'extraction : sur les 2372 titres du catalogue actuel, aucun n'a de bpm. La requête de tendance de tempo dans `sql/dashboard_queries.sql` tourne sans erreur mais ne renvoie rien tant qu'une autre source ne vient pas compléter ce champ.

Un même titre peut apparaître dans le chart de plusieurs genres. Au dédoublonnage (`staging_job.py`), le titre garde un genre arbitraire parmi ceux où il était classé, plutôt que de les combiner : ça simplifie le modèle en étoile mais peut sous-représenter un genre secondaire pour certains titres multi-genres.

L'alerte d'échec du DAG se contente d'un log structuré dans Airflow (dag, tâche, run, exception). Il n'y a pas de canal externe branché (email, Slack) : aucun credential ni canal n'est configuré pour ce projet portfolio, en simuler un aurait été une fausse promesse de fonctionnalité.

Aucun vrai job AWS Glue managé n'a été déployé : la transformation tourne en PySpark portable, testée en local et dans le conteneur Airflow, ce qui évite la facturation DPU tant que la logique n'était pas stabilisée. Un déploiement en jobs Glue managés reste à faire si le volume de données devait vraiment grandir.

Le build de l'image Airflow signale un conflit de versions entre `pandas` (utilisé par ce projet) et les bornes attendues par `apache-airflow-providers-google`/`snowflake` (qui veulent une version plus ancienne). Sans effet observé puisque ces providers ne sont pas utilisés par ce DAG, mais à surveiller si ça change.

## État d'avancement

- [x] Phase 1, cadrage : repo, structure, Terraform de base.
- [x] Phase 2, ingestion : extraction Deezer et zone raw S3.
- [x] Phase 3, transformation : jobs staging et curated.
- [x] Phase 4, orchestration : DAG Airflow complet, retry, alertes.
- [x] Phase 5, qualité et tests : contrôles automatisés, unitaires et intégration.
- [x] Phase 6a, restitution SQL : Glue Data Catalog, workgroup Athena, requêtes de `sql/dashboard_queries.sql` vérifiées sur les vraies données.
- [ ] Phase 6b, rapport Power BI construit : étape manuelle utilisateur (Power BI Desktop), hors périmètre agent.
- [x] Phase 7a, documentation : ce README, `DEMO_CHECKLIST.md`.
- [ ] Phase 7b, vidéo ou GIF de démo : étape manuelle utilisateur, voir `DEMO_CHECKLIST.md`.
