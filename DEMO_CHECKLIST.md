# Checklist démo SoundPulse

Guide pour enregistrer la vidéo ou le GIF de démonstration du pipeline (Phase 7b du CDC). Cette checklist ne remplace pas l'enregistrement lui-même : la capture d'écran reste une étape manuelle, à faire soi-même.

## Avant d'enregistrer

- Repo à jour, `.env` renseigné, `terraform apply` déjà passé. Pas la peine de montrer le provisionnement Terraform dans la vidéo, ça n'apporte rien visuellement et ça prend du temps.
- Docker Desktop lancé, `docker compose build` déjà fait au moins une fois (le premier build prend une à deux minutes, à éviter en plein enregistrement).
- Outil de capture prêt : l'enregistreur Windows intégré (Win+Alt+R) suffit pour une vidéo, ou un outil dédié type ShareX/OBS si tu veux découper un GIF ensuite.

## Séquence suggérée (5 à 8 minutes)

1. Terminal : `docker compose up -d`, montrer les conteneurs démarrer (`docker compose ps` une fois qu'ils sont up).
2. Navigateur : ouvrir http://localhost:8080, se connecter (admin/admin), montrer le DAG `soundpulse_pipeline` dans la liste.
3. Déclencher le DAG manuellement (bouton Trigger DAG), montrer les 4 tâches passer au vert dans la vue Graph. `extract_deezer` prend plusieurs minutes à cause du throttling sur l'API Deezer : couper le montage ou accélérer cette partie plutôt que de la montrer en entier.
4. Cliquer sur une tâche (`curated_job` par exemple) et montrer un extrait de log pertinent, comme la ligne qui affiche la complétude du contrôle qualité.
5. Console AWS Athena (ou CLI) : lancer une des requêtes de `sql/dashboard_queries.sql`, par exemple le top artistes par nombre de fans, et montrer le résultat.
6. Power BI Desktop : montrer le rapport une fois construit (Phase 6b), avec un ou deux visuels branchés sur les vraies données curated.

## Après l'enregistrement

- Recouper les temps morts, surtout l'attente de l'extraction Deezer.
- Exporter dans un format raisonnable : GIF court pour un aperçu dans le README, vidéo complète si une démo plus longue est utile ailleurs.
- Déposer le fichier dans le repo ou le lier depuis le README une fois prêt.
