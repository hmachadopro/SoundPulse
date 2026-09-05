-- Requêtes prêtes à l'emploi pour les questions métier du CDC §1.2
-- (repérage d'artistes émergents, tendances de genre/tempo), à copier-coller
-- dans l'éditeur Athena ou dans Power BI (import via le connecteur Athena,
-- voir README section "Connexion Power BI").
--
-- Base : `soundpulse` (Glue Data Catalog, voir terraform/glue_catalog.tf).
-- `score_popularite` = `rank` Deezer brut : plus la valeur est élevée, plus
-- le titre est populaire (aucune inversion appliquée dans le pipeline).
-- Le filtre `date = '2026-09-05'` cible le seul snapshot disponible à ce
-- jour ; à adapter (ou retirer, pour agréger tout l'historique) au fil des
-- runs quotidiens suivants.

-- 1. Popularité moyenne par genre — tendance de genre demandée au §1.2.
SELECT
    dt.genre,
    COUNT(*) AS nb_titres,
    AVG(fp.score_popularite) AS score_popularite_moyen
FROM soundpulse.fait_popularite fp
JOIN soundpulse.dim_titre dt ON dt.id_titre = fp.id_titre AND dt.date = fp.date
WHERE fp.date = '2026-09-05'
GROUP BY dt.genre
ORDER BY score_popularite_moyen DESC;

-- 2. Top 20 artistes par nombre de fans.
SELECT
    id_artiste,
    nom,
    nb_fan,
    nb_album
FROM soundpulse.dim_artiste
WHERE date = '2026-09-05'
ORDER BY nb_fan DESC
LIMIT 20;

-- 3. Distribution des bpm par genre — tendance de tempo demandée au §1.2.
-- `bpm` est souvent NULL côté Deezer (non fourni pour la majorité des
-- titres) : count(bpm) mesure la couverture réelle par genre, pas le total
-- de titres (voir requête 4 pour le total).
SELECT
    genre,
    COUNT(bpm) AS nb_titres_avec_bpm,
    AVG(bpm) AS bpm_moyen,
    MIN(bpm) AS bpm_min,
    MAX(bpm) AS bpm_max
FROM soundpulse.dim_titre
WHERE date = '2026-09-05'
GROUP BY genre
ORDER BY bpm_moyen DESC;

-- 4. Vue d'ensemble du catalogue : titres et artistes distincts par genre.
SELECT
    dt.genre,
    COUNT(DISTINCT dt.id_titre) AS nb_titres,
    COUNT(DISTINCT fp.id_artiste) AS nb_artistes
FROM soundpulse.dim_titre dt
JOIN soundpulse.fait_popularite fp ON fp.id_titre = dt.id_titre AND fp.date = dt.date
WHERE dt.date = '2026-09-05'
GROUP BY dt.genre
ORDER BY nb_titres DESC;

-- 5. Repérage d'artistes émergents — objectif central du §1.2 : des
-- artistes avec un score de popularité élevé sur leurs titres malgré une
-- base de fans encore modeste (à l'inverse des stars déjà installées,
-- visibles en requête 2). Seuil `nb_fan < 500000` arbitraire, à ajuster.
SELECT
    da.id_artiste,
    da.nom,
    da.nb_fan,
    AVG(fp.score_popularite) AS score_popularite_moyen,
    COUNT(*) AS nb_titres_classes
FROM soundpulse.fait_popularite fp
JOIN soundpulse.dim_artiste da ON da.id_artiste = fp.id_artiste AND da.date = fp.date
WHERE fp.date = '2026-09-05'
  AND da.nb_fan < 500000
GROUP BY da.id_artiste, da.nom, da.nb_fan
ORDER BY score_popularite_moyen DESC
LIMIT 20;
