# Étend l'image officielle Airflow avec un JRE (pour PySpark) et les
# dépendances du projet — l'image de base n'a ni Java ni PySpark, donc
# staging_job.py/curated_job.py ne pourraient pas y tourner tels quels.
FROM apache/airflow:2.10.4-python3.12

# Un JRE headless suffit pour exécuter PySpark 4.2.0 (pas de JDK complet,
# pas besoin d'outils graphiques) : l'environnement Linux du conteneur est
# plus simple que le poste Windows local (Prompt 2), pas de winutils/HADOOP_HOME
# à gérer ici, Spark local sous Linux n'en a pas besoin.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Les dépendances Python du projet (pyspark, boto3, requests...) s'installent
# sous l'utilisateur non-root `airflow`, comme le reste des paquets Airflow.
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
