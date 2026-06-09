# TP 2B – Pipeline Open-Meteo --> transformation --> PostgreSQL

## Objectif

Ce projet met en place un pipeline Airflow complet qui :

1. récupère des données météo depuis l'API Open-Meteo,
2. transforme les réponses JSON en un format exploitable,
3. charge le résultat dans une base PostgreSQL,
4. enregistre une ligne de suivi dans une table d'audit.

L'architecture est construite autour de tâches bien séparées et d'une configuration paramétrable.

## Fichiers principaux

- `dags/meteo_postgres_pipeline.py` : DAG principal pour TP 2B.
- `sql/create_postgres_tables.sql` : script SQL de création des tables PostgreSQL.
- `dags/mon_premier_dag.py` : ancien DAG TP 2A, conservé comme référence.
- `data/` : dossier de travail utilisé pour les fichiers JSON et CSV produits.

## Architecture du pipeline

Le DAG s'articule autour de cinq étapes distinctes :

1. `extract_data` : appel de l'API Open-Meteo pour plusieurs villes,
2. `transform_data` : construction d'une table plate (une ligne par ville/date),
3. `ensure_postgres_tables` : création des tables cibles si nécessaire,
4. `load_data_to_postgres` : chargement effectif dans PostgreSQL,
5. `track_ingestion` : insertion d'une ligne de suivi dans la table d'audit.

Le flux d'exécution est le suivant :

```text
extract_data --> transform_data --> ensure_postgres_tables --> load_data_to_postgres --> track_ingestion
```

## Données retenues

Le pipeline extrait ces champs métiers depuis l'API Open-Meteo :

- `city` : nom de la ville,
- `date` : date de la prévision,
- `max_temperature_c` : température maximale journalière,
- `min_temperature_c` : température minimale journalière,
- `precipitation_mm` : quantité de précipitations journalières,
- `timezone` : fuseau horaire associé.

### Pourquoi ces champs ?

Ils constituent une base simple et cohérente pour une table analytique météo. Ils permettent de :

- comparer plusieurs villes,
- suivre l'évolution des températures,
- repérer les épisodes de pluie,
- charger des données dans un entrepôt ou un outil de BI.

## Tables PostgreSQL

Le projet utilise deux tables :

1. `weather_forecast`
   - stocke les mesures météo par ville et par date,
   - met à jour les données en cas de doublon (`ON CONFLICT`).

2. `weather_ingestion_audit`
   - conserve une ligne de suivi pour chaque exécution du DAG,
   - enregistre le nombre de villes traitées, le nombre de lignes insérées et le statut.

Le script de création est fourni dans `sql/create_postgres_tables.sql`.

## Paramétrage du DAG

Le DAG est paramétrable via :

- un `postgres_conn_id` Airflow (par défaut `open_meteo_postgres`),
- la liste des villes à traiter,
- les dates de début et de fin,
- les noms de tables PostgreSQL.

Ces paramètres peuvent être passés depuis `dag_run.conf` ou conservés par défaut.

## Instructions d'installation

### 1. Dépendances

Ce pipeline nécessite :

- Airflow 3.x,
- `apache-airflow-providers-postgres`,
- un connecteur PostgreSQL compatible (`psycopg` ou `psycopg2`).

Dans le virtualenv du projet, installez :

```bash
pip install -r requirements.txt
```

Le fichier `requirements.txt` contient les dépendances nécessaires au projet.

### 2. Configuration Airflow

Créez une connexion Airflow PostgreSQL nommée `open_meteo_postgres` avec les informations suivantes :

- hôte,
- port,
- base de données,
- utilisateur,
- mot de passe.

### 3. Création des tables

Exécutez le script SQL sur la base PostgreSQL :

```bash
psql -h <host> -U <user> -d <database> -f sql/create_postgres_tables.sql
```

### 4. Déploiement du DAG

Placez `dags/meteo_postgres_pipeline.py` dans le dossier `dags/`. Airflow doit détecter automatiquement le DAG.

## Exécution

Démarrez Airflow puis utilisez l'interface web :

```bash
airflow standalone
```

Ouvrez ensuite :

```text
http://localhost:8080
```

Activez le DAG `meteo_pipeline_postgres` et déclenchez une exécution manuelle.

## Vérification

Après exécution, vérifiez :

- la table `weather_forecast` contient les données météo,
- la table `weather_ingestion_audit` contient une ligne de suivi,
- aucune erreur n'est apparue dans les logs des tâches.

## Exemple de structure cible

| city | date | max_temperature_c | min_temperature_c | precipitation_mm | timezone | ingestion_ts |
|------|------|-------------------|-------------------|------------------|----------|--------------|

Chaque ligne représente une prévision météo pour une ville à une date donnée.

## Preuve de chargement

La table `weather_ingestion_audit` contient le suivi de chaque exécution :

- `run_id`,
- `execution_date`,
- `city_count`,
- `row_count`,
- `status`,
- `message`.

Ce mécanisme garantit qu'une ligne de suivi est bien écrite pour chaque run.

## Remarques

- Le code sépare clairement récupération, transformation et chargement.
- Le DAG est paramétrable et extensible à de nouvelles villes ou tables.
- Le pipeline est conçu pour être facilement maintenable et compréhensible sans consulter le code.
