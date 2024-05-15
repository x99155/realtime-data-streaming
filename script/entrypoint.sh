#!/bin/bash
set -e # Cette commande fait en sorte que le script s'arrête immédiatement si une commande retourne une erreur

# Installe les dépendances spécifiées si le fichier requirements.txt est présent
if [ -e "/opt/airflow/requirements.txt" ]; then
    $(command -v pip) install --user -r requirements.txt
fi

# Initialise la base de données Airflow et crée un utilisateur administrateur si la base de données n'existe pas
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@estiam.com \
    --password admin
fi

# Met à jour la base de données vers la dernière version du schéma.
$(command -v airflow) db upgrade

# Démarre le serveur web Airflow
exec airflow webserver