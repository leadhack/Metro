import streamlit as st
import os
import subprocess

def generate_dag(dag_id, command):
    dag_template = f"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}}

dag = DAG(
    '{dag_id}',
    default_args=default_args,
    schedule_interval=None
)

task = BashOperator(
    task_id='execute_command',
    bash_command="{command}",
    dag=dag
)
"""
    return dag_template

def git_push(repo_path, commit_message):
    try:
        subprocess.run(["git", "-C", repo_path, "add", "-A"], check=True)
        subprocess.run(["git", "-C", repo_path, "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "-C", repo_path, "push"], check=True)
        return "Modifications poussées avec succès !"
    except subprocess.CalledProcessError as e:
        return f"Erreur lors du push Git : {e}"

st.title("Générateur de DAGs Airflow")

# Sélection du dossier avec validation
storage_path = st.text_input("Chemin de stockage des DAGs", "dags")
if not os.path.isdir(storage_path):
    st.warning("Le chemin spécifié n'existe pas. Veuillez le créer ou choisir un autre chemin valide.")


# Saisie utilisateur
num_dags = st.number_input("Nombre de DAGs à générer", min_value=1, value=1, step=1)
command = st.text_input("Commande à exécuter", "echo 'Hello Airflow'")
commit_message = st.text_input("Message du commit Git", "Ajout de nouveaux DAGs")

dags_content = {}
if st.button("Générer les DAGs"):
    os.makedirs(storage_path, exist_ok=True)
    for i in range(1, num_dags + 1):
        dag_id = f"generated_dag_{i}"
        dag_code = generate_dag(dag_id, command)
        dags_content[dag_id] = dag_code
        with open(f"{storage_path}/{dag_id}.py", "w") as f:
            f.write(dag_code)
    st.success(f"{num_dags} DAGs générés avec succès dans {storage_path} !")

    # Générer un fichier ZIP pour téléchargement
    #import zipfile
    #zip_filename = "generated_dags.zip"
    #with zipfile.ZipFile(zip_filename, 'w') as zipf:
    #    for dag_id, dag_code in dags_content.items():
    #        zipf.writestr(f"{dag_id}.py", dag_code)
    
    #with open(zip_filename, "rb") as f:
    #    st.download_button("Télécharger les DAGs", f, file_name=zip_filename)

# Bouton pour pousser les fichiers sur Git
if st.button("Pousser sur Git"):
    git_result = git_push(storage_path, commit_message)
    st.write(git_result)
