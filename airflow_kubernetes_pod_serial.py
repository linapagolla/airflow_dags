from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


namespace = conf.get('kubernetes', 'NAMESPACE')

# This will detect the default namespace locally and read the 
# environment namespace when deployed to Astronomer.
if namespace =='airflow-update':
    #config_file = '/usr/local/airflow/include/.kube/config'
    in_cluster=True
else:
    in_cluster=True
    config_file=None

dag = DAG('example_kubernetes_pod_serial',
          schedule_interval='@once',
          default_args=default_args)


compute_resource = {'request_cpu': '200m', 'request_memory': '1Gi', 'limit_cpu': '200m', 'limit_memory': '1Gi'}

secret_env = secret.Secret(deploy_type='env', deploy_target='POSTGRES_DB_HOST', secret='airflow-ciox-ls-db-lfsci', key='host')
env_vars={'POSTGRES_DB_HOST': os.environ['POSTGRES_DB_HOST'],'POSTGRES_DB_PORT':os.environ['POSTGRES_DB_PORT'],'POSTGRES_DB_USER':os.environ['POSTGRES_DB_USER'],'POSTGRES_DB_PWD':os.environ['POSTGRES_DB_PWD'],'POSTGRES_DB_NAME':os.environ['POSTGRES_DB_NAME']}

with dag:
    chasefile_export_failed = KubernetesPodOperator(
        namespace=namespace,
        image="lifesciences.docker.cioxhealth.com/ciox-ls-chasefile-export:75724c36a",
        cmds=["python","./chasefile-export/chasefile-export2.py"],
        env_vars=env_vars,
        labels={"foo": "bar"},
        name='chasefile_export_failed',
        task_id='chasefile_export_failed',
        in_cluster=in_cluster, # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-for-desktop', # is ignored when in_cluster is set to True
        #config_file=config_file,
        resources=compute_resource,
        is_delete_pod_operator=False,
        get_logs=True)

    chasefile_export_passed = KubernetesPodOperator(
        namespace=namespace,
        image="lifesciences.docker.cioxhealth.com/ciox-ls-chasefile-export:75724c36",
        cmds=["python","./chasefile-export/chasefile-export.py"],
        env_vars=env_vars,
        labels={"foo": "bar"},
        name='chasefile_export_passed',
        task_id='chasefile_export_passed',
        in_cluster=in_cluster, # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-for-desktop', # is ignored when in_cluster is set to True
        #config_file=config_file,
        resources=compute_resource,
        is_delete_pod_operator=False,
        get_logs=True)
    chasefile_export_failed >> chasefile_export_passed
