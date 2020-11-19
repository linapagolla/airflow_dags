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

dag = DAG('example_kubernetes_pod',
          schedule_interval='@once',
          default_args=default_args)


compute_resource = {'request_cpu': '200m', 'request_memory': '1Gi', 'limit_cpu': '200m', 'limit_memory': '1Gi'}

secret_env = secret.Secret(deploy_type='env', deploy_target='POSTGRES_DB_HOST', secret='airflow-ciox-ls-db-lfsci', key='host')
env_vars={'POSTGRES_DB_HOST': os.environ['POSTGRES_DB_HOST'],'POSTGRES_DB_PORT':os.environ['POSTGRES_DB_PORT'],'POSTGRES_DB_USER':os.environ['POSTGRES_DB_USER'],'POSTGRES_DB_PWD':os.environ['POSTGRES_DB_PWD'],'POSTGRES_DB_NAME':os.environ['POSTGRES_DB_NAME']}

with dag:
    k = KubernetesPodOperator(
        namespace=namespace,
        #image="1.10.10.1-alpha2-python3.6",
        image="apache/airflow:1.10.10.1-alpha2-python3.6",
        #image="ubuntu:16.04",
        #cmds=['pip', 'install', 'awscli', '--user'], 
        #cmds=["/bin/bash","-c","pip install awscli --user && echo $POSTGRES_DB_HOST && mkdir chasefile-export && cd chasefile-export && pwd && ls -ltr && aws s3 cp s3://ciox-195338640440-hsdp-dev-document-pipeline/airflow_code_base/chasefile-export/chasefile-export.py  /opt/airflow/chasefile-export/chasefile-export.py && ls -ltr"],
        cmds=["/bin/bash","-c","pip install awscli --user && echo $POSTGRES_DB_HOST && mkdir chasefile-export && cd chasefile-export && pwd && ls -ltr && aws s3 cp s3://ciox-195338640440-hsdp-dev-document-pipeline/airflow_code_base/chasefile-export/Initialize.sh  /opt/airflow/chasefile-export/Initialize.sh && ksh Initialize.kshr"],
        #cmds=["/bin/bash","-c","pip install awscli --user && pwd && ls -ltr && mkdir chasefile-export && cd chasefile-export && pwd && ls -ltr"],
        #arguments=["echo", "10"],
        #image_pull_secrets=["airflow-ciox-ls-db-lfsci"],
        env_vars=env_vars,
        labels={"foo": "bar"},
        name='airflow-test-pod',
        task_id='task_one',
        in_cluster=in_cluster, # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-for-desktop', # is ignored when in_cluster is set to True
        #config_file=config_file,
        resources=compute_resource,
        is_delete_pod_operator=False,
        get_logs=True)
