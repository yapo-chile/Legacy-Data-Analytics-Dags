from __future__ import print_function

import logging
from datetime import datetime

from airflow import models
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators import ssh_operator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models.variable import Variable
from airflow.operators import python_operator
from lib.get_dates import get_date

# Common methods
from lib.slack_msg import slack_msg_body

# DEFINE INIT PARAMS
# Dag
dag_name = "legacy_trigger_data-revenues_store-purshases"
dag_tags = [
    "production",
    "trigger",
    "ETL",
    "dockerhost",
    "legacy",
    "git: legacy/data-revenues",
    "input: pending",
    "output: pending",
]
# Docker image
docker_image = "gcr.io/data-poc-323413/legacy/store-purchase:latest"
# Schedule interal
schedule_interval = None
# Slack msg
riskiness = "Medium"
utility = "Retrieves stored purchases from blocketDb and store them in dwh"
sshHook = SSHHook(ssh_conn_id="ssh_public_aws")
connect_dockerhost = Variable.get("CONNECT_DOCKERHOST")
SLACK_CONN_ID = "slack"


default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime(2022, 7, 6),
}


def task_fail_slack_alert(context):
    slack_msg = slack_msg_body(
        context,
        riskiness=riskiness,
        utility=utility,
    )
    failed_alert = SlackWebhookOperator(
        task_id="failed_job_alert",
        http_conn_id=SLACK_CONN_ID,
        message=slack_msg,
        username="airflow",
        dag=dag,
    )
    return failed_alert.execute(context=context)


with models.DAG(
    dag_name,
    tags=dag_tags,
    schedule_interval=schedule_interval,
    default_args=default_args,
    max_active_runs=1,
    on_failure_callback=task_fail_slack_alert,
) as dag:

    def call_ssh(**kwargs):
        dates = get_date(**kwargs)
        logging.info(f"detected days: {dates}")
        command_line = f"""--rm -v /home/bnbiuser/secrets/blocket_db:/app/db-blocket \
                            -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                            -e APP_BLOCKET_SECRET=/app/db-blocket \
                            -e APP_DB_SECRET=/app/db-secret \
                            {docker_image}"""
        call = ssh_operator.SSHOperator(
            task_id="taskrun_store-purshases",
            ssh_hook=sshHook,
            command=f"""{connect_dockerhost} <<EOF \n
                        sudo docker pull {docker_image} \n
                        sudo docker run {command_line}""",
        )
        call.execute(context=kwargs)

    run_store_purshases = python_operator.PythonOperator(
        task_id="task_run_store-purshases",
        provide_context=True,
        python_callable=call_ssh,
    )

    run_store_purshases
