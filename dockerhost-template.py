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
dag_name = "<DAG_NAME>"
dag_tags = [
    "testing",
    "ETL",
    "schedule",
    "dockerhost",
    "git: legacy/data-content",
    "input: dwh",
    "output: dwh",
]
# Docker image
docker_image = "<DOCKER_IMAGE>"
# Schedule interal
schedule_interval = (
    "<SCHEDULE_INTERVAL>"  # This example scheduled at daily 6 AM: "0 6 * * *"
)
# Slack msg
riskiness = "<RISKINESS>"  # High, Medium or Low
utility = "<UTILITY_OF_DAG>"  # Example: "This etl generates ... data in DWH."

sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")
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
        command_line = "<COMMAND_LINE>"
        # An example of COMMAND_LINE
        # command_line = f"""-v /home/bnbiuser/secrets/dw_db:/app/db-secret \
        #                    -e APP_DB_SECRET=/app/db-secret \
        #                    {docker_image} \
        #                    -date_from={dates["start_date"]} \
        #                    -date_to={dates["end_date"]}"""
        call = ssh_operator.SSHOperator(
            task_id="example_task_id",
            ssh_hook=sshHook,
            command=f"""{connect_dockerhost} <<EOF \n
                        sudo docker pull {docker_image} \n
                        sudo docker run {command_line}""",
        )
        call.execute(context=kwargs)

    run_ssh = python_operator.PythonOperator(
        task_id="example_task_id", provide_context=True, python_callable=call_ssh
    )

    run_ssh
