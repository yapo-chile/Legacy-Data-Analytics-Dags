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
dag_name = "legacy_data-content_professional_report"
dag_tags = [
    "production",
    "ETL",
    "schedule",
    "dockerhost",
    "legacy",
    "git: pending",
    "input: dwh",
    "output: dwh",
]
# Docker image
docker_image_massive_upload = "bi.professional.report.carga.masiva:1.0"
docker_image_professional_report = "bi.professional.report.professional:1.0"
# Schedule interal
schedule_interval = "0 10 * * *"
# Slack msg
riskiness = "Medium"
utility = "ETL process related to Professional Report"


sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")
connect_dockerhost = Variable.get("CONNECT_DOCKERHOST")
SLACK_CONN_ID = "slack"


default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime(2022, 7, 18),
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

    def call_ssh_massive_upload(**kwargs):
        dates = get_date(**kwargs)
        logging.info(f"detected days: {dates}")
        command_line = (
            f"""{docker_image_massive_upload} --date1={dates['start_date']}"""
        )
        logging.info(f"Executing massive upload command: {command_line}")
        call = ssh_operator.SSHOperator(
            task_id="task_massive_upload_professional_report",
            ssh_hook=sshHook,
            command=f"""{connect_dockerhost} <<EOF \n
                        sudo docker run {command_line}""",
        )
        call.execute(context=kwargs)

    def call_ssh_professional_report(**kwargs):
        dates = get_date(**kwargs)
        logging.info(f"detected days: {dates}")
        command_line = (
            f"""{docker_image_professional_report} --date1={dates['start_date']}"""
        )
        logging.info(f"Executing professional report command: {command_line}")
        call = ssh_operator.SSHOperator(
            task_id="task_professional_report",
            ssh_hook=sshHook,
            command=f"""{connect_dockerhost} <<EOF \n
                        sudo docker run {command_line}""",
        )
        call.execute(context=kwargs)

    massive_upload_professional_report = python_operator.PythonOperator(
        task_id="task_massive_upload_professional_report",
        provide_context=True,
        python_callable=call_ssh_massive_upload,
    )

    professional_report = python_operator.PythonOperator(
        task_id="task_professional_report",
        provide_context=True,
        python_callable=call_ssh_professional_report,
    )

    massive_upload_professional_report >> professional_report
