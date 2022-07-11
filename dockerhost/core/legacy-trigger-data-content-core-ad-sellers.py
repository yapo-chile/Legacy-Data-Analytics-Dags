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
dag_name = "legacy_trigger_data-content_ad_sellers"
dag_tags = [
    "production",
    "trigger",
    "ETL",
    "dockerhost",
    "legacy",
    "git: legacy/data-content",
    "input: pending",
    "output: pending",
]
# Docker image
docker_image = "gcr.io/data-poc-323413/legacy/core-ad-sellers:latest"
# Schedule interal
schedule_interval = None
# Slack msg
riskiness = "Medium"
utility = "Daily this pipeline makes available into DWH the data related to sellers of ours ads whatever its classification private or professional as well as a detail available related to sellers pro in order to know in which categories a seller is professional"

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
        command_line = f"""--rm --net=host \
                            -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                            -v /home/bnbiuser/secrets/blocket_db:/app/blocket-secret \
                            -v /home/bnbiuser/secrets/smtp:/app/smtp-secret \
                            -e APP_DB_SECRET=/app/blocket-secret \
                            -e APP_DW_SECRET=/app/db-secret \
                            -e APP_SMTP_SECRET=/app/smtp-secret \
                            {docker_image} \
                            -email_from='noreply@yapo.cl' \
                            -email_to='gp_data_analytics@yapo.cl'"""
        call = ssh_operator.SSHOperator(
            task_id="task_run_add_sellers",
            ssh_hook=sshHook,
            command=f"""{connect_dockerhost} <<EOF \n
                        sudo docker pull {docker_image} \n
                        sudo docker run {command_line}""",
        )
        call.execute(context=kwargs)

    run_add_sellers = python_operator.PythonOperator(
        task_id="task_run_add_sellers", provide_context=True, python_callable=call_ssh
    )

    run_add_sellers
