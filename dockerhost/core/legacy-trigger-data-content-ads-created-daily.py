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
dag_name = "legacy_trigger_data-content_ads_created_daily"
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
docker_image = "gcr.io/data-poc-323413/legacy/core-ads-created-daily:latest"
# Schedule interal
schedule_interval = None
# Slack msg
riskiness = "Medium"
utility = "Ads created daily pipeline that aims to obtain from blocket db the ads created on the day of the process, in addition to calculating the dates of approval and elimination / rejection of ads on the same process date"
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


def set_dates(**kwargs):
    dates = get_date(**kwargs)
    kwargs["ti"].xcom_push(key="start_date", value=dates["start_date"])
    kwargs["ti"].xcom_push(key="end_date", value=dates["end_date"])
    logging.info(f"detected days: {dates}")


with models.DAG(
    dag_name,
    tags=dag_tags,
    schedule_interval=schedule_interval,
    default_args=default_args,
    max_active_runs=1,
    on_failure_callback=task_fail_slack_alert,
) as dag:

    def call_ssh(**kwargs):
        ti = kwargs["ti"]
        command_line = f"""--rm -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                            -v /home/bnbiuser/secrets/blocket_db:/app/blocket-secret \
                            -e APP_DB_SECRET=/app/blocket-secret \
                            -e APP_DW_SECRET=/app/db-secret \
                            {docker_image} \
                            -date_from={ ti.xcom_pull(task_ids="task_set_dates_ads_created_daily", key="start_date") } \
                            -date_to={ ti.xcom_pull(task_ids="task_set_dates_ads_created_daily", key="end_date") }"""
        call = ssh_operator.SSHOperator(
            task_id="task_run_ads_created_daily",
            ssh_hook=sshHook,
            command=f"""{connect_dockerhost} <<EOF \n
                        sudo docker pull {docker_image} \n
                        sudo docker run {command_line}""",
        )
        call.execute(context=kwargs)

    set_dates_ads_created_daily = python_operator.PythonOperator(
        task_id="task_set_dates_ads_created_daily",
        provide_context=True,
        python_callable=set_dates,
    )

    run_ads_created_daily = python_operator.PythonOperator(
        task_id="task_run_ads_created_daily",
        provide_context=True,
        python_callable=call_ssh,
    )

    set_dates_ads_created_daily >> run_ads_created_daily
