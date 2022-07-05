from __future__ import print_function

import json
from datetime import datetime, timedelta
import requests

from lib.slack_msg import slack_msg_body
#TODO incorporar get_date common

from airflow import models
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

SLACK_CONN_ID = 'slack'
sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")

# Define schedule interal
schedule_interval = '0 6 * * *' # This example scheduled at daily 6 AM
# Define slack msg params
riskiness = "Medium" # or High or Low
utility = "This etl generates ... data in DWH."

default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime(2022, 7, 5),
}

# Calculate the date
def get_date():
    start_date = "{{ dag_run.conf['start_date'] if dag_run.conf and 'start_date' in dag_run.conf else False}}"
    end_date = "{{ dag_run.conf['end_date'] if dag_run.conf and 'end_date' in dag_run.conf else False}}"
    if start_date and end_date:
        date = {"start_date": start_date, "end_date": end_date}
    else:
        execution_date = "{{ dag_run.logical_date }}"
        execution_date = execution_date - timedelta(days=1)
        execution_date = execution_date.date().strftime('%Y-%m-%d')
        date = {"start_date": execution_date, "end_date": execution_date}
    return date


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
    "trigger_bi-insight-dw_blocketdb-kpis_operacionales",
    tags=[
        "production",
        "ETL",
        "trigger",
        "core",
        "git: legacy/bi-insight",
        "input: dwh",
        "output: dwh",
    ],
    schedule_interval=schedule_interval,
    default_args=default_args,
    max_active_runs=1,
    on_failure_callback=task_fail_slack_alert
) as dag:

    run_kpi_operacionales = SSHOperator(
        task_id="run_kpi_operacionales",
        ssh_hook=sshHook,
        command="sh /opt/dw_schibsted/yapo_bi/dw_blocketdb/kpis_operacionales/run_jb_kpis_operacionales.sh "
    )

    run_kpi_operacionales

