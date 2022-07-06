from __future__ import print_function

from datetime import datetime

# Common methods
from lib.slack_msg import slack_msg_body
from lib.get_dates import get_date

from airflow import models
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

# DEFINE INIT PARAMS
# Dag
dag_name = "<DAG_NAME>"
dag_tags =  ["testing", "ETL", "schedule", "pentaho", "git: legacy/bi-insight", "input: dwh", "output: dwh"]
# Schedule interal
schedule_interval = "<SCHEDULE_INTERVAL>"  # This example scheduled at daily 6 AM: "0 6 * * *"
# Slack msg
riskiness = "<RISKINESS>"  # High, Medium or Low
utility = "<UTILITY_OF_DAG>"  # Example: "This etl generates ... data in DWH."

SLACK_CONN_ID = 'slack'
sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")


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
    on_failure_callback=task_fail_slack_alert
) as dag:

    run_ssh = SSHOperator(
        task_id="example_task_id",
        ssh_hook=sshHook,
        # Example of command:
        # sh /opt/dw_schibsted/yapo_bi/dw_blocketdb/kpis_operacionales/run_jb_kpis_operacionales.sh
        command="sh /opt/dw_schibsted/yapo_bi/dw_blocketdb/kpis_operacionales/run_jb_kpis_operacionales.sh "  # You need add a space at the end of the command, to avoid error: Jinja template not found
    )

    run_ssh

