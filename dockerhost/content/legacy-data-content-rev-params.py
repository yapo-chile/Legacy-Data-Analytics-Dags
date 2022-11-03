from __future__ import print_function

import logging
from datetime import datetime

from airflow import models
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators import ssh_operator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models.variable import Variable
from airflow.operators import python_operator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from lib.get_dates import get_date

# Common methods
from lib.slack_msg import slack_msg_body

# DEFINE INIT PARAMS
# Dag
dag_name = "legacy_data-content_rev_params"
dag_tags = [
    "production",
    "ETL",
    "schedule",
    "dockerhost",
    "legacy",
    "git: legacy/data-content",
    "input: dwh",
    "input: blocket" "output: dwh",
]
# Docker image
docker_image = "gcr.io/data-poc-323413/legacy/rev-params:latest"
# Schedule interal
schedule_interval = "10 4 * * *"
# Slack msg
riskiness = "High"
utility = "Legacy etl related to Rev Params"

sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")
connect_dockerhost = Variable.get("CONNECT_DOCKERHOST")
SLACK_CONN_ID = "slack"


default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime(2022, 7, 14),
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
        kwargs["ti"].xcom_push(key="start_date", value=dates["start_date"])
        kwargs["ti"].xcom_push(key="end_date", value=dates["end_date"])
        logging.info(f"detected days: {dates}")
        command_line = f"""--rm -v /home/bnbiuser/secrets/blocket_db_claro:/app/db-blocket \
                -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                -e APP_BLOCKET_SECRET=/app/db-blocket \
                -e APP_DB_SECRET=/app/db-secret \
                {docker_image} \
                -date_from={dates['start_date']} \
                -date_to={dates['end_date']}"""
        call = ssh_operator.SSHOperator(
            task_id="task_run_content_rev_params",
            ssh_hook=sshHook,
            command=f"""{connect_dockerhost} <<EOF \n
                        sudo docker pull {docker_image} \n
                        sudo docker run {command_line}""",
        )
        call.execute(context=kwargs)

    content_rev_params = python_operator.PythonOperator(
        task_id="task_run_content_rev_params",
        provide_context=True,
        python_callable=call_ssh,
    )
    trigger_dag_legacy_trigger_sent_email_impio_sample = TriggerDagRunOperator(
        task_id="task_trigger_dag_legacy_trigger_sent_email_impio_sample",
        trigger_dag_id="legacy_trigger_bi-insight-dw-blocketdb_sent_email_impio_sample",
        wait_for_completion=True,
        conf={
            "start_date": '{{ti.xcom_pull(task_ids="task_run_content_rev_params", key="start_date")}}',
            "end_date": '{{ti.xcom_pull(task_ids="task_run_content_rev_params", key="end_date")}}',
        },
        do_xcom_push=False,
    )

    content_rev_params >> trigger_dag_legacy_trigger_sent_email_impio_sample
