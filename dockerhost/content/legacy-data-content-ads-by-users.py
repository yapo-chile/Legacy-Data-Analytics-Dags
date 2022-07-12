from __future__ import print_function

import logging
from datetime import datetime

# Common methods
from lib.slack_msg import slack_msg_body
from lib.get_dates import get_date

from airflow import models
from airflow.contrib.operators import ssh_operator
from airflow.operators import python_operator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.models.variable import Variable
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

# DEFINE INIT PARAMS
# Dag
dag_name = "legacy_data-content_ads-by-users"
dag_tags = [
    "production",
    "ETL",
    "schedule",
    "dockerhost",
    "legacy",
    "git: legacy/data-content",
    "input: dwh",
    "output: dwh",
    "output: statistics"]
# Docker image
docker_image = "registry.gitlab.com/yapo_team/legacy/data-analytics/data-content:333df124_ads-by-user"
# Schedule interal
schedule_interval = "30 5 * * *"
# Slack msg
riskiness = "High"
utility = "Ads by user Etl processes params from verticals as it would be cars, inmo and big sellers, " \
          "then store them in datawarehouse as appended method so a historical data would be always " \
          "available."

sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")
connect_dockerhost = Variable.get("CONNECT_DOCKERHOST")
SLACK_CONN_ID = "slack"


default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime(2022, 7, 13),
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

    def call_ssh(**kwargs):
        dates = get_date(**kwargs)
        logging.info(f"detected days: {dates}")
        command_line = f"""--rm -v /home/bnbiuser/secrets/stats_db:/app/db-stats \
                -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                -e APP_STATISTICS_SECRET=/app/db-stats \
                -e APP_DB_SECRET=/app/db-secret \
                {docker_image} \
                -start_date={dates['start_date']} \
                -end_date={dates['end_date']}"""
        call = ssh_operator.SSHOperator(
            task_id="task_run_content_ads_by_users",
            ssh_hook=sshHook,
            command=f"""{connect_dockerhost} <<EOF \n
                        sudo docker pull {docker_image} \n
                        sudo docker run {command_line}"""
        )
        call.execute(context=kwargs)


    run_content_ads_by_users = python_operator.PythonOperator(
        task_id="task_run_content_ads_by_users",
        provide_context=True,
        python_callable=call_ssh
    )

    run_content_ads_by_users
