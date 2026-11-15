
from datetime import datetime, timedelta
import base64

import pendulum

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

ROOT = r"C:\Users\sambit\Desktop\de microproject"

PYTHON = ROOT + r"\.venv\Scripts\python.exe"
DBT = ROOT + r"\.venv\Scripts\dbt.exe"

JAVA_HOME = (
    r"C:\Program Files\Eclipse Adoptium"
    r"\jdk-17.0.17.10-hotspot"
)

HADOOP_HOME = ROOT + r"\hadoop"

SSH_CONNECTION = "windows_ssh"


# --------------------------------------------------
# POWERSHELL COMMAND GENERATOR
# Encoded commands avoid SSH quoting problems.
# --------------------------------------------------

def powershell_command(command):
    script = f"""
$ErrorActionPreference = 'Stop'

$ROOT = '{ROOT}'
$PYTHON = '{PYTHON}'
$DBT = '{DBT}'

$env:JAVA_HOME = '{JAVA_HOME}'
$env:HADOOP_HOME = '{HADOOP_HOME}'
$env:VIRTUAL_ENV = "$ROOT\\.venv"

$env:PATH = "$env:HADOOP_HOME\\bin;$env:JAVA_HOME\\bin;$env:PATH"

$env:PYSPARK_PYTHON = $PYTHON
$env:PYSPARK_DRIVER_PYTHON = $PYTHON

Set-Location -LiteralPath $ROOT

{command}
"""

    encoded = base64.b64encode(
        script.encode("utf-16le")
    ).decode("ascii")

    return (
        "powershell.exe -NoProfile -NonInteractive "
        "-ExecutionPolicy Bypass "
        f"-EncodedCommand {encoded}"
    )


# --------------------------------------------------
# TASK GENERATORS
# --------------------------------------------------

def python_task(task_id, script, extra_command=""):

    script_path = ROOT + "\\" + script

    command = f"""
$SCRIPT = '{script_path}'

if (!(Test-Path -LiteralPath $SCRIPT)) {{
    throw "Script not found: $SCRIPT"
}}

Write-Output "Running $SCRIPT"

& $PYTHON $SCRIPT

if ($LASTEXITCODE -ne 0) {{
    exit $LASTEXITCODE
}}

{extra_command}
"""

    return SSHOperator(
        task_id=task_id,
        ssh_conn_id=SSH_CONNECTION,
        command=powershell_command(command),
        cmd_timeout=7200,
        conn_timeout=30,
        get_pty=True,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=2),
    )


# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------

with DAG(
    dag_id="ecommerce_data_engineering_pipeline",
    description="Ecommerce batch and Kafka streaming pipeline",
    start_date=pendulum.datetime(
        2026, 11, 16, tz="Asia/Kolkata"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "spark", "delta", "kafka", "dbt"],
) as dag:

    # ----------------------------------------------
    # 1. DATA VALIDATION AND BATCH INGESTION
    # ----------------------------------------------

    check_prepared_data = python_task(
        "check_prepared_data",
        r"scripts\check_prepared_data.py",
    )

    batch_ingestion = python_task(
        "batch_ingestion",
        r"ingestion\batch_ingestion.py",
        extra_command=r"""
$expected = @(
    "$ROOT\data\ingested\prepared_clickstream.parquet",
    "$ROOT\data\ingested\prepared_customer.parquet",
    "$ROOT\data\ingested\prepared_product.parquet"
)

foreach ($path in $expected) {
    if (!(Test-Path -LiteralPath $path)) {
        throw "Expected ingestion output missing: $path"
    }
}
Write-Output "All ingestion output files exist."
""",
    )

    verify_ingestion = python_task(
        "verify_ingestion",
        r"ingestion\verify_ingestion.py",
    )

    # ----------------------------------------------
    # 2. BRONZE LAYER
    # ----------------------------------------------

    bronze_delta = python_task(
        "bronze_delta",
        r"scripts\bronze_delta.py",
        extra_command=r"""
$expected = @(
    "$ROOT\data\delta\bronze\bronze_clickstream\_delta_log",
    "$ROOT\data\delta\bronze\bronze_customers\_delta_log",
    "$ROOT\data\delta\bronze\bronze_products\_delta_log"
)

foreach ($path in $expected) {
    if (!(Test-Path -LiteralPath $path)) {
        throw "Bronze Delta table missing: $path"
    }
}
Write-Output "Bronze Delta tables verified."
""",
    )

    # ----------------------------------------------
    # 3. SILVER LAYER
    # ----------------------------------------------

    silver_batch = python_task(
        "silver_batch",
        r"scripts\silver_batch.py",
        extra_command=r"""
$path = "$ROOT\data\delta\silver\silver_clickstream_batch\_delta_log"

if (!(Test-Path -LiteralPath $path)) {
    throw "Silver batch Delta table missing."
}
""",
    )

    silver_dimensions = python_task(
        "silver_dimensions",
        r"scripts\silver_dimensions.py",
        extra_command=r"""
$expected = @(
    "$ROOT\data\delta\silver\silver_products\_delta_log",
    "$ROOT\data\delta\silver\silver_customers\_delta_log"
)

foreach ($path in $expected) {
    if (!(Test-Path -LiteralPath $path)) {
        throw "Silver dimension table missing: $path"
    }
}
""",
    )

    # ----------------------------------------------
    # 4. GOLD ANALYTICS
    # ----------------------------------------------

    gold_analytics = python_task(
        "gold_analytics",
        r"scripts\gold_analytics.py",
        extra_command=r"""
$expected = @(
    "$ROOT\data\delta\gold\gold_event_summary\_delta_log",
    "$ROOT\data\delta\gold\gold_category_cart_analysis\_delta_log",
    "$ROOT\data\delta\gold\gold_booking_analysis\_delta_log",
    "$ROOT\data\delta\gold\gold_session_summary\_delta_log"
)

foreach ($path in $expected) {
    if (!(Test-Path -LiteralPath $path)) {
        throw "Gold Delta table missing: $path"
    }
}
""",
    )

    verify_gold = python_task(
        "verify_gold",
        r"scripts\verify_gold.py",
    )

    # ----------------------------------------------
    # 5. DBT TRANSFORMATIONS AND TESTS
    # ----------------------------------------------

    dbt_build = SSHOperator(
        task_id="dbt_build",
        ssh_conn_id=SSH_CONNECTION,
        command=powershell_command(r"""
$PROJECT = "$ROOT\ecommerce_analytics"

Set-Location -LiteralPath $PROJECT

& $DBT build --project-dir $PROJECT

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Output "dbt build completed successfully."
"""),
        cmd_timeout=7200,
        conn_timeout=30,
        get_pty=True,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=2),
    )

    # ----------------------------------------------
    # 6. KAFKA STREAMING BRANCH
    # ----------------------------------------------

    kafka_producer = python_task(
        "kafka_producer",
        r"scripts\kafka_producer.py",
    )

    spark_to_silver = python_task(
        "spark_to_silver",
        r"scripts\spark_to_silver.py",
    )

    # ----------------------------------------------
    # DEPENDENCIES
    # ----------------------------------------------

    check_prepared_data >> batch_ingestion
    batch_ingestion >> verify_ingestion
    verify_ingestion >> bronze_delta

    bronze_delta >> [
        silver_batch,
        silver_dimensions,
    ]

    [silver_batch, silver_dimensions] >> gold_analytics

    gold_analytics >> verify_gold >> dbt_build

    # Separate Kafka streaming branch
    batch_ingestion >> kafka_producer >> spark_to_silver
