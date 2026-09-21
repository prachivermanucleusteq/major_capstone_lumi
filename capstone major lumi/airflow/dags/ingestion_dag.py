from datetime import datetime
import json
import os
import subprocess
import csv
from urllib.parse import urlparse
from pathlib import Path
from uuid import UUID

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import psycopg2

BEAM_JAR = "/opt/lumi/beam/lumi-beam-pipeline.jar"

def _container_jdbc_url(jdbc_url):
    """Translate host-local PostgreSQL URLs to the Compose service hostname."""
    parsed = urlparse(jdbc_url.removeprefix("jdbc:"))
    if parsed.scheme != "postgresql" or parsed.hostname not in ("localhost", "127.0.0.1"):
        return jdbc_url
    port = parsed.port or 5432
    container_url = parsed._replace(netloc=f"postgres:{port}").geturl()
    return f"jdbc:{container_url}"

with DAG(
    dag_id="lumi_ingestion_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    validate_params = BashOperator(
        task_id="validate_params",
        bash_command="""
            set -e
            echo "=== Lumi Ingestion Run ==="
            echo "execution_id    : {{ dag_run.conf.get('execution_id', 'MISSING') }}"
            echo "input_directory : {{ dag_run.conf.get('input_directory', 'MISSING') }}"
            echo "file_format     : {{ dag_run.conf.get('file_format', 'MISSING') }}"
            echo "error_output    : {{ dag_run.conf.get('error_output', 'MISSING') }}"
            [ -n "{{ dag_run.conf.get('execution_id', '') }}" ] \
                || { echo "ERROR: execution_id missing from conf"; exit 1; }
            [ -n "{{ dag_run.conf.get('input_directory', '') }}" ] \
                || { echo "ERROR: input_directory missing from conf"; exit 1; }
            [ -n "{{ dag_run.conf.get('file_format', '') }}" ] \
                || { echo "ERROR: file_format missing from conf"; exit 1; }
            [ -n "{{ dag_run.conf.get('jdbc_url', '') }}" ] \
                || { echo "ERROR: jdbc_url missing from conf"; exit 1; }
            [ -d "{{ dag_run.conf.get('input_directory', '') }}" ] \
                || { echo "ERROR: input directory does not exist"; exit 1; }

            echo "Params OK"
        """,
    )

    def _detect_file_type(**context):
        input_directory = context["dag_run"].conf.get("input_directory", "")
        file_format = context["dag_run"].conf.get("file_format", "").lower()

        if not input_directory:
            raise ValueError("input_directory is missing")

        if not os.path.isdir(input_directory):
            raise FileNotFoundError(f"Input directory does not exist: {input_directory}")

        if file_format not in ("json", "csv"):
            raise ValueError(f"Unsupported file format: {file_format} Supported formats are JSON and CSV." )
        extension = f".{file_format}"
        files = sorted(file for file in Path(input_directory).iterdir()
        if file.is_file() and file.suffix.lower() == extension)
        if not files:
            raise FileNotFoundError( f"No {file_format.upper()} files found in: {input_directory}" )
        empty_files = [file for file in files if file.stat().st_size == 0]
        if empty_files:
            raise ValueError(f"Empty {file_format.upper()} input file(s): " + ", ".join(str(file) for file in empty_files))
        print("=== Input Files ===")
        print(f"Directory : {input_directory}")
        print(f"Format    : {file_format.upper()}")
        print(f"File count: {len(files)}")

        for file in files:
            print(f"File      : {file}")
            print(f"Size      : {file.stat().st_size} bytes")

        print(f"Detected file type: {file_format.upper()}")
        return f"prepare_{file_format}"

    detect_file_type = BranchPythonOperator(
        task_id="detect_file_type",
        python_callable=_detect_file_type,
    )

    prepare_json = BashOperator(
        task_id="prepare_json",
        bash_command="""
            set -e

            echo "=== JSON Processing Steps ==="
            echo "JSON files will be processed by the Beam pipeline."
            echo "Execution ID: {{ dag_run.conf.get('execution_id') }}"
            echo "Input directory: {{ dag_run.conf.get('input_directory') }}"

            echo "JSON preparation completed."
        """,
    )

    prepare_csv = BashOperator(
        task_id="prepare_csv",
        bash_command="""
            set -e

            echo "=== CSV Processing Steps ==="
            echo "CSV files will be processed by the Beam pipeline."
            echo "Execution ID: {{ dag_run.conf.get('execution_id') }}"
            echo "Input directory: {{ dag_run.conf.get('input_directory') }}"

            echo "CSV preparation completed."
        """,
    )

    def _run_beam_pipeline(**context):
        dag_run = context["dag_run"]
        conf = dag_run.conf

        execution_id = conf.get("execution_id")
        input_directory = conf.get("input_directory")
        file_format = conf.get("file_format", "").lower()
        jdbc_url = _container_jdbc_url(conf.get("jdbc_url"))
        jdbc_username = conf.get("jdbc_username")
        jdbc_password = conf.get("jdbc_password")
        error_output_base = conf.get("error_output")

        if not execution_id:
            raise ValueError("execution_id is missing")

        if not input_directory:
            raise ValueError("input_directory is missing")

        if file_format not in ("json", "csv"):
            raise ValueError(f"Unsupported file format: {file_format}")

        if not os.path.isdir(input_directory):
            raise FileNotFoundError(
                f"Input directory does not exist: {input_directory}"
            )

        extension = f".{file_format}"

        input_files = sorted(
            file for file in Path(input_directory).iterdir()
            if file.is_file() and file.suffix.lower() == extension
        )

        if not input_files:
            raise FileNotFoundError(
                f"No {file_format.upper()} files found in {input_directory}"
            )

        encryption_key = os.environ.get("LUMI_ENCRYPTION_KEY", "")

        if len(encryption_key.encode("utf-8")) != 32:
            raise ValueError(
                "LUMI_ENCRYPTION_KEY must be exactly 32 bytes"
            )

        print("=== Starting Beam Processing ===")
        print(f"Execution ID : {execution_id}")
        print(f"Input directory: {input_directory}")
        print(f"File format  : {file_format.upper()}")
        print(f"File count   : {len(input_files)}")

        for index, input_file in enumerate(input_files, start=1):

            file_name = input_file.name
            file_stem = input_file.stem

            file_error_output = (
                f"{error_output_base}_{file_stem}"
            )

            print("")
            print("=== Processing File ===")
            print(f"File {index}/{len(input_files)}")
            print(f"Input file   : {input_file}")
            print(f"Execution ID : {execution_id}")
            print(f"Error output : {file_error_output}")

            command = [
                "java",
                "-jar",
                BEAM_JAR,
                "--runner=DirectRunner",
                f"--inputFile={input_file}",
                f"--executionId={execution_id}",
                f"--jdbcUrl={jdbc_url}",
                f"--jdbcUsername={jdbc_username}",
                f"--jdbcPassword={jdbc_password}",
                f"--errorOutput={file_error_output}",
                f"--encryptionKey={encryption_key}",
            ]

            result = subprocess.run(
                command,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=os.environ.copy(),
            )

            if result.stdout:
                print("=== Beam Output ===")
                print(result.stdout.rstrip())

            if result.returncode != 0:
                raise RuntimeError(
                    f"Beam pipeline failed for file {file_name} "
                    f"with exit code {result.returncode}"
                )

            print(
                f"Beam processing completed successfully: {file_name}"
            )

        print("")
        print("=== All Files Processed Successfully ===")
        print(f"Execution ID: {execution_id}")
        print(f"Files processed: {len(input_files)}")

    run_beam_pipeline = PythonOperator(
        task_id="run_beam_pipeline",
        python_callable=_run_beam_pipeline,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    def _verify_record_count(**context):
        """Verify valid records against existing and newly inserted rows."""
        conf = context["dag_run"].conf
        execution_id = conf["execution_id"]
        input_directory = Path(conf["input_directory"])
        file_format = conf["file_format"].lower()
        error_output_base = Path(conf["error_output"])

        input_files = sorted(
            file for file in input_directory.iterdir()
            if file.is_file() and file.suffix.lower() == f".{file_format}"
        )

        total_input_records = 0
        employee_ids = set()
        for input_file in input_files:
            if file_format == "json":
                with input_file.open(encoding="utf-8") as source:
                    document = json.load(source)
                records = document if isinstance(document, list) else [document]
                total_input_records += len(records)
                for record in records:
                    try:
                        employee_ids.add(str(UUID(record.get("employee_id", ""))))
                    except (ValueError, TypeError):
                        pass
            else:
                with input_file.open(encoding="utf-8") as source:
                    rows = list(csv.DictReader(source))
                total_input_records += len(rows)
                for row in rows:
                    try:
                        employee_ids.add(str(UUID(row.get("employee_id", "").strip())))
                    except (ValueError, TypeError):
                        pass

        error_files = sorted(error_output_base.parent.glob(f"{error_output_base.name}_*.txt"))
        total_error_records = sum(
            sum(1 for line in error_file.open(encoding="utf-8") if line.strip())
            for error_file in error_files
        )
        expected_records = total_input_records - total_error_records

        jdbc_url = _container_jdbc_url(conf["jdbc_url"])
        parsed_url = urlparse(jdbc_url.removeprefix("jdbc:"))
        if parsed_url.scheme != "postgresql" or not parsed_url.hostname:
            raise ValueError(f"Unsupported PostgreSQL JDBC URL: {jdbc_url}")

        with psycopg2.connect(
            host=parsed_url.hostname,
            port=parsed_url.port or 5432,
            dbname=parsed_url.path.lstrip("/"),
            user=conf["jdbc_username"],
            password=conf["jdbc_password"],
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM employee WHERE employee_id = ANY(%s::uuid[])",
                    (list(employee_ids),),
                )
                matching_database_records = cursor.fetchone()[0]
                cursor.execute(
                    "SELECT COUNT(*) FROM employee WHERE execution_id = %s",
                    (execution_id,),
                )
                new_records = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM employee")
                total_database_records = cursor.fetchone()[0]

        already_existing_records = matching_database_records - new_records
        accounted_records = already_existing_records + new_records

        print("=== Record Count Verification ===")
        print(f"Input records          : {total_input_records}")
        print(f"Error records          : {total_error_records}")
        print(f"Valid input records    : {expected_records}")
        print(f"Already existing       : {already_existing_records}")
        print(f"New records inserted   : {new_records}")
        print(f"Total database records : {total_database_records}")

        if accounted_records != expected_records:
            raise RuntimeError(
                f"Record count mismatch for execution {execution_id}: "
                f"expected {expected_records} valid records, "f"accounted for {accounted_records}"
            )

        print("Record count verification passed.")

    verify_record_count = PythonOperator(
        task_id="verify_record_count",
        python_callable=_verify_record_count,
    )

    check_errors = BashOperator(
        task_id="check_errors",
        bash_command="""
            set -e
            ERROR_BASE="{{ dag_run.conf.get('error_output') }}"
            echo "=== Checking Error Files ==="
            echo "Error base: ${ERROR_BASE}"
            ERROR_FILES=$(find "$(dirname "${ERROR_BASE}")" \
                -type f \
                -name "$(basename "${ERROR_BASE}")_*.txt" \
                2>/dev/null || true)
            if [ -n "$ERROR_FILES" ]; then
                TOTAL_ERRORS=0
                while IFS= read -r ERROR_FILE; do
                    if [ -f "$ERROR_FILE" ]; then
                        COUNT=$(wc -l < "$ERROR_FILE")
                        echo "Error file: $ERROR_FILE"
                        echo "Error records: $COUNT"
                        TOTAL_ERRORS=$((TOTAL_ERRORS + COUNT))
                        if [ "$COUNT" -gt 0 ]; then
                            echo "--- First 5 errors from $ERROR_FILE ---"
                            head -n 5 "$ERROR_FILE"
                        fi
                    fi
                done <<< "$ERROR_FILES"
                echo "Total error records: $TOTAL_ERRORS"
            else
                echo "No error files found. All records processed successfully."
            fi
        """,
    )

    notify_completion = BashOperator(
        task_id="notify_completion",
        bash_command="""
            echo "=== Ingestion Run Summary ==="
            echo "execution_id    : {{ dag_run.conf.get('execution_id') }}"
            echo "input_directory : {{ dag_run.conf.get('input_directory') }}"
            echo "file_format     : {{ dag_run.conf.get('file_format') }}"
            echo "finished_at     : $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
            echo "dag_run_state   : {{ dag_run.get_state() }}"
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    validate_params >> detect_file_type
    detect_file_type >> prepare_json
    detect_file_type >> prepare_csv
    prepare_json >> run_beam_pipeline
    prepare_csv >> run_beam_pipeline
    run_beam_pipeline >> verify_record_count >> check_errors >> notify_completion