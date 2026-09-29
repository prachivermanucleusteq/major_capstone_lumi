from datetime import datetime
import json
import os
import subprocess
from urllib.parse import urlparse
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

BEAM_JAR = os.environ.get("LUMI_BEAM_JAR", "/opt/lumi/beam/lumi-beam-pipeline.jar")

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
            echo "execution_id : {{ dag_run.conf.get('execution_id', 'MISSING') }}"
            echo "file_format  : {{ dag_run.conf.get('file_format', 'MISSING') }}"
            echo "error_output : {{ dag_run.conf.get('error_output', 'MISSING') }}"
            echo "input_files  : {{ dag_run.conf.get('input_files', 'MISSING') }}"
            [ -n "{{ dag_run.conf.get('execution_id', '') }}" ] \
                || { echo "ERROR: execution_id missing from conf"; exit 1; }
            [ -n "{{ dag_run.conf.get('file_format', '') }}" ] \
                || { echo "ERROR: file_format missing from conf"; exit 1; }
            [ -n "{{ dag_run.conf.get('jdbc_url', '') }}" ] \
                || { echo "ERROR: jdbc_url missing from conf"; exit 1; }
            [ -n "{{ dag_run.conf.get('input_files', '') }}" ] \
                || { echo "ERROR: input_files missing from conf"; exit 1; }
            echo "Params OK"
        """,
    )

    def _detect_file_type(**context):
        input_files = context["dag_run"].conf.get("input_files", [])
        file_format = context["dag_run"].conf.get("file_format", "").lower()

        if not input_files:
            raise ValueError("input_files is missing or empty")

        if file_format not in ("json", "csv"):
            raise ValueError(f"Unsupported file format: {file_format}. Supported formats are JSON and CSV.")

        files = [Path(f) for f in input_files]

        missing = [f for f in files if not f.is_file()]
        if missing:
            raise FileNotFoundError("Input file(s) not found: " + ", ".join(str(f) for f in missing))

        empty = [f for f in files if f.stat().st_size == 0]
        if empty:
            raise ValueError("Empty input file(s): " + ", ".join(str(f) for f in empty))

        print("=== Input Files ===")
        print(f"Format     : {file_format.upper()}")
        print(f"File count : {len(files)}")
        for f in files:
            print(f"File       : {f}")
            print(f"Size       : {f.stat().st_size} bytes")
        return f"prepare_{file_format}"

    detect_file_type = BranchPythonOperator( task_id="detect_file_type", python_callable=_detect_file_type,)

    prepare_json = BashOperator(
        task_id="prepare_json",
        bash_command="""
            set -e
            echo "=== JSON Processing Steps ==="
            echo "Execution ID : {{ dag_run.conf.get('execution_id') }}"
            echo "Input files  : {{ dag_run.conf.get('input_files') }}"
            echo "JSON preparation completed."
        """,
    )

    prepare_csv = BashOperator(
        task_id="prepare_csv",
        bash_command="""
            set -e
            echo "=== CSV Processing Steps ==="
            echo "Execution ID : {{ dag_run.conf.get('execution_id') }}"
            echo "Input files  : {{ dag_run.conf.get('input_files') }}"
            echo "CSV preparation completed."
        """,
    )

    def _run_beam_pipeline(**context):
        conf = context["dag_run"].conf

        execution_id = conf.get("execution_id")
        input_files = conf.get("input_files", [])
        jdbc_url = _container_jdbc_url(conf.get("jdbc_url"))
        jdbc_username = conf.get("jdbc_username")
        jdbc_password = conf.get("jdbc_password")
        error_output_base = conf.get("error_output")
        source_creation_time = conf.get("source_creation_time")

        if not execution_id:
            raise ValueError("execution_id is missing")
        if not input_files:
            raise ValueError("input_files is missing or empty")
        if not source_creation_time:
            raise ValueError("source_creation_time is missing")

        encryption_key = os.environ.get("LUMI_ENCRYPTION_KEY", "")
        if len(encryption_key.encode("utf-8")) != 32:
            raise ValueError("LUMI_ENCRYPTION_KEY must be exactly 32 bytes")

        print("=== Starting Beam Processing ===")
        print(f"Execution ID : {execution_id}")
        print(f"File count   : {len(input_files)}")

        successful_database_operations = 0
        for index, input_file_str in enumerate(input_files, start=1):
            input_file = Path(input_file_str)
            file_error_output = f"{error_output_base}_{input_file.stem}"

            print(f"\n=== Processing File {index}/{len(input_files)} ===")
            print(f"Input file   : {input_file}")
            print(f"Error output : {file_error_output}")

            command = [
                "java", "-jar", BEAM_JAR,
                "--runner=DirectRunner",
                f"--inputFile={input_file}",
                f"--executionId={execution_id}",
                f"--sourceCreationTime={source_creation_time}",
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

            count_prefix = "LUMI_DB_WRITE_SUCCESS_COUNT="
            count_lines = [line for line in result.stdout.splitlines() if line.startswith(count_prefix)]
            if not count_lines:
                raise RuntimeError(f"Beam did not report database operation count for {input_file.name}")
            successful_database_operations += int(count_lines[-1][len(count_prefix):])

            if result.returncode != 0:
                raise RuntimeError(
                    f"Beam pipeline failed for file {input_file.name} "
                    f"with exit code {result.returncode}"
                )

            print(f"Beam processing completed successfully: {input_file.name}")

        print(f"\n=== All Files Processed Successfully ===")
        print(f"Execution ID: {execution_id}")
        print(f"Files processed: {len(input_files)}")
        return successful_database_operations

    run_beam_pipeline = PythonOperator(
        task_id="run_beam_pipeline",
        python_callable=_run_beam_pipeline,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    def _verify_record_count(**context):
        """Compare the control-file count with successful PostgreSQL operations."""
        conf = context["dag_run"].conf
        execution_id = conf["execution_id"]
        try:
            expected_input_records = int(conf["expected_record_count"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                "expected_record_count is missing or invalid."
            ) from exc

        successful_database_operations = context["ti"].xcom_pull(task_ids="run_beam_pipeline")
        if successful_database_operations is None:
            raise RuntimeError(f"Beam database operation count is missing for execution {execution_id}")
        record_count_difference = expected_input_records - successful_database_operations
        not_loaded_records = max(0, record_count_difference)

        print("=== Record Count Verification ===")
        print(f"Execution ID             : {execution_id}")
        print(f"Expected record count     : {expected_input_records:,}")
        print(f"Actual record count       : {successful_database_operations:,}")
        print(f"Not loaded records        : {not_loaded_records:,}")

        if successful_database_operations != expected_input_records:
            raise RuntimeError(
                "Record count mismatch. "
                f"Expected record count: {expected_input_records}; "
                f"Actual record count: {successful_database_operations}; "
                f"Not loaded record count: {not_loaded_records}; "
                f"Difference (expected - actual): {record_count_difference}; "
                f"Execution ID: {execution_id}"
            )

        print("Record count verification passed.")
        return {
            "expected_record_count": expected_input_records,
            "actual_record_count": successful_database_operations,
            "record_count_difference": record_count_difference,
            "not_loaded_records": not_loaded_records,
            "successful_database_operations": successful_database_operations,
        }

    verify_record_count = PythonOperator(
        task_id="verify_record_count",
        python_callable=_verify_record_count,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    def _check_errors(**context):
        import glob
        conf = context["dag_run"].conf
        error_output_base = conf.get("error_output")
        error_dir = str(Path(error_output_base).parent)
        error_stem = Path(error_output_base).name

        error_files = sorted(glob.glob(f"{error_dir}/{error_stem}_*.txt"))

        print("=== Checking Error Files ===")
        print(f"Error base: {error_output_base}")

        if not error_files:
            print("No error files found. All records processed successfully.")
            return

        total_errors = 0
        for error_file in error_files:
            count = 0
            samples = []
            with open(error_file, encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        count += 1
                        if len(samples) < 3:
                            try:
                                samples.append(json.loads(line))
                            except json.JSONDecodeError:
                                samples.append({"errorReason": line.strip()})
            total_errors += count
            print(f"Error file       : {error_file}")
            print(f"Error record count: {count:,}")
            for index, sample in enumerate(samples, start=1):
                if isinstance(sample, dict):
                    print(f"  {index}. Reason    : {sample.get('errorReason', 'Unknown error')}")
                else:
                    print(f"  {index}. Reason    : {sample}")

        print(f"Total error records: {total_errors:,}")
        if total_errors > 0:
            print(
                f"Captured {total_errors} error record(s) under {error_dir}; "
                "continuing because the Beam pipeline completed successfully."
            )

    check_errors = PythonOperator(
        task_id="check_errors",
        python_callable=_check_errors,
    )

    notify_completion = BashOperator(
        task_id="notify_completion",
        bash_command="""
            {% set counts = ti.xcom_pull(task_ids='verify_record_count') or {} %}
            echo "=== Ingestion Run Summary ==="
            echo "Execution ID             : {{ dag_run.conf.get('execution_id', 'unknown') }}"
            echo "Input files               : {{ dag_run.conf.get('input_files', []) | join(', ') }}"
            echo "File format               : {{ dag_run.conf.get('file_format', 'unknown') }}"
            echo "Expected record count     : {{ counts.get('expected_record_count', 'unknown') }}"
            echo "Actual record count       : {{ counts.get('actual_record_count', 'unknown') }}"
            echo "Not loaded records        : {{ counts.get('not_loaded_records', 'unknown') }}"
            echo "Finished at               : $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
            {% if counts.get('not_loaded_records', 0) | int > 0 %}
            echo "Completion status         : COMPLETED_WITH_ERRORS"
            {% else %}
            echo "Completion status         : SUCCESS"
            {% endif %}
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    validate_params >> detect_file_type
    detect_file_type >> prepare_json
    detect_file_type >> prepare_csv
    prepare_json >> run_beam_pipeline
    prepare_csv >> run_beam_pipeline
    run_beam_pipeline >> check_errors >> verify_record_count >> notify_completion