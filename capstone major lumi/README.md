# Capstone Lumi Ingestion Pipeline

A scalable data ingestion pipeline that processes JSON and CSV employee files, validates and transforms records with Apache Beam, optionally splits large files using PySpark, orchestrates ingestion with Apache Airflow, and stores processed data in PostgreSQL.
The project also includes a Spring Boot REST API to control and trigger the ingestion workflow.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Project Workflow](#project-workflow)
- [Features](#features)
- [Supported File Formats](#supported-file-formats)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Spring Boot API](#spring-boot-api)
- [Apache Airflow](#apache-airflow)
- [Apache Beam](#apache-beam)
- [PySpark File Splitting](#pyspark-file-splitting)
- [Encryption](#encryption)
- [Validation and Error Handling](#validation-and-error-handling)
- [Docker Setup](#docker-setup)
- [Running the Project](#running-the-project)
- [Example Request](#example-request)
- [Execution Flow](#execution-flow)
- [Logging](#logging)
- [Error Handling](#error-handling)
- [Conclusion](#conclusion)

## Overview

The Lumi Ingestion Pipeline is designed to ingest employee data from source files into a PostgreSQL database.
The system accepts an input file through a Spring Boot REST API. The application checks the file size and determines whether the file can be processed directly or needs to be split into smaller files.
For large files, PySpark is used to split the source file into smaller files based on a configurable size threshold.
The resulting files are then processed through an Apache Airflow DAG, which triggers the Apache Beam ingestion pipeline.
Apache Beam performs parsing, validation, cleansing, metadata enrichment, encryption of sensitive fields, and database loading.

## Architecture

```text
                         +----------------------+
                         |      Client/User     |
                         +----------+-----------+
                                    |
                                    | REST API
                                    v
                         +----------------------+
                         |     Spring Boot      |
                         |      REST API        |
                         +----------+-----------+
                                    |
                         Check File Size
                                    |
                    +---------------+---------------+
                    |                               |
             File <= Threshold              File > Threshold
                    |                               |
                    |                               v
                    |                       +---------------+
                    |                       |    PySpark    |
                    |                       | File Splitting|
                    |                       +-------+-------+
                    |                               |
                    +---------------+---------------+
                                    |
                              Input Files
                                    |
                                    v
                         +----------------------+
                         |    Apache Airflow    |
                         |       DAG            |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         |    Apache Beam       |
                         |  Ingestion Pipeline  |
                         +----------+-----------+
                                    |
                    +---------------+---------------+
                    |               |               |
                    v               v               v
                 Parse          Validate        Transform
                    |               |               |
                    +---------------+---------------+
                                    |
                                    v
                           Metadata Enrichment
                                    |
                                    v
                              Encryption
                                    |
                                    v
                         +----------------------+
                         |      PostgreSQL      |
                         |    Employee Table    |
                         +----------------------+

                                    |
                                    v
                         Invalid Records / Errors
                                    |
                                    v
                              Error File
```

## Technology Stack

| Technology | Purpose |
| --- | --- |
| Java | Backend and Beam pipeline development |
| Spring Boot | REST API and ingestion orchestration |
| Apache Beam | Data parsing, validation and transformation |
| Apache Airflow | Workflow orchestration |
| PySpark | Splitting large input files |
| PostgreSQL | Target database |
| Docker | PostgreSQL and Airflow environment |
| Maven | Java dependency and build management |
| Python | Airflow DAGs and PySpark |
| Git | Version control |

## Project Workflow

The complete ingestion workflow is:

```text
Source File
    |
    v
Spring Boot API
    |
    v
Check File Size
    |
    +----------------------+
    |                      |
    v                      v
Small File              Large File
    |                      |
    |                      v
    |                  PySpark
    |                      |
    |                      v
    |                Split Files
    |                      |
    +----------+-----------+
               |
               v
        Airflow DAG
               |
               v
       Apache Beam
               |
       +-------+-------+
       |       |       |
      Parse  Validate Cleanse
               |
               v
          Enrichment
               |
               v
           Encryption
               |
               v
          PostgreSQL
```

## Features

### File Ingestion

- Supports JSON files
- Supports CSV files
- Accepts input file location through the REST API
- Supports configurable file-size threshold

### Large File Processing

- Checks the input file size before ingestion
- Files below or equal to the configured threshold are processed directly
- Files above the threshold are split using PySpark
- Split files are stored inside an execution-specific directory

### Apache Beam Processing

- Parses JSON and CSV records
- Validates employee records
- Handles missing or null values
- Enriches records with ingestion metadata
- Encrypts sensitive employee information
- Writes valid records to PostgreSQL
- Sends invalid records to an error output

### Orchestration

- Apache Airflow manages the ingestion workflow
- Spring Boot triggers Airflow DAG runs
- Each ingestion request generates a single executionId
- The same executionId is passed to all files generated from that request

### Data Validation

- Validates incoming employee records
- Validates expected record count
- Compares expected and actual processed records
- Fails the ingestion when the record count does not match

### Security

Sensitive employee fields are encrypted before being stored in PostgreSQL.

The encrypted fields include:

- Salary
- Phone number
- Emergency contact phone number

## Supported File Formats

Currently supported:

- JSON
- CSV

Other formats such as XML and Fixed Width may be added in future versions.

## Project Structure

```text
lumi-ingestion/
│
├── beam-pipeline/
│   ├── pom.xml
│   └── src/
│       └── main/
│           └── java/
│               └── com/
│                   └── lumi/
│                       └── ingestion/
│                           ├── configuration/
│                           │   └── IngestionPipelineOptions.java
│                           │
│                           ├── error/
│                           │   └── ErrorRecord.java
│                           │
│                           ├── model/
│                           │   └── EmployeeRecord.java
│                           │
│                           ├── transform/
│                           │   ├── ParseEmployeeFn.java
│                           │   ├── ValidateEmployeeFn.java
│                           │   └── EnrichEmployeeFn.java
│                           │
│                           └── IngestionPipeline.java
│
├── spring-boot-ingestion/
│   ├── src/
│   │   └── main/
│   │       ├── java/
│   │       │   └── com/
│   │       │       └── lumi/
│   │       │           └── ingestion/
│   │       │               ├── controller/
│   │       │               ├── service/
│   │       │               ├── repository/
│   │       │               ├── dto/
│   │       │               ├── exception/
│   │       │               ├── configuration/
│   │       │               └── util/
│   │       │
│   │       └── resources/
│   │           └── application.properties
│   │
│   └── pom.xml
│
├── airflow/
│   └── dags/
│       └── ingestion_dag.py
│
├── pyspark/
│   └── split_file.py
│
├── database/
│   └── schema/
│       └── v1_create_employee_table.sql
│
├── data/
│   ├── json/
│   ├── csv/
│   ├── errors/
│   └── split/
│
├── docker-compose.yaml
│
├── .env
│
├── .gitignore
│
├── README.md
│
└── .idea/
```

## Data Flow

1. Client sends request
2. Spring Boot validates the file and checks file size
3. If the file is within the threshold, it is processed directly
4. If the file exceeds the threshold, PySpark splits it into smaller chunks
5. Airflow triggers the Beam pipeline
6. Beam parses, validates, enriches, encrypts, and loads data
7. Valid records are written to PostgreSQL
8. Invalid records are written to an error output

### Control file example

The control file contains metadata such as:

```properties
file_name=employees.json
file_path=data/json/employees.json
record_count=1000
```

### Execution ID

One API request generates one executionId.

Example:

```text
executionId = 8f9d2a4e-6d3f-4f42-91c2-xxxxxxxxxxxx
```

All split files generated from that request use the same execution ID.

```text
data/
└── split/
    └── 8f9d2a4e-6d3f-4f42-91c2-xxxxxxxxxxxx/
        ├── employee_part_001.json
        ├── employee_part_002.json
        └── employee_part_003.json
```

## Spring Boot API

The Spring Boot application acts as the entry point for the ingestion process.

### Main endpoint

```http
POST /api/ingestion/trigger
```

The endpoint accepts a control file path and starts the ingestion workflow.

### Responsibilities

- Input validation
- File existence validation
- File format validation
- File size checking
- Threshold configuration
- PySpark execution
- Split file discovery
- Execution ID generation
- Airflow DAG triggering
- DAG run tracking
- Exception handling
- API response generation

## Apache Airflow

Apache Airflow orchestrates the ingestion process.

The main DAG is:

```text
lumi_ingestion_dag
```

The DAG is responsible for:

- Receiving the input file
- Determining the input format
- Executing the appropriate Beam pipeline
- Handling successful ingestion
- Handling ingestion failures
- Managing error output

### Airflow access

Airflow is exposed locally at:

```text
http://localhost:8081
```

Internally, the Airflow container exposes port 8080.

## Apache Beam

Apache Beam performs the core processing.

The pipeline follows the sequence:

```text
Read Input
    |
    v
Parse
    |
    v
Validate
    |
    v
Cleanse
    |
    v
Enrich
    |
    v
Encrypt Sensitive Data
    |
    v
Write to PostgreSQL
```

### Parsing

The pipeline supports:

- JSON
- CSV

### Validation

Employee records are validated before database insertion.

Invalid records are separated from valid records.

```text
Valid Record
     |
     v
PostgreSQL

Invalid Record
     |
     v
Error Output
```

### Metadata Enrichment

Metadata is added to each successfully processed record.

The metadata includes:

- ingestion_timestamp
- execution_id
- source_creation_time

Example:

```text
execution_id: 8f9d2a4e-6d3f-4f42-91c2-xxxxxxxxxxxx
```

## PySpark File Splitting

PySpark is used when the source file exceeds the configured size threshold.

Example:

- Configured threshold: 500 KB
- Input file size: 1.8 MB
- Result: the Spring Boot application invokes the PySpark splitting process

The split files are stored under the execution ID directory:

```text
data/split/<executionId>/
```

The original format is preserved.

For JSON:

```text
employee.json
        |
        v
employee_part_001.json
employee_part_002.json
employee_part_003.json
```

For CSV:

```text
employee.csv
        |
        v
employee_part_001.csv
employee_part_002.csv
employee_part_003.csv
```

## Encryption

Sensitive employee information is encrypted before being inserted into PostgreSQL.

The encrypted fields include:

- salary
- phone_number
- emergency_contact.phone

The encryption process is performed inside the Beam transformation stage.

```text
Source Data
    |
    v
Parse Employee
    |
    v
Validate Employee
    |
    v
Enrich Employee
    |
    v
Encrypt Sensitive Fields
    |
    v
PostgreSQL
```

The encryption key is supplied through runtime configuration or environment variables rather than being hard-coded in source code.

## Validation and Error Handling

The pipeline separates valid and invalid records.

### Valid records

```text
Validate
   |
   v
Transform
   |
   v
Encrypt
   |
   v
PostgreSQL
```

### Invalid records

Invalid records are written to an error output.

Example:

```text
errors/
└── errors-00000-of-00001.jsonl
```

The error record contains information about the failed record and the reason for failure.

### Record count validation

The ingestion process supports record-count validation using the control file.

The control file includes:

```properties
record_count=1000
```

After processing, the system compares:

```text
Expected Records
        |
        v
      1000

Actual Records
        |
        v
      1000
```

If the counts match, ingestion is considered successful. If they do not match, ingestion fails and the response includes both expected and actual counts.

## Docker Setup

Docker is used for the infrastructure components.
The main services include:
- PostgreSQL
- Airflow

Spring Boot runs locally and communicates with these services.

### PostgreSQL

PostgreSQL is exposed locally via the configured host port, for example:

```text
localhost:5432
```

### Airflow

Airflow is available at:

```text
http://localhost:8082
```

## Running the Project

### 1. Clone the repository

```bash
git clone <repository-url>
cd lumi-ingestion
```

### 2. Configure environment variables

Create the required environment configuration.

Example:

```bash
DB_HOST=localhost
DB_PORT=5433
DB_NAME=lumi
DB_USERNAME=postgres
DB_PASSWORD=postgres
ENCRYPTION_KEY=<your-key>
```

### 3. Start Docker services

```bash
docker compose up -d
```

Check running containers:

```bash
docker ps
```

### 4. Verify PostgreSQL

Confirm that PostgreSQL is running and accessible.

Example:

```text
localhost:5432
```

### 5. Verify Airflow

Open:

```text
http://localhost:8081
```

Confirm that the DAG named `lumi_ingestion_dag` is available.

### 6. Build the Beam pipeline

Navigate to the Beam project:

```bash
cd beam-pipeline
mvn clean package
```

The generated JAR is used by Airflow to execute the Beam pipeline.

### 7. Start Spring Boot

From the Spring Boot project:

```bash
cd spring-boot-ingestion
mvn spring-boot:run
```

The application runs on:

```text
http://localhost:8080
```

## Example Request

Example API request:

```http
POST http://localhost:8080/api/ingestion/trigger
Content-Type: application/json
```

Request body:

```json
{
  "controlFile": "data/control/employee_control.txt"
}
```

Example response:

```json
{
  "executionId": "test-exec-id",
  "dagRunIds": "lumi-test-exec-id",
  "inputFileCount": 1,
  "split": false,
  "status": "ACCEPTED"
}
```

This matches the actual API contract returned by the Spring Boot service: `executionId`, `dagRunIds`, `inputFileCount`, `split`, and `status`.

## Execution Flow

A typical execution looks like:

```text
POST /api/ingestion/trigger
             |
             v
       Generate executionId
             |
             v
       Read Control File
             |
             v
       Validate Input File
             |
             v
        Check File Size
             |
       +-----+-----+
       |           |
       v           v
    Small        Large
       |           |
       |        PySpark
       |           |
       |      Split Files
       |           |
       +-----+-----+
             |
             v
       Trigger Airflow
             |
             v
       Run Beam Pipeline
             |
             v
          Parse
             |
             v
         Validate
             |
             v
          Cleanse
             |
             v
          Enrich
             |
             v
          Encrypt
             |
             v
       PostgreSQL
             |
             v
     Record Count Check
             |
       +-----+-----+
       |           |
       v           v
     Match      Mismatch
       |           |
       v           v
   Successful    Failed
```

## Logging

The application uses structured logging to make ingestion failures easier to diagnose.

Typical logging levels:

### INFO

Used for normal application flow:

- Ingestion started
- File detected
- Execution ID generated
- Airflow DAG triggered
- Ingestion completed

### WARN

Used for recoverable or unexpected situations:

- File approaching threshold
- Invalid record detected
- Unexpected configuration

### ERROR

Used for failures:

- File not found
- PySpark execution failed
- Airflow DAG failed
- Beam pipeline failed
- Database insertion failed
- Record count mismatch

Sensitive information such as:

- Passwords
- Encryption keys
- Decrypted employee data

must not be written to logs.

## Error Handling

The application uses custom exceptions and centralized exception handling for meaningful API responses.

Examples include:

- FileNotFoundException
- InvalidFileException
- IngestionException
- AirflowException
- PySparkException
- RecordCountMismatchException

Instead of returning a vague internal server error, the API returns a meaningful error response describing the actual failure.

Example:

```json
{
  "status": 400,
  "message": "Input file does not exist",
  "timestamp": "2026-09-22T10:30:00"
}
```

## Conclusion

The Lumi Ingestion Pipeline provides an end-to-end ingestion workflow combining:

- Spring Boot
- Apache Airflow
- Apache Beam
- PySpark
- PostgreSQL

The system is designed to process employee data reliably while supporting:

- File-size-based processing
- Large-file splitting
- JSON and CSV ingestion
- Data validation
- Data cleansing
- Metadata enrichment
- Sensitive-data encryption
- Error handling
- Record-count validation
- Workflow orchestration

The architecture separates API orchestration, file processing, workflow management, transformation, and database storage into dedicated components, making the pipeline easier to maintain and extend.

## Author
Prachi Verma