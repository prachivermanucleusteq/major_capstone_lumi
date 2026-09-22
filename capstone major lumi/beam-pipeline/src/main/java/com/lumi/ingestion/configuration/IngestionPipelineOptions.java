package com.lumi.ingestion.configuration;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/* All pipeline configuration is supplied at runtime via command-line arguments.*/
public interface IngestionPipelineOptions extends PipelineOptions {

    @Description("Path to the source JSON or CSV file")
    @Validation.Required
    String getInputFile();
    void setInputFile(String value);

    @Description("JDBC connection URL for PostgreSQL")
    @Validation.Required
    String getJdbcUrl();
    void setJdbcUrl(String value);

    @Description("PostgreSQL username")
    @Validation.Required
    String getJdbcUsername();
    void setJdbcUsername(String value);

    @Description("PostgreSQL password")
    @Validation.Required
    String getJdbcPassword();
    void setJdbcPassword(String value);

    @Description("Unique identifier for this ingestion run (UUID). Supplied by the caller.")
    @Validation.Required
    String getExecutionId();
    void setExecutionId(String value);

    @Description("File path where invalid/error records will be written")
    @Validation.Required
    String getErrorOutput();
    void setErrorOutput(String value);

    @Description("AES-256 encryption key — exactly 32 UTF-8 bytes")
    @Validation.Required
    String getEncryptionKey();
    void setEncryptionKey(String value);
}
