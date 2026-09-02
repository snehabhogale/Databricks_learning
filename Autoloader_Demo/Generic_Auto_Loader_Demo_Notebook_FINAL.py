# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ============================================================
# GENERIC AUTO LOADER NOTEBOOK — DEMO VERSION
#
# ONE PARAMETER ONLY:
#     ecname
#
# Configuration for mutiple stage tables:
#     demo_autoloader.etlcontrol
#     demo_autoloader.autoloader_config
#     demo_autoloader.query_repo
#
# One notebook is reused by mutiple independent Job Tasks.
# ============================================================


# COMMAND ----------

# STEP 1 — ONLY INPUT PARAMETER
# ============================================================

import json
import uuid
import time

from functools import reduce
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    row_number,
    regexp_extract,
    concat_ws
)

from pyspark.sql.window import Window


dbutils.widgets.text("ecname", "users_demo")

ecname = dbutils.widgets.get("ecname").strip()

if not ecname:
    raise Exception("ecname parameter is mandatory")

print(f"===================================================")
print(f"Starting Generic Auto Loader")
print(f"ECNAME : {ecname}")
print(f"===================================================")


# COMMAND ----------

# STEP 2 — RESOLVE JOB RUN ID
# ============================================================

try:

    ctx = (
        dbutils.notebook
        .entry_point
        .getDbutils()
        .notebook()
        .getContext()
    )

    job_run_id = ctx.jobId().getOrElse(None)
    task_run_id = ctx.taskRunId().getOrElse(None)

    if job_run_id and task_run_id:
        run_id = f"{job_run_id}_{task_run_id}"

    elif task_run_id:
        run_id = str(task_run_id)

    else:
        run_id = "manual_run"

except Exception:

    run_id = "manual_run"


print(f"Pipeline Run ID : {run_id}")


# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT
# MAGIC             id,
# MAGIC             ecname,
# MAGIC             metadatatype,
# MAGIC             batchsetting,
# MAGIC             sourceobject,
# MAGIC             sinkobject,
# MAGIC             loadsetting,
# MAGIC             mappingsettings,
# MAGIC             enableflag
# MAGIC         FROM demo_autoloader.etlcontrol

# COMMAND ----------

# STEP 3 — GET CONFIGURATION FROM ETL CONTROL
# ============================================================

try:

    control_rows = spark.sql(f"""
        SELECT
            id,
            ecname,
            metadatatype,
            batchsetting,
            sourceobject,
            sinkobject,
            loadsetting,
            mappingsettings,
            enableflag
        FROM demo_autoloader.etlcontrol
        WHERE ecname = '{ecname.replace("'", "''")}'
          AND enableflag = 'y'
        ORDER BY modifieddate DESC
        LIMIT 1
    """).collect()

    if not control_rows:
        raise Exception(
            f"No enabled configuration found in etlcontrol "
            f"for ecname = '{ecname}'"
        )

    control = control_rows[0]

    etl_id = control["id"]
    mappingsettings_str = control["mappingsettings"]

    sourceobject_str = control["sourceobject"]
    sinkobject_str = control["sinkobject"]
    loadsetting_str = control["loadsetting"]

except Exception as e:

    print(f"[ERROR] etlcontrol lookup failed: {e}")
    raise


# COMMAND ----------

# STEP 4 — PARSE ETL CONTROL JSON
# ============================================================

def safe_json(value):

    if value is None:
        return {}

    if isinstance(value, dict):
        return value

    try:
        return json.loads(value)
    except Exception:
        return {}


mapping_settings = safe_json(mappingsettings_str)
sourceobject = safe_json(sourceobject_str)
sinkobject = safe_json(sinkobject_str)
loadsetting = safe_json(loadsetting_str)


print("ETL configuration loaded")


# COMMAND ----------

# STEP 5 — GET AUTO LOADER CONFIG
# ============================================================

try:

    auto_rows = spark.sql(f"""
        SELECT *
        FROM demo_autoloader.autoloader_config
        WHERE ecname = '{ecname.replace("'", "''")}'
          AND enabled_flag = 'y'
    """).collect()

    if not auto_rows:
        raise Exception(
            f"No enabled Auto Loader configuration found "
            f"for ecname = '{ecname}'"
        )

    auto_cfg = auto_rows[0]

except Exception as e:

    print(f"[ERROR] autoloader_config lookup failed: {e}")
    raise


# COMMAND ----------

# STEP 6 — READ AUTO LOADER CONFIG
# ============================================================
#
# Change these field names only if your autoloader_config
# table uses different column names.
#
# Recommended config:
#
# ecname
# filesystem
# directory
# schema_location
# checkpoint_location
# target_full_name
# archive_directory
# archive_enabled
# sourceobjecttype
# enableflag
#
# Optional (STEP 11 backpressure / scale controls, all have
# safe defaults below if the columns don't exist yet):
#
# max_files_per_trigger   (default '1000')
# max_bytes_per_trigger   (default none / unset)
# use_notifications       (default 'n' — directory listing;
#                          set 'y' once cloud notification
#                          queue is provisioned, see STEP 11)
#
# ============================================================

def cfg_value(row, column_name, default=None):

    try:
        value = row[column_name]

        if value is None:
            return default

        return value

    except Exception:
        return default


# Demo configuration uses a complete source_path instead of a
# company-specific ADLS filesystem + storage-account combination.
source_path = cfg_value(auto_cfg, "source_path")
schema_location = cfg_value(auto_cfg, "schema_location")
checkpoint_location = cfg_value(auto_cfg, "checkpoint_location")

target_full_name = cfg_value(
    auto_cfg,
    "target_full_name"
)

archive_directory = cfg_value(
    auto_cfg,
    "archive_directory"
)

archive_enabled = str(
    cfg_value(auto_cfg, "archive_enabled", "N")
).upper()


# If target is stored in sinkobject instead
if not target_full_name:

    target_schema = sinkobject.get("schemaname")

    target_table = sinkobject.get("tablename")

    if target_schema and target_table:

        target_full_name = (
            f"{target_schema}.{target_table}"
        )


if not target_full_name:
    raise Exception(
        f"target_full_name could not be resolved "
        f"for ecname = '{ecname}'"
    )


target_schema = target_full_name.split(".")[0]
target_table_name = target_full_name.split(".")[1]

staging_table = (
    f"{target_schema}.{target_table_name}_stg"
)

delete_stg_table = (
    f"{target_schema}.{target_table_name}_del_stg"
)


print(f"Source Path         : {source_path}")
print(f"Target              : {target_full_name}")
print(f"Schema Location     : {schema_location}")
print(f"Checkpoint Location : {checkpoint_location}")
print(f"Archive Enabled     : {archive_enabled}")
print(f"Archive Directory   : {archive_directory}")


# COMMAND ----------

# STEP 7 — BUILD SOURCE PATH
# ============================================================

if not source_path:
    raise Exception(
        f"source_path could not be resolved for ecname = '{ecname}'"
    )

print(f"Source Path : {source_path}")


# COMMAND ----------

# STEP 8 — ERROR LOGGER
# ============================================================

def log_pipeline_error(
    step_name,
    error_msg,
    filename=None
):

    try:

        safe_error = (
            str(error_msg)
            .replace("'", "''")
            [:4000]
        )

        safe_filename = (
            str(filename or "")
            .replace("'", "''")
        )

        spark.sql(f"""
            INSERT INTO demo_autoloader.etl_error_log
            (
                etl_id,
                tablename,
                filename,
                step_name,
                status,
                cmnts,
                pipelinerunid,
                createddt
            )
            VALUES
            (
                {int(etl_id) if etl_id else 'NULL'},
                '{target_table_name}',
                '{safe_filename}',
                '{step_name.replace("'", "''")}',
                'FAILED',
                '{safe_error}',
                '{run_id}',
                current_timestamp()
            )
        """)

    except Exception as log_error:

        print(
            f"[WARN] etl_error_log write failed: "
            f"{log_error}"
        )


# COMMAND ----------

# STEP 9 — PARSE MAPPINGS
# ============================================================

mappings = mapping_settings.get(
    "mappings",
    []
)

column_mapping = {}

sink_column_types = {}

for mapping in mappings:

    try:

        source_path_value = (
            mapping
            .get("source", {})
            .get("path", "")
        )

        sink_obj = (
            mapping
            .get("sink", {})
        )

        if source_path_value.startswith("$['"):

            source_col = (
                source_path_value
                .split("$['")[1]
                .split("']")[0]
            )

        elif source_path_value.startswith("$["):

            source_col = (
                source_path_value
                .split("$[")[1]
                .split("]")[0]
            )

        else:

            source_col = source_path_value

        sink_col = sink_obj.get(
            "name",
            ""
        )

        sink_type = sink_obj.get(
            "type",
            ""
        )

        if source_col and sink_col:

            column_mapping[
                source_col
            ] = sink_col

            if sink_type:

                sink_column_types[
                    sink_col
                ] = sink_type

    except Exception:

        continue


id_source_column = next(
    (
        source
        for source, sink
        in column_mapping.items()
        if sink == "id"
    ),
    "id"
)


print(
    f"Mapped columns : {len(column_mapping)}"
)

print(
    f"ID source column : {id_source_column}"
)


# COMMAND ----------

# STEP 10 — TARGET COLUMN CHECK
# ============================================================
#
# IMPORTANT:
#
# The target table is NOT automatically altered.
#
# mappingsettings controls which columns are written.
#
# Therefore:
#
# New source column:
#       NOT mapped
#       NOT written
#       NOT added to target
#
# ============================================================

target_columns = {
    field.name
    for field in
    spark.table(target_full_name).schema.fields
}


mapped_target_columns = set(
    column_mapping.values()
)


unknown_target_columns = (
    mapped_target_columns
    - target_columns
)


if unknown_target_columns:

    error_msg = (
        "Mappingsettings contains target columns "
        "which do not exist in target table: "
        + ", ".join(
            sorted(unknown_target_columns)
        )
    )

    log_pipeline_error(
        "STEP_10_TARGET_SCHEMA",
        error_msg
    )

    raise Exception(error_msg)


print(
    f"Target columns validated : "
    f"{len(mapped_target_columns)}"
)


# COMMAND ----------

# STEP 11 — AUTO LOADER OPTIONS
# ============================================================
#
# IMPORTANT:
#
# schemaEvolutionMode = rescue
#
# This is intentional.
#
# We DO NOT want Auto Loader to automatically add
# columns to the target/staging table.
#
# New fields are captured in rescued_data instead.
#
# ------------------------------------------------------------
#
# pathGlobFilter = "*.json"
#
# The source directory also contains .done marker files
# (one per .json file, holding its expected record count
# for downstream count-check validation only). These must
# never be discovered/parsed by Auto Loader as data files,
# so the glob filter restricts discovery to *.json only.
#
# ------------------------------------------------------------
#
# maxFilesPerTrigger / maxBytesPerTrigger
#
# With trigger(availableNow=True), Auto Loader will otherwise
# try to discover and queue up EVERY backlog file before the
# first micro-batch even starts. If a large drop lands (e.g.
# a backfill, an upstream outage catching up, or simply a busy
# day for this ecname), that single run can blow past cluster
# memory/shuffle limits or just take a very long time before
# foreachBatch produces its first log line.
#
# Setting a per-trigger cap makes availableNow=True process the
# backlog across many small, bounded micro-batches instead of
# one huge one. Each micro-batch still commits its own offset
# to the checkpoint, so nothing is lost or reprocessed if a
# later batch fails partway through the backlog — Structured
# Streaming resumes from the last committed micro-batch.
#
# Tune these two per ecname's typical file size via
# autoloader_config if some sources are much larger than
# others; the values below are conservative demo defaults.
#
# ------------------------------------------------------------
#
# cloudFiles.useNotifications
#
# Default (false) = directory LISTING mode. Every run, Auto
# Loader lists the source path to find files newer than its
# checkpoint. Fine at low-to-moderate file counts. At millions
# of files in a single source directory, that LIST call itself
# becomes the bottleneck and cost driver on every single run,
# independent of how few files are actually new.
#
# Setting this to true switches to NOTIFICATION mode: Auto
# Loader has Databricks provision a cloud queue (S3 event
# notifications -> SQS on AWS, ADLS -> Event Grid+Queue on
# Azure) once, then each run just drains that queue for files
# that arrived since last drain — no full directory listing,
# so cost/latency stop scaling with total files in the path
# and instead scale with new files since last run.
#
# This is OFF by default here because it requires one-time
# cloud-side setup (the storage credential/service principal
# Databricks uses must be allowed to create the queue and
# notification config on your bucket/container - see
# https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-notification-mode.html).
# Set use_notifications = 'y' in autoloader_config per-ecname
# once that's done, particularly for any ecname whose source
# directory is expected to hold millions of files.
#
# Note this is a SEPARATE concern from the Databricks Jobs
# "file arrival trigger" discussed in scheduling notes below —
# that trigger decides WHEN this notebook runs; this option
# controls how Auto Loader finds new files ONCE it's running.
# Enabling file events on the Unity Catalog external location
# (for the job trigger) does not enable this; they're
# configured independently, even though both ultimately rely
# on the same underlying cloud notification services.
#
# ============================================================

max_files_per_trigger = str(
    cfg_value(auto_cfg, "max_files_per_trigger", "1000")
)

max_bytes_per_trigger = cfg_value(
    auto_cfg, "max_bytes_per_trigger", None
)

use_notifications = str(
    cfg_value(auto_cfg, "use_notifications", "n")
).strip().lower() in ("y", "yes", "true", "1")

reader = (
    spark.readStream
    .format("cloudFiles")
    .option(
        "cloudFiles.format",
        "json"
    )
    .option(
        "pathGlobFilter",
        "*.json"
    )
    .option(
        "cloudFiles.schemaLocation",
        schema_location
    )
    .option(
        "cloudFiles.schemaEvolutionMode",
        "rescue"
    )
    .option(
        "rescuedDataColumn",
        "_rescued_data"
    )
    .option(
        "cloudFiles.includeExistingFiles",
        "true"
    )
    .option(
        "multiline",
        "true"
    )
    .option(
        "cloudFiles.maxFilesPerTrigger",
        max_files_per_trigger
    )
    .option(
        "cloudFiles.useNotifications",
        "true" if use_notifications else "false"
    )
)

if max_bytes_per_trigger:
    reader = reader.option(
        "cloudFiles.maxBytesPerTrigger",
        str(max_bytes_per_trigger)
    )

if use_notifications:
    print(
        f"[INFO] ecname={ecname}: cloudFiles.useNotifications=true "
        f"— Auto Loader will use/create a cloud notification queue "
        f"for {source_path} instead of directory listing."
    )


# COMMAND ----------

# STEP 12 — LOAD AUTO LOADER STREAM
# ============================================================

source_stream = (
    reader
    .load(source_path)
    .withColumn(
        "_input_file_path",
        col("_metadata.file_path")
    )
)


# COMMAND ----------

# STEP 13 — FOREACH BATCH PROCESSOR
# ============================================================

def process_batch(
    micro_batch_df,
    batch_id
):

    print("")
    print(
        "==================================================="
    )
    print(
        f"Processing ECNAME : {ecname}"
    )
    print(
        f"Micro Batch ID    : {batch_id}"
    )
    print(
        "==================================================="
    )

    pipeline_run_id = (
        f"{run_id}_{batch_id}"
    )

    schema_error_records = []

    try:

        if micro_batch_df.isEmpty():

            print("Empty micro-batch")

            return


        # ------------------------------------------------
        # STEP 13A — GET FILE INFORMATION
        # ------------------------------------------------

        file_df = (
            micro_batch_df
            .select(
                "_input_file_path"
            )
            .distinct()
            .withColumn(
                "filename",
                regexp_extract(
                    col("_input_file_path"),
                    r"([^/]+)$",
                    1
                )
            )
        )


        incoming_files = [
            row["filename"]
            for row
            in file_df.collect()
        ]


        print(
            f"Files received : "
            f"{len(incoming_files)}"
        )

        print(
            incoming_files
        )


        # ------------------------------------------------
        # STEP 13B — CHECK ALREADY PROCESSED FILES
        # ------------------------------------------------

        if incoming_files:

            escaped_files = [
                f.replace("'", "''")
                for f in incoming_files
            ]

            files_clause = ", ".join(
                f"'{f}'"
                for f in escaped_files
            )

            processed_rows = spark.sql(f"""
                SELECT DISTINCT filename
                FROM demo_autoloader.file_process_logs
                WHERE tablename =
                    '{target_table_name}'
                  AND status = 'SUCCESS'
                  AND filename IN ({files_clause})
            """).collect()

            processed_files = {
                r["filename"]
                for r in processed_rows
            }

        else:

            processed_files = set()


        print(
            f"Already processed : "
            f"{len(processed_files)}"
        )


        # ------------------------------------------------
        # STEP 13C — REMOVE ALREADY PROCESSED FILES
        # ------------------------------------------------

        new_file_names = [
            f
            for f in incoming_files
            if f not in processed_files
        ]


        if not new_file_names:

            print(
                "All files already processed."
            )

            return


        new_file_names_set = set(
            new_file_names
        )


        working_df = (
            micro_batch_df
            .filter(
                regexp_extract(
                    col("_input_file_path"),
                    r"([^/]+)$",
                    1
                ).isin(
                    list(new_file_names_set)
                )
            )
        )


        # ------------------------------------------------
        # STEP 13D — SCHEMA DRIFT CHECK
        # ------------------------------------------------
        #
        # New columns are NOT automatically added.
        #
        # Auto Loader puts unknown fields in
        # _rescued_data.
        #
        # If _rescued_data contains values,
        # schema change requires confirmation.
        #
        # ------------------------------------------------

        rescued_count = 0

        if "_rescued_data" in working_df.columns:

            rescued_count = (
                working_df
                .filter(
                    col("_rescued_data").isNotNull()
                )
                .limit(1)
                .count()
            )


        if rescued_count > 0:

            drift_files = (
                working_df
                .filter(
                    col("_rescued_data").isNotNull()
                )
                .select(
                    regexp_extract(
                        col(
                            "_input_file_path"
                        ),
                        r"([^/]+)$",
                        1
                    ).alias("filename")
                )
                .distinct()
                .collect()
            )


            for row in drift_files:

                filename = row["filename"]

                error_message = (
                    "Schema change detected. "
                    "New/unmapped source field(s) "
                    "were found in file. "
                    "Mappingsettings/target schema "
                    "must be updated after confirmation."
                )

                schema_error_records.append(
                    {
                        "filename": filename,
                        "error_message": error_message
                    }
                )

                log_pipeline_error(
                    "STEP_13D_SCHEMA_DRIFT",
                    error_message,
                    filename
                )


            # ------------------------------------------------
            # IMPORTANT
            # ------------------------------------------------
            #
            # Do not process files with schema drift.
            #
            # They remain discoverable by Auto Loader.
            #
            # After configuration is corrected,
            # the stream can process them.
            #
            # ------------------------------------------------

            drift_file_names = {
                r["filename"]
                for r in drift_files
            }


            working_df = (
                working_df
                .filter(
                    ~regexp_extract(
                        col(
                            "_input_file_path"
                        ),
                        r"([^/]+)$",
                        1
                    ).isin(
                        list(drift_file_names)
                    )
                )
            )


        # ------------------------------------------------
        # STEP 13E — IF NOTHING VALID REMAINS
        # ------------------------------------------------

        if working_df.isEmpty():

            print(
                "No schema-valid files remain."
            )

            return


        # ------------------------------------------------
        # STEP 13F — ASSIGN SOURCE IDS
        # ------------------------------------------------
        #
        # Source IDs are maintained independently
        # per ECNAME.
        #
        # REQUIRED TABLE:
        #
        # file_process_id_sequence
        #
        # columns:
        #   ecname
        #   id
        #   lock_token
        #
        # ------------------------------------------------

        valid_file_names = [
            r["filename"]
            for r in
            working_df
            .select(
                regexp_extract(
                    col("_input_file_path"),
                    r"([^/]+)$",
                    1
                ).alias("filename")
            )
            .distinct()
            .collect()
        ]


        file_count = len(
            valid_file_names
        )


        sourceid_map = {}


        if file_count > 0:

            lock_token = str(
                uuid.uuid4()
            )


            for attempt in range(1, 6):

                try:

                    spark.sql(f"""
                        MERGE INTO
                            demo_autoloader.file_process_id_seq
                            AS seq
                        USING
                            (
                                SELECT
                                    '{ecname.replace("'", "''")}'
                                    AS ecname
                            ) AS src
                        ON
                            seq.ecname =
                            src.ecname

                        WHEN MATCHED THEN
                            UPDATE SET
                                seq.id =
                                    seq.id +
                                    {file_count},
                                seq.lock_token =
                                    '{lock_token}'

                        WHEN NOT MATCHED THEN
                            INSERT
                            (
                                ecname,
                                id,
                                lock_token
                            )
                            VALUES
                            (
                                src.ecname,
                                {file_count},
                                '{lock_token}'
                            )
                    """)

                    break

                except Exception as e:

                    if (
                        "ConcurrentAppendException"
                        in str(e)
                        and attempt < 5
                    ):

                        time.sleep(
                            attempt * 2
                        )

                    else:

                        raise


            sequence_row = spark.sql(f"""
                SELECT id
                FROM demo_autoloader.file_process_id_seq
                WHERE ecname =
                    '{ecname.replace("'", "''")}'
                  AND lock_token =
                    '{lock_token}'
            """).collect()


            if not sequence_row:

                raise Exception(
                    "Could not obtain source ID sequence"
                )


            sourceid_end = (
                sequence_row[0]["id"]
            )

            sourceid_start = (
                sourceid_end
                - file_count
                + 1
            )


            for idx, filename in enumerate(
                sorted(valid_file_names)
            ):

                sourceid_map[
                    filename
                ] = (
                    sourceid_start
                    + idx
                )


        print(
            f"Source ID range : "
            f"{sourceid_start} -> "
            f"{sourceid_end}"
        )


        # ------------------------------------------------
        # STEP 13G — SOURCE ID LOOKUP
        # ------------------------------------------------

        sourceid_lookup_df = (
            spark.createDataFrame(
                [
                    (
                        filename,
                        sourceid_map[
                            filename
                        ]
                    )
                    for filename
                    in valid_file_names
                ],
                [
                    "_fn",
                    "_sid"
                ]
            )
        )


        working_df = (
            working_df
            .withColumn(
                "_fn",
                regexp_extract(
                    col(
                        "_input_file_path"
                    ),
                    r"([^/]+)$",
                    1
                )
            )
            .join(
                sourceid_lookup_df,
                "_fn",
                "left"
            )
        )


        # ------------------------------------------------
        # STEP 13H — SEPARATE DELETE FILES
        # ------------------------------------------------

        working_df = (
            working_df
            .withColumn(
                "_is_delete",
                F.lower(
                    col("_fn")
                ).contains("delete")
            )
        )


        regular_df = (
            working_df
            .filter(
                ~col("_is_delete")
            )
        )


        delete_df = (
            working_df
            .filter(
                col("_is_delete")
            )
        )


        # ------------------------------------------------
        # STEP 13I — REGULAR DATA MAPPING
        # ------------------------------------------------

        regular_count = (
            regular_df
            .limit(1)
            .count()
        )


        merged_count = 0
        deleted_count = 0


        if regular_count > 0:

            column_dtypes = dict(
                regular_df.dtypes
            )


            select_expressions = []


            for (
                source_col,
                sink_col
            ) in column_mapping.items():

                if source_col not in column_dtypes:
                    continue


                source_dtype = (
                    column_dtypes[
                        source_col
                    ]
                )


                # Array → comma-separated string
                if (
                    source_dtype
                    .lower()
                    .startswith("array")
                ):

                    select_expressions.append(
                        concat_ws(
                            ",",
                            col(
                                f"`{source_col}`"
                            )
                        ).alias(
                            sink_col
                        )
                    )

                else:

                    select_expressions.append(
                        col(
                            f"`{source_col}`"
                        ).alias(
                            sink_col
                        )
                    )


            if not select_expressions:

                raise Exception(
                    "No mapped columns found."
                )


            staged_df = (
                regular_df
                .select(
                    *select_expressions,

                    col("_sid").alias(
                        "sourceid"
                    ),

                    current_timestamp().alias(
                        "loaddtm"
                    ),

                    lit("N").alias(
                        "deleted_flag"
                    ),

                    lit(None)
                    .cast("TIMESTAMP")
                    .alias(
                        "date_deleted"
                    )
                )
            )


            # ------------------------------------------------
            # DEDUPLICATE BY ID + NEWEST SOURCE ID
            # ------------------------------------------------

            staged_df = (
                staged_df
                .withColumn(
                    "_row_num",
                    row_number()
                    .over(
                        Window
                        .partitionBy("id")
                        .orderBy(
                            col(
                                "sourceid"
                            ).desc()
                        )
                    )
                )
                .filter(
                    col("_row_num") == 1
                )
                .drop(
                    "_row_num"
                )
            )


            merged_count = (
                staged_df.count()
            )


            print(
                f"Records to merge : "
                f"{merged_count}"
            )


            # ------------------------------------------------
            # WRITE STAGING
            # ------------------------------------------------

            if merged_count > 0:

                try:

                    spark.sql(
                        f"""
                        TRUNCATE TABLE
                        {staging_table}
                        """
                    )

                except Exception:

                    pass


                (
                    staged_df
                    .write
                    .format("delta")
                    .mode("overwrite")
                    .option(
                        "overwriteSchema",
                        "true"
                    )
                    .saveAsTable(
                        staging_table
                    )
                )


                # ------------------------------------------------
                # GET EXISTING MERGE QUERY
                # ------------------------------------------------

                merge_rows = spark.sql(f"""
                    SELECT query_text
                    FROM demo_autoloader.query_repo
                    WHERE query_name =
                        '{ecname.replace("'", "''")}'
                      AND query_type =
                        'merge'
                    LIMIT 1
                """).collect()


                if not merge_rows:

                    raise Exception(
                        f"MERGE query not found "
                        f"for ecname='{ecname}'"
                    )


                merge_query = (
                    merge_rows[0]["query_text"]
                )


                # ------------------------------------------------
                # EXECUTE EXISTING MERGE
                # ------------------------------------------------

                spark.sql(
                    merge_query
                )


                # Reset deleted records
                spark.sql(f"""
                    UPDATE {target_full_name}
                    SET
                        deleted_flag = 'N',
                        date_deleted = NULL
                    WHERE id IN
                        (
                            SELECT id
                            FROM {staging_table}
                        )
                      AND deleted_flag = 'Y'
                """)


        # ------------------------------------------------
        # STEP 13J — DELETE PROCESSING
        # ------------------------------------------------

        if (
            delete_df
            .limit(1)
            .count() > 0
        ):

            delete_parts = []


            delete_files = (
                delete_df
                .select("_fn", "_sid")
                .distinct()
                .collect()
            )


            for file_row in delete_files:

                filename = (
                    file_row["_fn"]
                )

                file_sid = (
                    file_row["_sid"]
                )


                raw_delete_df = (
                    working_df
                    .filter(
                        col("_fn")
                        == filename
                    )
                    .select(
                        "*"
                    )
                )


                if (
                    id_source_column
                    not in raw_delete_df.columns
                ):

                    continue


                delete_parts.append(
                    raw_delete_df
                    .select(
                        col(
                            f"`{id_source_column}`"
                        )
                        .cast("string")
                        .alias("_mid"),

                        lit(
                            file_sid
                        ).alias("_sid"),

                        lit(True).alias(
                            "_is_del"
                        )
                    )
                    .dropna(
                        subset=["_mid"]
                    )
                )


            if delete_parts:

                delete_id_df = reduce(
                    lambda a, b:
                    a.unionByName(b),
                    delete_parts
                )


                # ------------------------------------------------
                # FIND NEWEST EVENT FOR EACH ID
                # ------------------------------------------------

                delete_winners = (
                    delete_id_df
                    .withColumn(
                        "_row_num",
                        row_number()
                        .over(
                            Window
                            .partitionBy(
                                "_mid"
                            )
                            .orderBy(
                                col(
                                    "_sid"
                                ).desc()
                            )
                        )
                    )
                    .filter(
                        col(
                            "_row_num"
                        ) == 1
                    )
                    .select(
                        col(
                            "_mid"
                        ).alias("id"),

                        col(
                            "_sid"
                        ).alias("sourceid")
                    )
                )


                deleted_count = (
                    delete_winners.count()
                )


                if deleted_count > 0:

                    try:

                        spark.sql(
                            f"""
                            TRUNCATE TABLE
                            {delete_stg_table}
                            """
                        )

                    except Exception:

                        pass


                    (
                        delete_winners
                        .write
                        .format("delta")
                        .mode("overwrite")
                        .option(
                            "overwriteSchema",
                            "true"
                        )
                        .saveAsTable(
                            delete_stg_table
                        )
                    )


                    # ------------------------------------------------
                    # SOFT DELETE
                    # ------------------------------------------------

                    spark.sql(f"""
                        MERGE INTO
                            {target_full_name}
                            AS t

                        USING
                            {delete_stg_table}
                            AS d

                        ON
                            CAST(t.id AS STRING)
                            = d.id

                        WHEN MATCHED
                         AND COALESCE(
                                t.sourceid,
                                0
                             )
                             < d.sourceid

                        THEN UPDATE SET

                            t.deleted_flag =
                                'Y',

                            t.date_deleted =
                                current_timestamp(),

                            t.loaddtm =
                                current_timestamp(),

                            t.sourceid =
                                d.sourceid
                    """)


                    print(
                        f"Deleted records : "
                        f"{deleted_count}"
                    )


        # ------------------------------------------------
        # STEP 13K — FILE PROCESS LOG
        # ------------------------------------------------

        successful_files = (
            valid_file_names
        )


        if successful_files:

            log_rows = []


            for filename in successful_files:

                sid = sourceid_map[
                    filename
                ]


                log_rows.append(
                    f"""
                    (
                        {sid},
                        '{target_table_name}',
                        '{filename.replace("'", "''")}',
                        'SUCCESS',
                        current_timestamp(),
                        {merged_count},
                        {merged_count},
                        '{pipeline_run_id}',
                        {int(etl_id)},
                        current_timestamp(),
                        current_timestamp(),
                        current_timestamp()
                    )
                    """
                )


            spark.sql(f"""
                INSERT INTO
                    demo_autoloader.file_process_logs
                (
                    id,
                    tablename,
                    filename,
                    status,
                    lastmodified,
                    lndrowscopied,
                    stgrowscopied,
                    pipelinerunid,
                    batchid,
                    watermark,
                    startdate,
                    enddate
                )
                VALUES
                    {','.join(log_rows)}
            """)


        # ------------------------------------------------
        # STEP 13K.1 — CLEAN STAGING TABLES
        # ------------------------------------------------
        #
        # Once file_process_logs confirms a sourceid
        # was SUCCESSfully merged into the target table,
        # that data no longer needs to sit in staging.
        #
        # This deletes only rows whose sourceid has a
        # confirmed SUCCESS log entry — never a blind
        # truncate — so anything not yet logged (e.g. a
        # concurrent/overlapping batch, however unlikely
        # given single-writer foreachBatch) is left alone.
        #
        # ------------------------------------------------

        try:

            spark.sql(f"""
                DELETE FROM {staging_table}
                WHERE sourceid IN (
                    SELECT id
                    FROM demo_autoloader.file_process_logs
                    WHERE tablename = '{target_table_name}'
                      AND status = 'SUCCESS'
                )
            """)

        except Exception as cleanup_error:

            print(
                f"[WARN] staging_table cleanup failed: "
                f"{cleanup_error}"
            )


        try:

            spark.sql(f"""
                DELETE FROM {delete_stg_table}
                WHERE sourceid IN (
                    SELECT id
                    FROM demo_autoloader.file_process_logs
                    WHERE tablename = '{target_table_name}'
                      AND status = 'SUCCESS'
                )
            """)

        except Exception as cleanup_error:

            print(
                f"[WARN] delete_stg_table cleanup failed: "
                f"{cleanup_error}"
            )


        # ------------------------------------------------
        # STEP 13L — SCHEMA ERROR LOG
        # ------------------------------------------------

        if schema_error_records:

            for error in schema_error_records:

                safe_filename = (
                    error["filename"]
                    .replace("'", "''")
                )

                safe_error = (
                    error["error_message"]
                    .replace("'", "''")
                )


                spark.sql(f"""
                    INSERT INTO
                        demo_autoloader.file_schema_validation_errors
                    (
                        tablename,
                        filename,
                        error_message,
                        pipelinerunid,
                        batchid,
                        created_timestamp
                    )
                    VALUES
                    (
                        '{target_table_name}',
                        '{safe_filename}',
                        '{safe_error}',
                        '{pipeline_run_id}',
                        {int(etl_id)},
                        current_timestamp()
                    )
                """)


        print("")
        print(
            "==================================================="
        )

        print(
            f"ECNAME           : {ecname}"
        )

        print(
            f"Batch            : {batch_id}"
        )

        print(
            f"Files processed   : "
            f"{len(successful_files)}"
        )

        print(
            f"Records merged    : "
            f"{merged_count}"
        )

        print(
            f"Records deleted   : "
            f"{deleted_count}"
        )

        print(
            f"Schema rejected   : "
            f"{len(schema_error_records)}"
        )

        print(
            "Status             : SUCCESS"
        )

        print(
            "==================================================="
        )


    except Exception as e:

        print(
            f"[ERROR] Batch {batch_id} failed: {e}"
        )

        log_pipeline_error(
            "STEP_13_FOREACH_BATCH",
            str(e)
        )

        # VERY IMPORTANT:
        #
        # Raise exception so Structured Streaming
        # considers the micro-batch failed.
        #
        # Auto Loader checkpoint will not move
        # past the failed batch.

        raise


# COMMAND ----------

# STEP 14 — START AUTO LOADER STREAM
# ============================================================
#
# trigger(availableNow=True)
#
# This means:
#
# job task starts (on a schedule, or a file-arrival trigger)
#      ↓
# Auto Loader discovers currently available files, capped per
# micro-batch by maxFilesPerTrigger / maxBytesPerTrigger above
#      ↓
# processes them across one or more bounded micro-batches
#      ↓
# foreachBatch runs for each
#      ↓
# once fully caught up, the stream STOPS on its own
#      ↓
# cluster can terminate - no idle compute cost
#
# With trigger(availableNow=True), the stream stops on its
# own once all currently available files are processed.
#
# HOW TO SCHEDULE THIS NOTEBOOK — two options, not mutually
# exclusive:
#
# 1) Cron / interval schedule (simple, predictable cost)
#    Schedule the Job every N minutes (5-15 min is typical).
#    Good default when file arrivals are fairly steady through
#    the day. Worst-case latency = the interval. On serverless
#    job compute, startup overhead per run is low (seconds),
#    so short intervals are cheap even with 30 tasks.
#
# 2) File-arrival trigger (lower latency, event-driven)
#    Databricks Jobs support a native "file arrival" trigger
#    that watches a storage location and starts the job when
#    new files land, with min/max time between runs configured
#    on the trigger itself (e.g. "wait at least 60s between
#    runs" to coalesce a burst of files into one run instead of
#    firing once per file). This is worth adding when:
#      - latency matters (near-real-time ingestion), or
#      - arrivals are bursty/unpredictable and a fixed cron
#        interval would either run needlessly often (idle
#        cost, tiny/empty micro-batches) or too rarely
#        (latency during quiet periods).
#    It is generally NOT worth it if files land on a steady,
#    predictable cadence — a cron schedule gets you the same
#    outcome with a simpler, easier-to-reason-about setup and
#    one less moving part (the file-arrival trigger itself
#    depends on cloud-provider event notifications being
#    correctly configured).
#    A sensible middle ground for this pipeline: keep the file-
#    arrival trigger with a generous min-interval (e.g. 5-10
#    min) as a backstop, OR just run cron every 5-10 min - both
#    land you in a similar place operationally. availableNow is
#    what makes either choice cheap, since the cluster still
#    only runs while there's a backlog to drain.
#
# HANDLING A SUDDEN LARGE FILE DROP
#    availableNow=True on its own will try to discover and
#    queue up the ENTIRE backlog before starting. maxFilesPer-
#    Trigger / maxBytesPerTrigger (STEP 11) bound each micro-
#    batch instead, so a large drop is drained as many smaller,
#    checkpointed batches within the same run rather than one
#    unbounded one. If a task is chronically hitting a job
#    timeout under load, that's a signal to also shorten the
#    schedule interval (more, smaller runs) or size the job
#    compute up for that ecname specifically, rather than
#    raising the per-trigger caps back up.
#
# SCHEMA EVOLUTION
#    Already handled deliberately in STEP 11 via
#    cloudFiles.schemaEvolutionMode = "rescue": new/unmapped
#    source fields are captured in _rescued_data rather than
#    being auto-added to the target table. No change needed
#    here unless you also want an alert when rescued_data is
#    non-empty for a given ecname (i.e. upstream added a field
#    nobody has mapped yet) - that would be a monitoring query
#    against the target table, not an Auto Loader option.
#
# ============================================================

query = (
    source_stream
    .writeStream
    .foreachBatch(
        process_batch
    )
    .option(
        "checkpointLocation",
        checkpoint_location
    )
    .trigger(
        availableNow=True
    )
    .start()
)


print(
    f"Auto Loader started for {ecname}"
)
