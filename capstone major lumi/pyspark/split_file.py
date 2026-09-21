import argparse
import json
import math
import os
from pyspark.sql import SparkSession

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument( "--input_file", required=True)
    parser.add_argument( "--output_dir",required=True)
    parser.add_argument("--threshold_kb", type=int,required=True)
    return parser.parse_args()

def split_json(spark, input_file, output_dir, threshold_kb):
    # Read JSON records using Spark
    df = ( spark.read.option("multiLine", "true").json(input_file))
    record_count = df.count()
    if record_count == 0:
        return []

    # Number of desired partitions based on file size.
    file_size_bytes = os.path.getsize(input_file)
    threshold_bytes = threshold_kb * 1024
    num_parts = max( 1, math.ceil(file_size_bytes / threshold_bytes))
    print(f"Input file size: {file_size_bytes} bytes")
    print(f"Threshold: {threshold_bytes} bytes")
    print(f"Records: {record_count}")
    print(f"Partitions: {num_parts}")

    # Repartition the records
    df = df.repartition(num_parts)
    os.makedirs(output_dir, exist_ok=True)

    # Convert each Spark partition into one JSON array.
    def process_partition(iterator):
        records = []
        for row in iterator:
            records.append(row.asDict(recursive=True))
        if records:
            yield json.dumps(records, ensure_ascii=False)

    rdd = df.rdd.mapPartitions(process_partition)
    chunks = rdd.collect()

    output_files = []

    for index, chunk in enumerate(chunks, start=1):
        output_path = os.path.join( output_dir, f"employees_part_{index:03d}.json")
        with open(output_path, "w",encoding="utf-8") as f:
            f.write(chunk)
        output_files.append(output_path)
        print(f"SPLIT_OUTPUT:{output_path}")
    return output_files

def split_csv(spark, input_file, output_dir, threshold_kb):
    df = (spark.read.option("header", "true").option("inferSchema", "true").csv(input_file))
    record_count = df.count()
    if record_count == 0:
        return []
    file_size_bytes = os.path.getsize(input_file)
    threshold_bytes = threshold_kb * 1024
    num_parts = max(1,math.ceil(file_size_bytes / threshold_bytes))
    print(f"Input file size: {file_size_bytes} bytes")
    print(f"Threshold: {threshold_bytes} bytes")
    print(f"Records: {record_count}")
    print(f"Partitions: {num_parts}")
    df = df.repartition(num_parts)
    os.makedirs(output_dir, exist_ok=True)

    # Write CSV split files.
    temp_dir = os.path.join(output_dir, "csv_temp")
    ( df.write.mode("overwrite").option("header", "true").csv(temp_dir))

    output_files = []
    for filename in os.listdir(temp_dir):
        if filename.endswith(".csv"):
            source_path = os.path.join( temp_dir,filename )
            output_path = os.path.join(output_dir,f"employees_part_{len(output_files) + 1:03d}.csv" )
            os.replace(source_path,output_path)
            output_files.append(output_path)
            print(f"SPLIT_OUTPUT:{output_path}")

    return output_files

def main():
    args = parse_args()
    spark = (SparkSession.builder.appName("LumiFileSplitter").master("local[*]").getOrCreate())

    try:
        extension = os.path.splitext(args.input_file)[1].lower()
        if extension == ".json":
            split_json(spark, args.input_file, args.output_dir,args.threshold_kb)

        elif extension == ".csv":
            split_csv(spark, args.input_file, args.output_dir,args.threshold_kb)
        else:
            raise ValueError(f"Unsupported file extension: {extension}")
    finally:
        spark.stop()
if __name__ == "__main__":
    main()