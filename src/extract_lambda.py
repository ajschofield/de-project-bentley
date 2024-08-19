import csv
import json
import logging
import re
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from pg8000.native import Connection, InterfaceError, identifier

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# DB Exception class


class DBConnectionException(Exception):
    """Wraps pg8000.native Error or DatabaseError."""

    def __init__(self, e):
        """Initialise with provided error message."""
        self.message = str(e)
        super().__init__(self.message)


def lambda_handler(event, context):
    """This lambda function connects to the Totesys database, lists the contents of the ingestion bucket,
    and converts all tables to CSV and if any of those tables do not exist in, or are different to the ones in s3, it uploads them
    it uses 3 helper functions to achieve these 3 functionalities
    """
    try:
        db = connect_to_database()
        existing_files = list_existing_s3_files()
        any_changes = process_and_upload_tables(db, existing_files)

        if not any_changes["updated"]:
            logger.info("No changes detected in the database.")
            return {
                "statusCode": 200,
                "body": json.dumps("No changes detected, no CSV files were uploaded."),
            }
        return {
            "statusCode": 200,
            "body": json.dumps(
                f"""CSV files processed for {', '.join(any_changes['updated'])} and uploaded successfully.{
                'The following tables were not updated: '+', '.join(any_changes['no change']) if any_changes['no change'] else ''}"""
            ),
        }
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"statusCode": 500, "body": json.dumps("Internal server error.")}
    finally:
        if db:
            db.close()


def retrieve_secrets():
    secret_name = "bentley-secrets"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Failed to retrieve secret {secret_name}: {str(e)}")
        raise e
    except KeyError:
        logger.error(f"Secret {secret_name} does not contain a SecretString")
        raise ValueError(f"Secret {secret_name} does not contain a SecretString")

    return get_secret_value_response["SecretString"]


def connect_to_database() -> Connection:
    try:
        secrets = retrieve_secrets()
        host = secrets["host"]
        port = secrets["port"]
        user = secrets["user"]
        password = secrets["password"]
        database = secrets["database"]

        return Connection(
            database=database, user=user, password=password, host=host, port=port
        )
    except InterfaceError as i:
        logger.error(f"Interface error: {i}")
        raise DBConnectionException("Failed to connect to database")


def extract_bucket(client=boto3.client("s3")):
    response = client.list_buckets()
    extract_bucket_filter = [
        bucket["Name"] for bucket in response["Buckets"] if "extract" in bucket["Name"]
    ]

    return extract_bucket_filter[0]


def list_existing_s3_files(bucket_name=extract_bucket(), client=boto3.client("s3")):
    """Creates a dictionary and populates it with the
    results of listing the contents of the s3 bucket, then
    returns the populated dictionary
    """

    existing_files = {}

    try:
        response = client.list_objects_v2(Bucket=bucket_name)

        if "Contents" in response:
            for obj in response["Contents"]:
                s3_key = obj["Key"]
                try:
                    file_obj = client.get_object(Bucket=bucket_name, Key=s3_key)
                    file_content = file_obj["Body"].read().decode("utf-8")
                    existing_files[s3_key] = file_content
                except ClientError as e:
                    logger.error(f"Error retrieving S3 object {s3_key}: {e}")
        else:
            logger.error("The bucket is empty")

    except ClientError as e:
        logger.error(f"Error listing S3 objects: {e}")

    return existing_files


def get_latest_timestamp(existing_files):
    all_datetimes = []
    for file_name in existing_files.keys():
        match = re.search(r"\/(.+/).+_(.+)\.csv", file_name)
        if match:
            datetime_str = "".join(match.group(1, 2))
            all_datetimes.append(datetime.strptime(datetime_str, "%Y/%m/%d/%H:%M:%S"))
    return max(all_datetimes) if all_datetimes else datetime.min


def process_and_upload_tables(db, existing_files, client=boto3.client("s3")):
    """Creates a list of the tables from a database query and
    then selects everything from each table in individual queries
    it then writes each table to CSV files and compares with the item
    in the existing_files dictionary with the same name. If it finds any changes
    to files, or new tables/files it uploads them to the s3 bucket
    """
    load_status = {"updated": [], "no change": []}
    latest_timestamp = get_latest_timestamp(existing_files)

    tables = db.run(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE';
        """
    )

    for table in tables:
        table_name = table[0]
        rows = db.run(
            f"SELECT * FROM {identifier(table_name)} WHERE last_updated >= :latest;",
            latest={datetime.strftime(latest_timestamp, "%Y-%m-%d %H:%M:%S")},
        )
        # Creating a temporary file path and writing the column name to it followed by each row of data
        if rows:
            csv_file_path = f"/tmp/{table_name}.csv"
            with open(csv_file_path, "w", newline="") as file:
                writer = csv.writer(file)
                # column_names = [desc["name"] for desc in db.columns(f"SELECT * FROM {table_name};")]
                column_names = [
                    col_name[0]
                    for col_name in db.run(
                        """SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS 
                                       WHERE table_name = :table ;""",
                        table=table_name,
                    )
                ]
                writer.writerow(column_names)
                writer.writerows(rows)
            s3_key = datetime.strftime(
                datetime.today(), f"{table_name}/%Y/%m/%d/{table_name}_%H:%M:%S.csv"
            )

            # Writing the new file to S3 extract bucket:
            try:
                client.upload_file(csv_file_path, extract_bucket(), s3_key)
                load_status["updated"].append(table_name)
                logger.info(f"Uploaded {s3_key} to S3.")
            except ClientError as e:
                logger.error(f"Error uploading to S3: {e}")
        else:
            load_status["no change"].append(table_name)
            logger.info(f"No new data")
    return load_status
