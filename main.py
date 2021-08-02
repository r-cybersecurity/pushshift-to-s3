import argparse, boto3, json, logging, os, requests, time, yaml, zlib
from datetime import datetime
from multiprocessing.pool import ThreadPool

from config import *


def pull_subreddit(subreddit, item_type):
    item_count = 0
    retry_count = 0
    additional_backoff = 1
    start_epoch = int(datetime.utcnow().timestamp())
    previous_epoch = start_epoch
    item_count = 0
    pool = ThreadPool(processes=s3_put_processes)

    logger.info(f"pushshift-to-s3: Ingesting {item_type}s from {subreddit}")

    while True:
        new_url = pushshift_query_url.format(item_type, subreddit) + str(previous_epoch)

        try:
            fetched_data = requests.get(
                new_url, headers=pushshift_query_headers, timeout=8
            )
        except Exception as e:
            additional_backoff = additional_backoff * 2
            logger.info(f"pushshift-to-s3: Backing off due to api error: {e}")
            retry_count = retry_count + 1
            time.sleep(additional_backoff)
            if retry_count >= 13:
                break
            continue

        try:
            json_data = fetched_data.json()
        except Exception as e:
            additional_backoff = additional_backoff * 2
            logger.info(f"pushshift-to-s3: Backing off due to json error: {e}")
            retry_count = retry_count + 1
            time.sleep(additional_backoff)
            if retry_count >= 13:
                break
            continue

        if "data" not in json_data:
            additional_backoff = additional_backoff * 2
            logger.info(f"pushshift-to-s3: Backing off due to data error: no data")
            retry_count = retry_count + 1
            time.sleep(additional_backoff)
            if retry_count >= 13:
                break
            continue

        items = json_data["data"]
        retry_count = 0
        additional_backoff = 1

        if len(items) == 0:
            logger.info(
                f"pushshift-to-s3: Pushshift API returned no more {item_type}s for {subreddit}"
            )
            break

        clean_items = []

        for item in items:
            if not "id" in item.keys():
                logger.critical(
                    f"pushshift-to-s3: No 'id' in result, cannot create key"
                )
            else:
                if not "created_utc" in item.keys():
                    logger.warning(
                        f"pushshift-to-s3: No 'created_utc' in result {item['id']}, may cause loop"
                    )
                else:
                    previous_epoch = item["created_utc"] - 1

                item_count += 1
                clean_items.append([item, item_type])

        tempstamp = datetime.fromtimestamp(previous_epoch).strftime("%Y-%m-%d")
        logger.info(
            f"pushshift-to-s3: Retrieved {item_count} {item_type}s through {tempstamp}"
        )

        pool.map(s3_upload, clean_items)

        logger.info(
            f"pushshift-to-s3: Archived {item_count} {item_type}s through {tempstamp}"
        )

        if args.update:
            update_limit_in_seconds = args.update * 60 * 60 * 24
            if start_epoch - previous_epoch > update_limit_in_seconds:
                logger.info(
                    f"pushshift-to-s3: Stopping pull for {subreddit} due to update flag"
                )
                break


def s3_upload(packed_item):
    item = packed_item[0]
    item_type = packed_item[1]

    key = f"{subreddit}/{item_type}/{item['id']}.zz"
    body = zlib.compress(str.encode(json.dumps(item)), level=9)

    logger.debug(f"pushshift-to-s3: Attempting to save {key}")
    client.put_object(Bucket=s3_bucket_name, Key=key, Body=body)
    logger.debug(f"pushshift-to-s3: Saved {key} successfully")


parser = argparse.ArgumentParser(
    description=(
        "Consumes Reddit data from Pushshift, compresses, then stores it in S3 in parallel. "
        "Optionally, pulls only the most recent data, or pulls from a YAML file of subreddits. "
    )
)
parser.add_argument(
    "-u",
    "--update",
    type=int,
    help="How many days in the past we should fetch data for",
)
parser.add_argument(
    "-t",
    "--type",
    help="Changes type of data to fetch (default: submission) (can use 'both')",
    choices={"comment", "submission", "both"},
    default="submission",
)
parser.add_argument(
    "-s",
    "--subreddits",
    type=str,
    nargs="+",
    help="List of subreddits to fetch data for; can also be a YAML file",
)
parser.add_argument(
    "-l",
    "--log",
    type=str,
    help="File to put logs out to",
)
parser.add_argument(
    "-d", "--debug", help="Output a metric shitton of runtime data", action="store_true"
)
parser.add_argument(
    "-v",
    "--verbose",
    help="Output a reasonable amount of runtime data",
    action="store_true",
)

args = parser.parse_args()

if args.debug:
    log_level = logging.DEBUG
elif args.verbose:
    log_level = logging.INFO
else:
    log_level = logging.WARNING

if args.log:
    logging.basicConfig(
        filename=args.log,
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=log_level,
    )
else:
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s", level=log_level
    )
logger = logging.getLogger()

session = boto3.session.Session()
client = session.client(
    "s3",
    region_name=s3_region_name,
    endpoint_url=s3_endpoint_url,
    aws_access_key_id=s3_access_key_id,
    aws_secret_access_key=s3_secret_key,
)

subreddits = []
if os.path.isfile(args.subreddits[0]):
    with open(args.subreddits[0], "r") as config_file:
        yaml_config = yaml.safe_load(config_file)
        for classification, subreddits_from_classification in yaml_config.items():
            for subreddit_from_classification in subreddits_from_classification:
                subreddits.append(subreddit_from_classification)
else:
    subreddits = args.subreddits

types_to_fetch = []
if args.type == "both":
    types_to_fetch.append("submission")
    types_to_fetch.append("comment")
else:
    types_to_fetch.append(args.type)

for type_to_fetch in types_to_fetch:
    for subreddit in subreddits:
        pull_subreddit(subreddit, type_to_fetch)
