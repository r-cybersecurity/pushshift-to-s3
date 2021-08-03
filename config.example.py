pushshift_query_url = (
    "https://api.pushshift.io/reddit/{}/search?limit=100&sort=desc&subreddit={}&before="
)
pushshift_query_headers = {"User-Agent": "r-cybersecurity/pushshift_to_s3"}

s3_region_name = "sfo3"
s3_endpoint_url = "https://sfo3.digitaloceanspaces.com"
s3_access_key_id = ""
s3_secret_key = ""

pool_subreddits = 2
pool_s3_threads_per_subreddit = 2

pushshift_timeout = 10
pushshift_retries = 10