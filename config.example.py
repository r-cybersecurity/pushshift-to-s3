pushshift_query_url = (
    "https://api.pushshift.io/reddit/{}/search?limit=100&sort=desc&subreddit={}&before="
)
pushshift_query_headers = {"User-Agent": "r-cybersecurity/pushshift_to_s3"}

s3_region_name = "sfo3"
s3_endpoint_url = "https://sfo3.digitaloceanspaces.com"
s3_access_key_id = ""
s3_secret_key = ""
s3_put_processes = 2
