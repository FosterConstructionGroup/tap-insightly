import os
import time
import asyncio
import singer.metrics as metrics
from datetime import datetime


# constants
base_url = "https://api.insightly.com/v3.1/"
pageSize = 500

sem = asyncio.Semaphore(4)

# Rate-limited to 5 requests per second per https://api.insightly.com/v3.1/Help#!/Overview/Technical_Details
# API docs claim 10 per second, but in testing, anything above 5/s throws a 429
# Note that the API seems to 429 on both concurrency and frequency, so need to use both the rate limiter and semaphore techniques
# Adapted slightly from https://quentin.pradet.me/blog/how-do-you-rate-limit-calls-with-aiohttp.html
class RateLimiter:
    rate = 4  # requests per second

    def __init__(self, client):
        self.client = client
        self.tokens = self.rate
        self.updated_at = time.monotonic()

    async def get(self, *args, **kwargs):
        await self.wait_for_token()
        return self.client.get(*args, **kwargs)

    async def wait_for_token(self):
        while self.tokens < 1:
            self.add_new_tokens()
            await asyncio.sleep(0.1)
        self.tokens -= 1

    # would be nice to just make this an async loop but you can't do that easily in Python, unlike Node
    def add_new_tokens(self):
        now = time.monotonic()
        time_since_update = now - self.updated_at
        new_tokens = time_since_update * self.rate
        if self.tokens + new_tokens >= 1:
            self.tokens = min(self.tokens + new_tokens, self.rate)
            self.updated_at = now


async def get_generic(session, source, url, qs={}):
    async with sem:
        with metrics.http_request_timer(source) as timer:
            query_string = build_query_string({"count_total": "true", **qs})
            url = (
                base_url
                + url
                + (
                    # can only filter from the /search endpoint
                    ""
                    if "date_updated_utc" not in qs
                    else "/Search"
                )
                + query_string
            )
            async with await session.get(url) as resp:
                timer.tags[metrics.Tag.http_status_code] = resp.status
                resp.raise_for_status()
                return (await resp.json()), resp


async def get_all_pages(session, source, url, extra_query_string={}):
    skip = 0

    while True:
        json, resp = await get_generic(
            session,
            source,
            url,
            {**extra_query_string, "skip": skip, "top": pageSize},
        )
        yield json
        skip += pageSize
        total = resp.headers.get("x-total-count")
        if total == None or skip > int(total):
            break


def get_endpoint(resource):
    endpoints = {"pipeline_stages": "PipelineStages"}
    # get endpoint if available, default to just resource name
    return endpoints.get(resource, resource)


def formatDate(dt):
    return datetime.strftime(dt, "%Y-%m-%d %H:%M:%S")


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def build_query_string(dict):
    if len(dict) == 0:
        return ""

    return "?" + "&".join(["{}={}".format(k, v) for k, v in dict.items()])


def transform_record(properties, record):
    for key in record:
        if key in properties:
            prop = properties.get(key)
            # Replace linebreak character \n in strings with a space
            if "string" in prop.get("type", []):
                record[key] = (
                    record[key].replace("\n", " ") if record[key] is not None else None
                )

    return record
