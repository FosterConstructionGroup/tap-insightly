import os
import json
import asyncio
import aiohttp
import singer
from singer import metadata

from tap_insightly.utility import get_abs_path, RateLimiter
from tap_insightly.config import ID_FIELDS, HAS_LINKS
from tap_insightly.fetch import write_bookmark, handle_resource

logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["api_key"]


def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), "table-key-properties", [ID_FIELDS[schema_name]])

    for field_name in schema["properties"].keys():
        mdata = metadata.write(
            mdata,
            ("properties", field_name),
            "inclusion",
            "automatic" if field_name == ID_FIELDS[schema_name] else "available",
        )

    return mdata


def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # get metadata for each field
        mdata = populate_metadata(schema_name, schema)

        # create and add catalog entry
        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "metadata": metadata.to_list(mdata),
            "key_properties": [ID_FIELDS[schema_name]],
        }
        streams.append(catalog_entry)

    return {"streams": streams}


def do_discover():
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))


def get_selected_streams(catalog):
    """
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    """
    selected_streams = []
    for stream in catalog["streams"]:
        stream_metadata = stream["metadata"]
        if stream["schema"].get("selected", False):
            selected_streams.append(stream["tap_stream_id"])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry["breadcrumb"] and entry["metadata"].get("selected", None):
                    selected_streams.append(stream["tap_stream_id"])

    return selected_streams


def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog["streams"]:
        if stream["tap_stream_id"] == stream_id:
            return stream
    return None


async def do_sync(session, state, catalog, selected_stream_ids):
    # Only write links schema if it will be synced
    # Note that empty lists are falsy
    sync_links = "links" in selected_stream_ids and [
        s for s in selected_stream_ids if s in HAS_LINKS
    ]
    links_schema = None  # fixes linting error
    if sync_links:
        links_stream = next(
            s for s in catalog["streams"] if s["tap_stream_id"] == "links"
        )
        links_schema = links_stream["schema"]
        singer.write_schema("links", links_schema, links_stream["key_properties"])

    streams_to_sync = [
        stream
        for stream in catalog["streams"]
        if stream["tap_stream_id"] in selected_stream_ids
        and stream["tap_stream_id"] != "links"
    ]

    streams_futures = []

    for stream in streams_to_sync:
        stream_id = stream["tap_stream_id"]
        stream_schema = stream["schema"]
        mdata = stream["metadata"]

        singer.write_schema(stream_id, stream_schema, stream["key_properties"])

        schemas = {stream_id: stream_schema}
        if sync_links and stream_id in HAS_LINKS:
            schemas["links"] = links_schema

        streams_futures.append(
            handle_resource(
                session, stream_id, schemas, ID_FIELDS[stream_id], state, mdata
            )
        )

    # await everything except links
    streams_resolved = await asyncio.gather(*streams_futures)

    # update bookmark by merging in all streams
    for (resource, extraction_time, _links_futures) in streams_resolved:
        state = write_bookmark(state, resource, extraction_time)
    singer.write_state(state)

    # await all links
    await asyncio.gather(*[link for (_r, _e, ls) in streams_resolved for link in ls])


async def run_async(config, state, catalog):
    selected_stream_ids = get_selected_streams(catalog)

    # Insightly API uses basic auth. Pass API key as the username and leave the password blank
    auth = aiohttp.BasicAuth(login=config["api_key"], password="")
    async with aiohttp.ClientSession(auth=auth) as session:
        session = RateLimiter(session)
        await do_sync(session, state, catalog, selected_stream_ids)


@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover()
    else:
        catalog = args.properties if args.properties else get_catalog()
        asyncio.get_event_loop().run_until_complete(
            run_async(args.config, args.state, catalog)
        )


if __name__ == "__main__":
    main()
