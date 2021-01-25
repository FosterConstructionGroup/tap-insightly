import singer
import singer.metrics as metrics
from singer import metadata
from singer.bookmarks import get_bookmark
from tap_insightly.utility import (
    get_generic,
    get_all_pages,
    get_endpoint,
    formatDate,
)


CAN_FILTER = set(["contacts", "opportunities", "organisations", "users"])


async def handle_resource(session, resource, schemas, id_field, state, mdata):
    extraction_time = singer.utils.now()
    endpoint = get_endpoint(resource)
    bookmark = get_bookmark(state, resource, "since")
    qs = {} if resource not in CAN_FILTER else {"updated_after_utc": bookmark}
    has_links = "links" in schemas
    links_futures = []

    with metrics.record_counter(resource) as counter:
        async for page in get_all_pages(session, resource, endpoint, qs):
            for row in page:
                row = custom_transforms(resource, row)

                write_record(row, resource, schemas[resource], mdata, extraction_time)
                counter.increment()

                if has_links:
                    links_futures.append(
                        handle_links(
                            session,
                            resource,
                            row[id_field],
                            schemas["links"],
                            mdata,
                            extraction_time,
                        )
                    )
    return (resource, extraction_time, links_futures)


async def handle_links(session, parent_resource, parent_id, schema, mdata, dt):
    with metrics.record_counter("links") as counter:
        json, _resp = await get_generic(
            session, "links", f"{parent_resource}/{parent_id}/Links"
        )
        for row in json:
            write_record(row, "links", schema, mdata, dt)
            counter.increment()


def custom_transforms(resource, row):
    # Notes body can be over Redshift's 1k character limit for a column
    if resource == "notes" and "BODY" in row and row["BODY"] != None:
        row["BODY"] = row["BODY"][:999]

    return row


# More convenient to use but has to all be held in memory, so use write_record instead for resources with many rows
def write_many(rows, resource, schema, mdata, dt):
    with metrics.record_counter(resource) as counter:
        for row in rows:
            write_record(row, resource, schema, mdata, dt)
            counter.increment()


def write_record(row, resource, schema, mdata, dt):
    with singer.Transformer() as transformer:
        rec = transformer.transform(row, schema, metadata=metadata.to_map(mdata))
    singer.write_record(resource, rec, time_extracted=dt)


def write_bookmark(state, resource, dt):
    singer.write_bookmark(state, resource, "since", formatDate(dt))
    return state
