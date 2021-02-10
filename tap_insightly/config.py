ID_FIELDS = {
    "contacts": "CONTACT_ID",
    "links": "LINK_ID",
    "opportunities": "OPPORTUNITY_ID",
    "organisations": "ORGANISATION_ID",
    "pipelines": "PIPELINE_ID",
    "pipeline_stages": "STAGE_ID",
    "users": "USER_ID",
    "notes": "NOTE_ID",
}

# note that organisations has links but explicitly excluding here to save time and reduce API calls required
HAS_LINKS = set(["contacts", "opportunities", "notes"])
