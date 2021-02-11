# tap-insightly

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the Insightly API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Insightly API](https://api.insightly.com/)
- Extracts the following resources from Insightly:
  - contacts
  - links
  - pipelines
  - pipeline stages
  - opportunities
  - organisations
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

1. Install

   We recommend using a virtualenv:

   ```bash
   > virtualenv -p python3 venv
   > source venv/bin/activate
   > pip install -e .
   ```

2. Get your Insightly API key

   Follow [these instructions](https://support.insight.ly/hc/en-us/articles/204864594-Finding-your-Insightly-API-key) to find your Insightly API key.

3. Create the config file

   Create a JSON file called `config.json` containing the access token you were provided.

   ```json
   { "api_key": "yourapikey" }
   ```

4. Run the tap in discovery mode to get properties.json file

   ```bash
   tap-insightly --config config.json --discover > properties.json
   ```

5. In the properties.json file, select the streams to sync

   Each stream in the properties.json file has a "schema" entry. To select a stream to sync, add `"selected": true` to that stream's "schema" entry. For example, to sync the `opportunities` stream:

   ```
   ...
   "tap_stream_id": "opportunities",
   "schema": {
     "selected": true,
     "properties": {
   ...
   ```

6. Run the application

   `tap-insightly` can be run with:

   ```bash
   tap-insightly --config config.json --properties properties.json
   ```
