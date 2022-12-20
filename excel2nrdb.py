import pandas as pd
import click
import requests
import datetime
import gzip
import math

@click.command()
@click.option('--excel', type=str, required=True, help='Path to excel file.')
@click.option('--sheet', type=str, required=True, help='Sheet name to import.')
@click.option('--event', type=str, required=True, help='NRDB event name to import into.')
@click.option('--api', type=str, required=True, help='New Relic ingest api key.')
@click.option('--accountid', type=int, required=True, help='New Relic account id.')
@click.option('--date', type=str, show_default=True, required=True, default=datetime.datetime.now().strftime("%Y-%m-%d"), help='Date for event records.')
def main(excel, sheet, event, api, accountid, date):
    """This script imports an excel sheet into NRDB as an event. All options can be set through environment variables. Simply prefix the option with NR. For example, set sheet using NR_SHEET environment variable."""
    timestamp = int(datetime.datetime.timestamp(datetime.datetime.strptime(date,"%Y-%m-%d")))
    df = pd.read_excel(excel, sheet_name=sheet)
    df.columns = df.columns.str.replace(' ', '_')
    df['timestamp'] = timestamp
    df['eventType'] = event
    df.fillna("none", inplace=True)
    json_one_size = len(gzip.compress(df.head(1).to_json(orient="records").encode('utf-8')))
    rows_per_payload = int(1000000 / json_one_size*0.8)
    total_rows = df.shape[0]
    for start in range(0, total_rows, rows_per_payload):
        stop = start+rows_per_payload
        print(f"Sending rows from {start}")
        compressed_payload = gzip.compress(df.iloc[start:stop].to_json(orient="records").encode('utf-8'))
        session = requests.Session()
        session.headers.update({
            "Api-Key": api,
            "Content-Type": "application/json",
            "Content-Encoding": "gzip"
        })
        url = f"https://insights-collector.newrelic.com/v1/accounts/{accountid}/events"
        response = session.post(url, data = compressed_payload)
        response.raise_for_status()
        print(f"Response status {response.status_code}")


if __name__ == '__main__':
    main(auto_envvar_prefix="NR")