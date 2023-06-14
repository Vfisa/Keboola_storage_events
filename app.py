import streamlit as st
import requests
import json
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import scipy.stats as stats
from sklearn.ensemble import IsolationForest


# Static variables
MAX_PAGE = 100000

stacks = {
    "US": "https://connection.keboola.com/v2/storage/tables",
    "EU-N": "https://connection.north-europe.azure.keboola.com/v2/storage/tables",
    "EU-C": "https://connection.eu-central-1.keboola.com/v2/storage/tables"
}

# Sidebar
st.sidebar.header("Keboola Table Imports")
stack_selection = st.sidebar.radio("Stack:", list(stacks.keys()))
# User-entered variables
TOKEN = st.sidebar.text_input("Storage Token", type="password")
TABLEID = st.sidebar.text_input("Table ID")
URL = stacks[stack_selection] + "/" + TABLEID + "/events"


# Request preferences
headers = {
    'X-StorageApi-Token': TOKEN,
    "Accept": "application/json"
}
params = {
    "q": "event:storage.tableImportDone",
    "limit": 100,
    "offset": 0
}

def grab_events(URL):
    with st.expander("geeky stuff", expanded=False):
        st.write("Grabbing list of events from API")
        st.write(URL)
    
        data_response = []
        raw_data = pd.DataFrame(data_response)
        params["offset"] = 0

        while params["offset"] < MAX_PAGE:
            r = requests.get(URL, headers=headers, params=params)
            st.write(r.status_code, ":", r.url)
            new_data = pd.DataFrame(json.loads(r.text))

            #raw_data = raw_data.append(pd.DataFrame(json.loads(r.text)), ignore_index=True)
            raw_data = pd.concat([raw_data, new_data], axis=0, ignore_index=True)

            if len(json.loads(r.text)) == 0:
                params["offset"] = MAX_PAGE
            params["offset"] = params["offset"] + params["limit"]
        
    #st.write("Table events acquired")
    st.success("Table events acquired", icon="✅")
    
    return raw_data


# Streamlit app
def main():
    stats_df = data[["id","event","component","type","runId","created","params","results","performance","token"]]
    stats_df["table_event_id"] = stats_df["id"]
    stats_df = stats_df[stats_df['event'] == 'storage.tableImportDone']

    # Column names that contain JSON
    del_list = ["id","token","performance", "warnings", "results", "event","component"]
    normalize_list = ["token", "performance","results"]

    for b in normalize_list:
        try:
            stats_df = pd.concat([stats_df, pd.json_normalize(stats_df[b])], axis=1)
        except:
            st.write("Issue when normalizing {}".format(a))

    for a in del_list:
        try:
            stats_df.drop(a, axis=1, inplace=True)
        except:
            #st.write("Issue when deleting {}".format(a))
            st.warning("Issue when deleting {}".format(a), icon="⚠️")

    normalize_list = ["params"]
    del_list = ["params","importId","withoutHeaders", "source.origin", "source.fileId", "source.fileName","csv.delimiter","csv.enclosure","csv.escapedBy","fromSnapshot","async","source.tableName","source.type","source.dataObject","source.workspaceId","columns"]

    for b in normalize_list:
        try:
            stats_df = pd.concat([stats_df, pd.json_normalize(stats_df[b])], axis=1)
        except:
            st.write("Issue when normalizing {}".format(a))

    for a in del_list:
        try:
            stats_df.drop(a, axis=1, inplace=True)
        except:
            #st.write("Issue when deleting {}".format(a))
            st.warning("Issue when deleting {}".format(a), icon="⚠️")

    stats_df["created"] = pd.to_datetime(stats_df['created'])
    stats_df["rowsCount"] = stats_df["rowsCount"].astype(int)
    stats_df = stats_df.sort_values('created', ascending=True)

    stats_df["lag_columns"] = stats_df["importedColumns"].shift()
    stats_df['lag_rows'] = stats_df["rowsCount"].shift()

    stats_df["importedColumns"] = stats_df["importedColumns"].tolist()
    stats_df["lag_columns"] = stats_df["lag_columns"].tolist()

    stats_df['schema_evolution'] = stats_df['importedColumns'].equals(stats_df['lag_columns'])
    stats_df['rows_delta'] = stats_df['rowsCount'] - stats_df['lag_rows']

    del_list = ["lag_columns", "lag_rows"]

    for a in del_list:
        try:
            stats_df.drop(a, axis=1, inplace=True)
        except:
            #st.write("Issue when deleting {}".format(a))
            st.warning("Issue when deleting {}".format(a), icon="⚠️")

    stats_df["rows_delta"] = stats_df["rows_delta"].fillna(0)

    st.header("Imported Row Count")
    fig = px.line(
        stats_df,
        x="created",
        y="rowsCount",
        title="Table row count",
        markers=True
    )
    st.plotly_chart(fig)

    st.header("Import Duration")
    fig = px.line(
        stats_df,
        x="created",
        y="importDuration",
        title="Table import duration (s)",
        markers=True
    )
    st.plotly_chart(fig)

    st.header("Table size (Bytes)")
    fig = px.line(
        stats_df,
        x="created",
        y="sizeBytes",
        title="Table size (Bytes)",
        markers=True
    )
    st.plotly_chart(fig)

    st.header("Row increments")
    fig = px.line(
        stats_df,
        x="created",
        y="rows_delta",
        title="Row increments",
        markers=True
    )
    st.plotly_chart(fig)

    st.header("Schema Evolution")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=stats_df["created"],
        y=stats_df["schema_evolution"],
        mode='markers',
        line=dict(color='blue', width=1, dash='dot'),
        name='schema_evolution'
    ))
    st.plotly_chart(fig)

    zscore_rate = stats.zscore(stats_df["rows_delta"])
    stats_df = stats_df.assign(zscore=zscore_rate)

    model = IsolationForest(n_estimators=50, max_samples='auto', contamination=float(0.1), max_features=1.0)
    model.fit(stats_df[["rows_delta"]])

    stats_df["scores"] = model.decision_function(stats_df[["rows_delta"]])
    stats_df['anomaly'] = model.predict(stats_df[["rows_delta"]])

    stats_df['anomaly'] = stats_df['anomaly'].replace(1, "NaN")
    stats_df['anomaly'] = stats_df['anomaly'].replace(-1, 1)

    st.header("Anomalies")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=stats_df["created"],
        y=stats_df["rowsCount"],
        mode='lines+markers',
        line=dict(color='blue', width=1, dash='dot'),
        name='rows'
    ))

    fig.add_trace(go.Scatter(
        x=stats_df["created"],
        y=stats_df["zscore"],
        mode='lines+markers',
        line=dict(color='green', width=1),
        name='zscore'
    ))

    fig.add_trace(go.Scatter(
        x=stats_df["created"],
        y=stats_df["anomaly"],
        mode='markers',
        line=dict(color='red', width=1),
        name='anomaly'
    ))
    st.plotly_chart(fig)

if __name__ == '__main__':
    if st.sidebar.button('Get Events'):
        if not TOKEN or not TABLEID:
            st.warning('Please provide both Table ID and Storage Token.')
        else:
            data = grab_events(URL)
            main()