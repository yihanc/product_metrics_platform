#!/usr/bin/python3
import datetime

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import plotly
from dash.dependencies import Input, Output
from kafka import KafkaConsumer, TopicPartition
import json

# pip install pyorbital
from pyorbital.orbital import Orbital
satellite = Orbital('TERRA')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div(
    html.Div([
        html.H4('TERRA Satellite Live Feed'),
        html.Div(id='live-update-text'),
        # LINE CHART
        dcc.Graph(
            id='graph_line',
            config={
                'showSendToCloud': True,
                'plotlyServerURL': 'https://plot.ly'
            }
        ),
        # BAR CHART
        dcc.Graph(
            id='graph_bar',
            config={
                'showSendToCloud': True,
                'plotlyServerURL': 'https://plot.ly'
            }
        ),
        dcc.Interval(
            id='interval-component',
            interval=5*1000, # in milliseconds
            n_intervals=0
        )
    ])
)



# Load live data from Kafka
def kafka_load_dau(seconds=1800):
    dau = {}

    consumer = KafkaConsumer('posthistory', bootstrap_servers=['172.31.7.229:9092'], auto_offset_reset='earliest', enable_auto_commit=True, auto_commit_interval_ms=1000)

    # Finding end offset so that we could stop the loop.
    next(consumer)
    partition = consumer.assignment().pop()
    end_offset = consumer.end_offsets([partition])[partition] - 1

    for raw_msg in consumer:
        msg = raw_msg.value.decode('utf-8')
        msg_data = json.loads(msg)
        ts = msg_data['_CreationDate'][:16]
        dau[ts] = dau.get(ts, 0) + 1
        if raw_msg.offset == end_offset:
            break
    return dau


@app.callback(Output('live-update-text', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_metrics(n):
    # Latest
    dau = kafka_load_dau(seconds=1800)
    dau_str = json.dumps(dau)

    lon, lat, alt = satellite.get_lonlatalt(datetime.datetime.now())
    style = {'padding': '5px', 'fontSize': '30px'}

    return [ html.Span('Message: {}'.format(json.dumps(dau)), style=style)]


# Multiple components can update everytime interval gets fired.
@app.callback(
    [Output('graph_line', 'figure'),
    Output('graph_bar', 'figure')],
    [Input('interval-component', 'n_intervals')])
def update_graph_live(n):
    # Latest
    dau = kafka_load_dau(seconds=1800)
    dau_str = json.dumps(dau)

    datetime.datetime.now().strftime('%Y-%m-%dT%H-%M')
    minutes = 30 # minutes

    data = {
        'time': [],
        'dau': [],
    }

    # Collect data
    for i in range(minutes):
        now = datetime.datetime.now()
        before = now - datetime.timedelta(seconds=i*60)

        key = before.strftime('%Y-%m-%dT%H-%M')
        value = dau[key] if key in dau else 0

        data['time'].append(before)
        data['dau'].append(value)
        
    result_line = {
        'data': [{
            'type': 'scatter',
            'x': data['time'],
            'y': data['dau'],
        }],
        'layout': {
            'title': value
        }
    }

    result_bar = {
        'data': [
            go.Bar(
                x=data['time'],
                y=data['dau'],
                name='Bar',
                marker=go.bar.Marker(
                    color='rgb(55, 83, 109)'
                )
            ),
        ],
        'layout': go.Layout(
            title='Bar Chart',
            showlegend=True,
            legend=go.layout.Legend(
                x=0,
                y=1.0
            ),
            margin=go.layout.Margin(l=40, r=0, t=40, b=30)
        ),
    }

    return result_line, result_bar



if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0", port=80)
