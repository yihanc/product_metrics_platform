#!/usr/bin/python3
# -*- coding: utf-8 -*-
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go

import requests, json, subprocess, prestodb, time, datetime
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from smart_open import open
import lxml.etree
from random import randint



################################################################################
###
### Nav Bar
###
################################################################################

PLOTLY_LOGO = "https://images.plot.ly/logo/new-branding/plotly-logomark.png"

nav_item = dbc.NavItem(dbc.NavLink("Link", href="#"))
navbar = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                # Use row and col to control vertical alignment of logo / brand
                dbc.Row(
                    [
                        dbc.Col(html.Img(src=PLOTLY_LOGO, height="50px")),
                        dbc.Col(dbc.NavbarBrand("Product Metrics Platform", className="ml-2")),
                    ],
                    align="true",
                    no_gutters=True,
                ),
                href="https://plot.ly",
            ),
            dbc.NavbarToggler(id="navbar-toggler2"),
            dbc.Collapse(
                dbc.Nav(
                    [nav_item], className="ml-auto", navbar=True
                ),
                id="navbar-collapse2",
                navbar=True,
            ),
        ]
    ),
    color="dark",
    dark=True,
    className="mb-5",
)
################################################################################
###
### APP Start
###
################################################################################
# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])
# app = dash.Dash(external_stylesheets=[dbc.themes.JOURNAL])
# app = dash.Dash(external_stylesheets=[dbc.themes.SKETCHY])

# app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    navbar,
    # html.H1(children='Product Metrics Platform'),

    html.Div(children='''
        Product Metrics Platform: A web application to discover and explore predefined metrics fast and easily.
    '''),

    dcc.Tabs(id="tabs", children=[
        dcc.Tab(label='Tab one', children=[
            html.H3(children='Select a predefined metric and see result: '),
            dcc.Dropdown(
                id='my-dropdown',
                options=[
                    {
                        'label': 'Top 10 Tags',
                        'value': 'select tag_name, cnt from dim_tags limit 10',
                    },
                    {
                    'label': 'Posts Per Year (Slow About  ~1 min)',
                    'value': "select SUBSTR(creation_date,1,4) as year, count(1) as cnt from dim_posts group by 1 order by year"
                    },
                ],
                placeholder="Select a metric",
                value='',
            ),
            dcc.Dropdown(
                options=[
                    {'label': 'New York City', 'value': 'NYC'},
                    {'label': 'Montréal', 'value': 'MTL'},
                    {'label': 'San Francisco', 'value': 'SF'}
                ],
                placeholder='Group By',
                value='',
            ),
            dcc.Dropdown(
                options=[
                    {'label': 'New York City', 'value': 'NYC'},
                    {'label': 'Montréal', 'value': 'MTL'},
                    {'label': 'San Francisco', 'value': 'SF'}
                ],
                placeholder='Filter',
                value='',
            ),
            # BAR CHART
            dcc.Graph(
                id='graph_bar',
                config={
                    'showSendToCloud': True,
                    'plotlyServerURL': 'https://plot.ly'
                }
            ),
        ],style={'width': '49%', 'display': 'block', 'margin':'0, auto'}),
        dcc.Tab(label='Adhoc Query - (Compare Druid and Presto)', children=[
            html.H3(children='Compare Presto and Druid Speed:'),
            html.Div(children='''
                Try type "select count(1) from dim_posts" and see the differences..
            '''),
            dcc.Textarea(
                id="sql-state",
                placeholder='Enter a SQL query... eg: select * from sample limit 10',
                style={'width': '60%'}
            ),
            html.Button(id='submit-button', n_clicks=0, children='Submit'),
            html.H3(children='Presto Result:'),
            html.Div(id='presto-state'),
            html.P(),
            html.H3(children='Druid Result: '),
            html.Div(id='druid-state'),
            html.P(),
        ],style={'width': '49%', 'display': 'block', 'margin':'0, auto'}),
        dcc.Tab(label='Live Data', children=[
            dcc.RadioItems(
                id="live_control_button",
                options=[
                    {'label': 'Stop Data', 'value': 'stop'},
                    {'label': 'Start Data', 'value': 'start'},
                ],
                value='stop'
            ),
            html.H4('Live Active Users per minute:'),
            html.Div(id='live_text'),
            # LINE CHART
            dcc.Graph(
                id='live_graph_line',
                config={
                    'showSendToCloud': True,
                    'plotlyServerURL': 'https://plot.ly'
                }
            ),
            dcc.Interval(
                id='live_interval_component',
                interval=5*1000, # in milliseconds
                n_intervals=0
            ),
            html.Div(id='hidden-div', style={'display':'none'})
        ]),
    ]),
])


################################################################################
###
### CALLBACKS
###
################################################################################
# Graph Line and Graph Bar Call Back
@app.callback(
    [dash.dependencies.Output('graph_bar', 'figure')],
    [dash.dependencies.Input('my-dropdown', 'value')])
def update_bar_output(value):
    # sql = "select SUBSTR(creation_date,1,4) as year, count(1) as cnt from dim_posts group by 1 order by year"
    # sql = "select tag_name, cnt from dim_tags limit 10"

    rows, dur = exec_presto(value)
    # print("rows: ", rows)

    x_values = [ row[0] for row in rows ]
    y_values = [ row[1] for row in rows ]

    result_bar = {
        'data': [
            go.Bar(
                x=x_values,
                y=y_values,
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
    return (result_bar,)


# Comparing Presto and Druid for the same query
@app.callback(Output('presto-state', 'children'),
              [Input('submit-button', 'n_clicks')],
              [State('sql-state', 'value'),])
def get_presto_state(n_clicks, sql):
    # Remove semicolons if any
    if not sql: 
        return ''
    sql = sql.replace(';', '') 

    presto_result, dur = exec_presto(sql)

    return u'''
        The Button has been pressed {} times,
        SQL is "{}",
        Query finished in {} seconds,
        Result is: "{}".
    '''.format(n_clicks, sql, dur, presto_result)


@app.callback(Output('druid-state', 'children'),
              [Input('submit-button', 'n_clicks')],
              [State('sql-state', 'value'),])
def get_druid_state(n_clicks, sql):
    if not sql:
        return ''
    sql = sql.replace(';', '')

    druid_result, dur = exec_druid(sql)

    return u'''
        The Button has been pressed {} times,
        SQL is "{}",
        Query finished in {} seconds,
        Result is: "{}".
    '''.format(n_clicks, sql, dur, druid_result)


# Kafka Produce Live Data (Simulator)
# Randomly generate 5 - 50 messages to the topic
@app.callback(
    Output('hidden-div', 'children'),
    [Input('live_interval_component', 'n_intervals'),
    Input('live_control_button', 'value')])
def kafka_producer_active_user(n_intervals, value):
    print("kafka, ", value)
    if value == "stop":
        return
    
    producer = KafkaProducer(bootstrap_servers='172.31.7.229:9092')
    s3_url = 's3://stackoverflow-ds/PostHistory_rt.xml'

    stop = randint(5, 50)  # Random generate n events in 5 sec

    for i, line in enumerate(open(s3_url)):
        if i <= 1:  # Skip first two lines
            continue

        schema = ["Comment", 'Id', 'PostHistoryTypeId', 'PostId', 'RevisionGUID', 'Text', 'UserDisplayName', 'UserId']
        row = {}

        for col in schema:
            value = lxml.etree.fromstring(line).xpath('//row/@{}'.format(col))
            row[col] = value[0] if len(value) > 0 else ""

        now = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S.000')
        row['_CreationDate'] = now
        parsed = json.dumps(row)
        # print(parsed)

        # Send message to Kafka Topic
        producer.send('posthistory', parsed.encode('utf-8'))

        if i >= stop:
            return
    return
            

@app.callback(Output('live_text', 'children'),
              [Input('live_interval_component', 'n_intervals')])
def update_metrics(n):
    dau = kafka_load_dau(seconds=1800)

    now_minute = datetime.datetime.now().strftime('%Y-%m-%dT%H-%M')
    now_dau = dau[now_minute] if now_minute in dau else 0

    style = {'padding': '5px', 'fontSize': '50px', 'align': 'center'}

    return [ html.Span('{} Active Users'.format(now_dau), style=style)]


# Multiple components can update everytime interval gets fired.
@app.callback(
    Output('live_graph_line', 'figure'),
    [Input('live_interval_component', 'n_intervals')])
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
            'title': "Line Chart"
        }
    }

    return result_line



################################################################################
###
### FUNCTIONS
###
################################################################################
def exec_presto(sql):
    start = time.clock()
    if not sql or len(sql) == 0:
        return [], 0
    # Demo Only. Should come from a config file
    conn=prestodb.dbapi.connect(
        host='localhost',
        port=8889,
        user='hadoop',
        catalog='hive',
        schema='web',
    )
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    return result, time.clock() - start
    

def exec_druid(sql):
    start = time.clock()
    msg = json.dumps({'query': sql})
    # print("msg: ", msg)
    headers = {
        'content-type':'application/json'
    }
    
    # Demo Only. Should come from a config file
    druid_url = 'http://34.211.45.55:8082/druid/v2/sql/'
    r = requests.post(url=druid_url, data=msg, headers=headers)
    # print("r: ", r, r.text)
    return r.text, time.clock() - start
 
# Load live data from Kafka
def kafka_load_dau(seconds=1800):
    dau = {}

    consumer = KafkaConsumer(
        'posthistory', 
        bootstrap_servers=['172.31.7.229:9092'], 
        auto_offset_reset='earliest', 
        enable_auto_commit=True, 
        auto_commit_interval_ms=1000
    )

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


################################################################################
###
### RUN
###
################################################################################
if __name__ == '__main__':
    # Since it is demo, running on port 80 directly
    app.run_server(debug=True, host='0.0.0.0', port=80)
