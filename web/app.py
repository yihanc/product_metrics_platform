#!/usr/bin/python3
# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go

import requests, json, prestodb, time, datetime
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from smart_open import open
import lxml.etree
from random import randint
from jinja2 import Template
from textwrap import dedent
 



################################################################################
###
### Metrics Definitations
###
################################################################################

metrics_config = {
    "questions_posted": {
        "engine": "D",
        "sql_template": """
            SELECT
                {{ time_groupby }},
                count(1) as cnt
            FROM ( 
                SELECT *
                FROM dim_posts
                WHERE _PostTypeId = 1
                    AND {{ time_filter }}
            ) p
            {% if other_filter_value is defined and other_filter_value|trim|length > 0 %}
            JOIN (
                SELECT _Id, _Location
                FROM dim_users
                WHERE 
                    {{ other_filter }} {}
            ) u
                ON p._OwnerUserId = u._Id
            {% endif %}
            GROUP BY
                {{ time_groupby }}
            ORDER BY
                1
        """,
        "time_filter_enabled": True, 
        "time_groupby_enabled": True,
        "other_filter_enabled": True,
        "bar_graph_orientation": "v",
        "title":"Number of Questions Posts",
    },
    "answers_posted": {
        "engine": "D",
        "sql_template": '''
            SELECT
                {{ time_groupby }},
                count(1) as cnt
            FROM dim_posts p
            WHERE _PostTypeId = 2
                AND {{ time_filter }}
            GROUP BY
                {{ time_groupby }}
            ORDER BY
                1
        ''',
        "time_filter_enabled": True, 
        "time_groupby_enabled": True,
        "other_filter_enabled": False,
        "bar_graph_orientation": "v",
        "title":"Number of Answers Posts",
    },
    "top_tags": {
        "engine": "P",
        "sql_template": '''
            SELECT tag_name, cnt 
            FROM dim_tags 
            ORDER BY cnt desc 
            LIMIT 20
        ''',
        "time_filter_enabled": False, 
        "time_groupby_enabled": False,
        "other_filter_enabled": False,
        "bar_graph_orientation": "h",
        "title":"Top Tags in posts",
    },
    "total_posts": {
        "engine": "D",
        "sql_template": '''
            SELECT
              {{ time_groupby }},
              SUM(CASE WHEN _PostTypeId = 1 THEN 1 ELSE 0 END) AS questions,
              SUM(CASE WHEN _PostTypeId = 2 THEN 1 ELSE 0 END) AS answers,
              SUM(CASE WHEN _PostTypeId NOT IN (1,2) THEN 1 ELSE 0 END) AS other_posts
            FROM dim_posts
            WHERE
               {{ time_filter }}
            GROUP BY 
                {{ time_groupby }}             
            ORDER BY
              1
        ''',
        "time_filter_enabled": True, 
        "time_groupby_enabled": True,
        "other_filter_enabled": False,
        "bar_graph_orientation": "v",
        "title":"Total Posts Created",
    },
}



################################################################################
###
### Nav Bar
###
################################################################################

PLOTLY_LOGO = "https://images.plot.ly/logo/new-branding/plotly-logomark.png"

nav_item = dbc.NavItem(dbc.NavLink("", href="#"))
navbar = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                # Use row and col to control vertical alignment of logo / brand
                dbc.Row(
                    [
                        dbc.Col(html.Img(src=PLOTLY_LOGO, height="50px")),
                        dbc.Col(dbc.NavbarBrand("Metrics as a Service", className="ml-2")),
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
app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    navbar,

    dcc.Tabs(id="tabs", children=[
        dcc.Tab(label='Metrics Discovery', children=[
            html.Div(children=[
                html.Div( [     # Row 1
                    html.Div( [     # Row 1 Col 1
                        # Select a metric
                        html.H3(
                            children='Select a predefined metric: ',
                            style={'margin-top': '20px', 'margin-bottom': '15px'},
                        ),
                        dcc.Dropdown(
                            id='metric_dropdown',
                            options=[
                                {
                                    'label': 'Top Tags',
                                    'value': "top_tags",
                                },
                                {
                                    'label': 'Total Posts Created (stacked)',
                                    'value': "total_posts",
                                },
                                {
                                    'label': 'Answers Posted',
                                    'value': "answers_posted",
                                },
                                {
                                    'label': 'Questions Posted',
                                    'value': "questions_posted",
                                },
                            ],
                            placeholder="Select a metric",
                            value='',
                            style={'margin-top': '15px', 'margin-bottom': '10px', 'width':'300px'},
                        ),

                        # To be hidden if needed
                        html.Div(
                            [
                            # Time Filters
                            html.H5(
                                id='time_filter_text',
                                children='Select a date period',
                                style={'margin-top': '15px', 'margin-bottom': '10px'},
                            ),
                            dcc.DatePickerRange(
                                id='time_filter',
                                min_date_allowed=datetime.datetime(2009, 1, 1),
                                max_date_allowed=datetime.datetime(2019, 12, 31),
                                start_date=datetime.datetime(2009, 1, 1),
                                end_date=datetime.datetime(2020, 12, 31),
                                style={'margin-top': '15px', 'margin-bottom': '10px'},
                            )],
                            id='time_filter_div',
                            style={'display': 'block'}
                        ),

                        html.Div(
                            [
                            # Time Group By
                            html.H5(
                                id="time_groupby_text",
                                children='Select Group By',
                                style={'margin-top': '15px', 'margin-bottom': '15px'},
                            ),
                            dcc.Dropdown(
                                id="time_groupby",
                                options=[
                                    {'label': 'Year', 'value': "DATE_TRUNC('year', __time)"},
                                    {'label': 'Quarter', 'value': "DATE_TRUNC('quarter', __time)"},
                                    {'label': 'Month', 'value': "DATE_TRUNC('month', __time)"},
                                    {'label': 'Week', 'value': "DATE_TRUNC('week', __time)"},
                                    {'label': 'Day', 'value': "DATE_TRUNC('day', __time)"},
                                    # {'label': 'Hour', 'value': "DATE_TRUNC('hour', __time)"},
                                ],
                                placeholder='Group By Time Range',
                                value="DATE_TRUNC('year', __time)",
                                style={'margin-top': '15px', 'margin-bottom': '15px', 'width':'300px'},
                            ), ], 
                            id='time_groupby_div',
                            style={'display': 'block'},
                        ),

                        # Additional Filter
                        html.Div(
                            [
                                # Additional Filter
                                html.H5(
                                    id='other_filter_text',
                                    children='Select Filters From Other Table (Query may take ~1min)',
                                    style={'margin-top': '15px', 'margin-bottom': '10px'},
                                ),
                                dcc.Dropdown(
                                    id='other_filter',
                                    options=[
                                        {'label': 'dim_users._Location LIKE', 'value': "LOWER(_Location) like "},
                                    ],
                                    style={'margin-top': '15px', 'margin-bottom': '15px', 'width':'300px'},
                                ),
                                dcc.Dropdown(
                                    id="other_filter_value",
                                    options=[
                                        {'label': 'CA', 'value': """'%ca%'"""},
                                        {'label': 'United States', 'value': """'%united states%'"""},
                                    ],
                                    placeholder='Select predefined value',
                                    style={'margin-top': '15px', 'margin-bottom': '15px', 'width':'300px'},
                                ),
                            ],
                            id='other_filter_div',
                            style={'display': 'block'},
                        ),
                    ], className="col"),

                    html.Div( [     # Row 1 Col 2
                        # Query Output 
                        html.H3(
                            children="Result:  ",
                            style={'margin-top': '15px', 'margin-bottom': '15px', 'width':'300px'},
                        ),
                        dcc.Markdown(
                            id="query_result",
                        ),
                    ], className='col'),
                ], className='row', style={'height': '550px'} ),

                # BAR CHART
                dcc.Loading(id='query_result_loading'), 
                dcc.Graph(
                    id='graph_bar',
                    config={
                        'showSendToCloud': True,
                        'plotlyServerURL': 'https://plot.ly'
                    },
                    style={'margin-top': '15px', 'margin-bottom': '15px', 'height': '500px'},
                ),
            ], style={'display':'block', 'width':'80%', 'margin':'0 auto', 'margin-bottom': '500px'}),
        ]),

        dcc.Tab(label='Live Data', children=[
            html.Div(children=[
                html.H4('Control Button:'),
                dcc.RadioItems(
                    id="live_control_button",
                    options=[
                        {'label': 'Start Live Data', 'value': 'start'},
                        {'label': 'Stop Live Data', 'value': 'stop'},
                    ],
                    value='start',
                    labelStyle={'display': 'inline-block'},
                    style={'margin': '20px', 'vertical-align': 'middle'},
                ),
                html.H4('Live Active Users per minute:'),
                html.Div(
                    id='live_text',
                    style={'margin': '20px'},
                ),
                # LINE CHART
                dcc.Graph(
                    id='live_graph_line',
                    config={
                        'showSendToCloud': True,
                        'plotlyServerURL': 'https://plot.ly'
                    },
                    style={'margin': '50px'},
                ),
                # BAR CHART
                dcc.Graph(
                    id='live_graph_bar',
                    config={
                        'showSendToCloud': True,
                        'plotlyServerURL': 'https://plot.ly'
                    },
                    style={'margin-top': '50px', 'margin-top': '50px'},
                ),
                dcc.Interval(
                    id='live_interval_component',
                    interval=5*1000, # in milliseconds
                    n_intervals=0,
                ),
                html.Div(id='hidden-div', style={'display':'none'})
            ], style={'display':'block', 'width':'80%', 'margin':'0 auto'})
        ]),

        dcc.Tab(
            label='Adhoc Query - (Compare Druid and Presto)', children=[
            html.Div(children=[
                html.H3(children='Compare Presto and Druid Speed:'),
                html.Div(children='''
                    Try type "select count(1) from dim_posts" and see the differences..
                '''),
                dcc.Textarea(
                    id="sql-state",
                    placeholder='Enter a SQL query... eg: select * from dim_posts limit 10',
                    style={'width': '60%'},
                ),
                dbc.Button(id='submit-button', n_clicks=0, children='Submit'),
                html.H3(children='Presto Result:'),
                html.Div(
                    [
                        dcc.Loading(id='presto-state') 
                    ],
                ),
                html.P(),
                html.H3(children='Druid Result: '),
                html.Div(
                    [
                        dcc.Loading(id='druid-state')
                    ],
                ),
                html.P(),
            ], style={'display':'block', 'width':'80%', 'margin':'0 auto', 'min-height':'1500px'}),
        ]),
    ]),
])


################################################################################
###
### CALLBACKS
###
################################################################################


@app.callback(
    [ 
        Output(component_id='time_filter_div', component_property='style'), 
        Output(component_id='time_groupby_div', component_property='style'), 
        Output(component_id='other_filter_div', component_property='style'), 
    ], 
    [
        Input('metric_dropdown', 'value')
    ])
def hide_menus(name):
    if not name or name not in metrics_config:
        return (None, None, None)
    metric = metrics_config[name]
    time_filter_enabled = metric["time_filter_enabled"]
    time_groupby_enabled = metric["time_groupby_enabled"]
    other_filter_enabled = metric["other_filter_enabled"]

    res_time_filter = {'display': 'block'} if time_filter_enabled else {'display': 'none'}
    res_time_groupby = {'display': 'block'} if time_groupby_enabled else {'display': 'none'}
    res_other_filter = {'display': 'block'} if other_filter_enabled else {'display': 'none'}

    return (res_time_filter, res_time_groupby, res_other_filter)


@app.callback(
    [
        Output('query_result', 'children'),
    ],
    [
        Input('metric_dropdown', 'value'),
        Input('time_filter', 'start_date'),
        Input('time_filter', 'end_date'),
        Input('other_filter', 'value'),
        Input('other_filter_value', 'value'),
        Input('time_groupby', 'value'),
    ])
def update_bar_output(name, start_date, end_date, other_filter, other_filter_value, time_groupby):
    ### Rendoring SQL...
    if name not in metrics_config:
        return ("", )
    metric = metrics_config[name]
    engine = metric["engine"]
    sql = metric["sql_template"]
    bar_graph_orientation = metric["bar_graph_orientation"]
    title = metric["title"]

    if other_filter_value is None or other_filter_value.strip() == "":
        other_filter_value = ""
        other_filter = ""

    time_filter = "__time BETWEEN '{}' AND '{}'".format(start_date, end_date)


    # Determine if JOIN IS needed
    if other_filter_value is not None and len(other_filter_value.split()) > 0:
        engine = "p"
        time_groupby = time_groupby.replace("__time", "FROM_ISO8601_TIMESTAMP(__time)")

    rendered_sql = Template(sql).render(
        time_groupby=time_groupby,
        time_filter=time_filter,
        other_filter=other_filter,
        other_filter_value=other_filter_value,
    ).format(
        other_filter_value,
    )

    print("rendered sql: ", rendered_sql)
    engine_name = "Presto" if engine == "p" else "Druid"

    markdown = gen_markdown(engine_name, rendered_sql)

    print("markdown : ", markdown)
    
    return (markdown, )


# Metrics Tab - Update Graph
@app.callback(
    [
        Output('graph_bar', 'figure'),
        Output('query_result_loading', 'children'),
#        Output('query_time', 'children'),
    ],
    [
        Input('metric_dropdown', 'value'),
        Input('time_filter', 'start_date'),
        Input('time_filter', 'end_date'),
        Input('other_filter', 'value'),
        Input('other_filter_value', 'value'),
        Input('time_groupby', 'value'),
    ])
def update_bar_output(name, start_date, end_date, other_filter, other_filter_value, time_groupby):
    ### Rendoring SQL...
    if name not in metrics_config:
        return ({}, "")
    metric = metrics_config[name]
    engine = metric["engine"]
    sql = metric["sql_template"]
    bar_graph_orientation = metric["bar_graph_orientation"]
    title = metric["title"]

    if other_filter_value is None or other_filter_value.strip() == "":
        other_filter_value = ""
        other_filter = ""

    time_filter = "__time BETWEEN '{}' AND '{}'".format(start_date, end_date)


    # Determine if JOIN IS needed
    if other_filter_value is not None and len(other_filter_value.split()) > 0:
        engine = "p"
        time_groupby = time_groupby.replace("__time", "FROM_ISO8601_TIMESTAMP(__time)")

    rendered_sql = Template(sql).render(
        time_groupby=time_groupby,
        time_filter=time_filter,
        other_filter=other_filter,
        other_filter_value=other_filter_value,
    ).format(
        other_filter_value,
    )


    ### Rendor SQL Finished

    if name == "total_posts":  # Special Graph 
        return gen_total_posts_graph(engine, rendered_sql, title)
    
    if engine.lower() == "p":
        engine_name = "Presto"
        rows, dur = exec_presto(rendered_sql)
        x_values = [ row[0] for row in rows[:1000] ]    # Limit output to 1000 elements only
        y_values = [ row[1] for row in rows[:1000] ]

    elif engine.lower() == "d":
        engine_name = "Druid"
        rows, dur = exec_druid(rendered_sql)
        rows = json.loads(rows)
        x_values = [ row["EXPR$0"] for row in rows[:1000] ]
        y_values = [ row["cnt"] for row in rows[:1000] ]


    time_result = gen_query_time_result(dur)

    # Return result based on if it is Vertical Bar or Horizontal Bar

    if bar_graph_orientation == "v":
        result_bar = {
            'data': [
                go.Bar(
                    x=x_values,
                    y=y_values,
                    marker=go.bar.Marker(
                        color='rgb(55, 83, 109)'
                    ),
                    orientation=bar_graph_orientation
                ),
            ],
            'layout': go.Layout(
                title=title,
                showlegend=True,
                legend=go.layout.Legend(
                    x=0,
                    y=1.0
                ),
                margin=go.layout.Margin(l=40, r=0, t=40, b=30)
            ),
        }
    else:
        result_bar = {
            'data': [
                go.Bar(
                    x=y_values,
                    y=x_values,
                    marker=go.bar.Marker(
                        color='rgb(55, 83, 109)'
                    ),
                    orientation=bar_graph_orientation
                ),
            ],
            'layout': go.Layout(
                title=title,
                showlegend=True,
                legend=go.layout.Legend(
                    x=0,
                    y=1.0
                ),
                margin=go.layout.Margin(l=40, r=0, t=40, b=30)
            ),
        }
    
    return (result_bar, time_result)


# Adhoc Tab - Presto callback
@app.callback([
    Output('presto-state', 'children'),
    ],
    [Input('submit-button', 'n_clicks')],
    [State('sql-state', 'value'),])
def get_presto_state(n_clicks, sql):
    # Remove semicolons if any
    if not sql: 
        return ('', )
    sql = sql.replace(';', '') 

    presto_result, dur = exec_presto(sql)

    return (u'''
        SQL is "{}",
        Query finished in {} seconds,
        Result is: "{}".
    '''.format(sql, dur, presto_result), )


# Adhoc Tab - Druid callback
@app.callback(Output('druid-state', 'children'),
              [Input('submit-button', 'n_clicks')],
              [State('sql-state', 'value'),])
def get_druid_state(n_clicks, sql):
    if not sql:
        return ''
    sql = sql.replace(';', '')

    if "join" in sql.lower().split():   # Disable Druid 
        return u'''Druid doesn't support join. Ignoring the query.'''

    druid_result, dur = exec_druid(sql)

    return u'''
        SQL is "{}",
        Query finished in {} seconds,
        Result is: "{}".
    '''.format(sql, dur, druid_result)


# Kafka Produce Live Data (Simulator)
# Randomly generate 5 - 50 messages to the topic
@app.callback(
    Output('hidden-div', 'children'),
    [Input('live_interval_component', 'n_intervals'),
    Input('live_control_button', 'value')])
def kafka_producer_active_user(n_intervals, value):
    if value == "stop":
        return
    
    producer = KafkaProducer(bootstrap_servers='54.189.125.21:9092')
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

        # Send message to Kafka Topic
        producer.send('posthistory', parsed.encode('utf-8'))

        if i >= stop:
            return
    return
            

# Live Text 
@app.callback(Output('live_text', 'children'),
              [Input('live_interval_component', 'n_intervals')])
def update_metrics(n):
    dau = kafka_load_dau(seconds=1800)

    now_minute = datetime.datetime.now().strftime('%Y-%m-%dT%H-%M')
    now_dau = dau[now_minute] if now_minute in dau else 0

    style = {'padding': '5px', 'fontSize': '50px', 'align': 'center'}

    return [ html.Span('{} Active Users'.format(now_dau), style=style)]


# Live Chart
@app.callback(
    [Output('live_graph_line', 'figure'),
    Output('live_graph_bar', 'figure')],
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



################################################################################
###
### FUNCTIONS
###
################################################################################
def exec_presto(sql):
    start = time.time()
    if not sql or len(sql) == 0:
        return [], 0
    # Demo Only. Should come from a config file
    conn=prestodb.dbapi.connect(
        host='54.69.164.133',
        port=8889,
        user='hadoop',
        catalog='hive',
        schema='web',
    )
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    return result, time.time() - start
    

def exec_druid(sql):
    start = time.time()
    msg = json.dumps({'query': sql})
    headers = {
        'content-type':'application/json'
    }
    
    # Demo Only. Should come from a config file
    druid_url = 'http://34.211.45.55:8082/druid/v2/sql/'
    r = requests.post(url=druid_url, data=msg, headers=headers)
    return r.text, time.time() - start
 
# Load live data from Kafka
def kafka_load_dau(seconds=1800):
    dau = {}

    consumer = KafkaConsumer(
        'posthistory', 
        bootstrap_servers=['54.189.125.21:9092'], 
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


# Generate Customized Graph for All Posts Metrics
def gen_total_posts_graph(engine, rendered_sql, title):
    engine_name = "Druid"
    rows, dur = exec_druid(rendered_sql)
    rows = json.loads(rows)
        
    x = [ row["EXPR$0"] for row in rows[:1000] ]
    y1 = [ row["questions"] for row in rows[:1000] ]
    y2 = [ row["answers"] for row in rows[:1000] ]
    y3 = [ row["other_posts"] for row in rows[:1000] ]

    trace1 = go.Bar(
        x=x,
        y=y1,
        name='questions'
    )
    trace2 = go.Bar(
        x=x,
        y=y2,
        name='answers',
    )
    trace3 = go.Bar(
        x=x,
        y=y3,
        name='other_posts',
    )
    figure = go.Figure(
        data=[trace1, trace2, trace3],
        layout=go.Layout(
            title=title,
            showlegend=True,
            legend=go.layout.Legend(
                x=0,
                y=1.0
            ),
            margin=go.layout.Margin(l=40, r=0, t=40, b=30),
            barmode='stack',
        ),
    )
    time_result = gen_query_time_result(dur)

    return figure, time_result


# Generate Markdown Result Text in Metric Discovery Tab
def gen_markdown(engine_name, rendered_sql):
    return dedent('''
        ###### Query executed using **{}** engine.
        ###### SQL
          ``` {} ```
    '''.format(engine_name, rendered_sql))

    
# Generate Markdown Result Text in Metric Discovery Tab
def gen_query_time_result(dur):
    return '''
        Finished in {} seconds.
    '''.format(dur)


################################################################################
###
### RUN
###
################################################################################
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
