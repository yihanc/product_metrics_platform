# -*- coding: utf-8 -*-
import dash
import dash_table

import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
import pandas as pd

import requests, json
import subprocess
import prestodb

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/solar.csv')

app.layout = dash_table.DataTable(
    id='table',
    columns=[{"name": i, "id": i} for i in df.columns],
    data=df.to_dict('records'),
)


app.layout = html.Div([
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),


    dcc.Textarea(
        id="sql-state",
        placeholder='Enter a value...',
        value='select * from sample limit 10',
        style={'width': '100%'}
    ),
    html.Button(id='submit-button', n_clicks=0, children='Submit'),
    html.Div(id='output-state'),

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
        value='Top 10 Tags',
    ),
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
])


# Graph Call Back
# One Query is getting two bar
@app.callback(
    [dash.dependencies.Output('graph_line', 'figure'),
    dash.dependencies.Output('graph_bar', 'figure')],
    [dash.dependencies.Input('my-dropdown', 'value')])
def update_bar_output(value):
    # sql = "select SUBSTR(creation_date,1,4) as year, count(1) as cnt from dim_posts group by 1 order by year"
    # sql = "select tag_name, cnt from dim_tags limit 10"
    rows = exec_presto(value)

    x_values = [ row[0] for row in rows ]
    y_values = [ row[1] for row in rows ]
    result_line = {
        'data': [{
            'type': 'scatter',
            'x': x_values,
            'y': y_values,
        }],
        'layout': {
            'title': value
        }
    }

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
    return result_line, result_bar


# Text Input Callback
@app.callback(Output('output-state', 'children'),
              [Input('submit-button', 'n_clicks')],
              [State('sql-state', 'value'),])
def update_output(n_clicks, input1):
    # Remove semicolons if any
    input1 = input1.replace(';', '') 

    rows = exec_presto(input1)
    print(rows)

    return u'''
        The Button has been pressed {} times,
        Input 1 is "{}",
        and Input 2 is ""
        and output is "{}"
    '''.format(n_clicks, input1, rows)


def exec_presto(sql):
    import prestodb
    conn=prestodb.dbapi.connect(
        host='localhost',
        port=8889,
        user='hadoop',
        catalog='hive',
        schema='web',
    )
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()
    
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')
