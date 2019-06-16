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
#     html.Button('Submit', id='button'),
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),


    dcc.Textarea(
        id="sql-state",
        placeholder='Enter a value...',
        value='This is a TextArea component',
        style={'width': '100%'}
    ),

    html.Button(id='submit-button', n_clicks=0, children='Submit'),
    html.Div(id='output-state'),

])



# @app.callback(
#     Output(component_id='my-div', component_property='children'),
#     [Input(component_id='my-id', component_property='value')]
# )
# def update_output_div(input_value):
# 
#     if len(input_value) < 170:
#         return 'You\'ve entered "{}"'.format(input_value)
#     else:
#         payload = { "query": input_value }
#         url = 'http://10.0.0.6:8082/druid/v2/sql'
#         headers = {'content-type': 'application/json'}
#         r = requests.post(url, data=json.dumps(payload), headers=headers)
    # return 'You\'ve entered "{}". ----------------- {}. '.format(input_value, json.dumps(payload))
#     return u'You\'ve entered "{}". ------------- Headers are {}. --------- Data is {}'.format(input_value, r.headers, r.text)



@app.callback(Output('output-state', 'children'),
              [Input('submit-button', 'n_clicks')],
              [State('sql-state', 'value'),])
#              [State('input-1-state', 'value'),])
#                State('input-2-state', 'value')])
def update_output(n_clicks, input1):

    import prestodb
    conn=prestodb.dbapi.connect(
        host='localhost',
        port=8889,
        user='hadoop',
        catalog='hive',
        schema='web',
    )
    cur = conn.cursor()
    cur.execute(input1)
    rows = cur.fetchall()
   # cmd = "/usr/bin/presto-cli"

    #cp = subprocess.run([cmd, "--catalog", "hive", "--schema", "web", "--execute", input1], stdout=subprocess.PIPE)

    print(rows)
    #print(cp.stdout)
    

    return u'''
        The Button has been pressed {} times,
        Input 1 is "{}",
        and Input 2 is ""
        and output is "{}"
    '''.format(n_clicks, input1, rows)




if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')
