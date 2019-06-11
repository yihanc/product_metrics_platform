import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import requests, json

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    dcc.Input(id='my-id', value='initial value', type='text'),
    html.Div(id='my-div')
])



@app.callback(
    Output(component_id='my-div', component_property='children'),
    [Input(component_id='my-id', component_property='value')]
)
def update_output_div(input_value):

    if len(input_value) < 170:
        return 'You\'ve entered "{}"'.format(input_value)
    else:
        payload = { "query": input_value }
        url = 'http://10.0.0.6:8082/druid/v2/sql'
        headers = {'content-type': 'application/json'}
        r = requests.post(url, data=json.dumps(payload), headers=headers)
    # return 'You\'ve entered "{}". ----------------- {}. '.format(input_value, json.dumps(payload))
    return u'You\'ve entered "{}". ------------- Headers are {}. --------- Data is {}'.format(input_value, r.headers, r.text)


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')
