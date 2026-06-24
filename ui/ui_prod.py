from dash import Dash, html, dcc, callback, Output, Input, State
import plotly.express as px
import pandas as pd
import boto3
from datetime import datetime, timedelta
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import plotly.graph_objs as go
import json

load_figure_template("darkly")


app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
app.title = 'Log Realtime Analysis'
app._favicon = ('money.png')


def make_card(theme='success',header='Card',body='Body Empty'):
    return dbc.Card([
        dbc.CardHeader(str(header)),

        dbc.CardBody(
            [
                html.H5(str(body), className="card-title"),
            ]
        ),
    ], color=str(theme), outline=True)

def fetch_log_data():
    dynamodb = boto3.resource('dynamodb',
                              endpoint_url="http://localhost:8000",
                              region_name="us-west-2",  # Use a valid AWS region
                              aws_access_key_id="dummy",
                              aws_secret_access_key="dummy")

    table_name = 'agg_system_logs'
    table = dynamodb.Table(table_name)
    response = table.scan()
    items = response.get('Items', [])
    return pd.DataFrame(items)

def calculate_sla(df):
    total_logs = df['log_count'].sum()
    error_critical_logs = df[df['log_level'].isin(['ERROR', 'CRITICAL'])]['log_count'].sum()
    sla = 100 - (error_critical_logs / total_logs*100)
    return sla

def create_sla_gauge(sla_value):
    fig = go.Figure(go.Indicator(
        domain={'x': [0, 1], 'y': [0, 1]},
        value=sla_value,
        mode="gauge+number+delta",
        title={'text': "SLA Percentage"},
        delta={'reference': 90},  # Reference line for SLA threshold (you can change it)
        gauge={
            'axis': {'range': [None, 100], 'tickwidth': 2, 'tickcolor': 'white'},  # Axis appearance
            'steps': [
                {'range': [0, 50], 'color': "#8B0000"},  # Dark red for poor performance (Muted)
                {'range': [50, 80], 'color': "#B8860B"},  # Dark gold for medium performance (Muted)
                {'range': [80, 100], 'color': "#006400"}  # Dark green for good performance (Muted)
            ],
            'threshold': {
                'line': {'color': "#1E90FF", 'width': 4},  # A calm blue threshold line
                'thickness': 0.75,
                'value': 90
            }
        }
    ))

    # Update layout for dark theme consistency
    fig.update_layout(
        paper_bgcolor="#2E2E2E",  # Dark background color for the entire figure
        font={'color': 'white'},  # White text for better readability
    )

    return fig

app.layout = dbc.Container([
    dcc.Interval(
        id='interval-component',
        interval=10 * 1000,  # in milliseconds (10 seconds)
        n_intervals=0
    ),
    dbc.Row(
        [
            dbc.Col(
                html.Img(src="money.png", style={"width": "100%", "max-width": "50px", "height": "auto"}),
                width="auto"
            ),

            dbc.Col(
                html.H3("Log Realtime Analysis", className="text-left p-1"),
                width="auto",
            ),
        ],
        align="center",
        justify="start",
        className="mb-4"
    ),

    dbc.Row(
        [
            dbc.Col(make_card('info', 'INFO', '0'), id="info-card"),
            dbc.Col(make_card('warning', 'WARNING', '0'), id="warning-card"),
            dbc.Col(make_card('danger', 'ERROR', '0'), id="error-card"),
            dbc.Col(make_card('light', 'DEBUG', '0'), id="debug-card"),
            dbc.Col(make_card('success', 'SUCCESS', '0'), id="success-card"),
            dbc.Col(make_card('#ff3333', 'CRITICAL', '0'), id="critical-card"),
        ],
        className="mb-4",
    ),
    dbc.Row(
        [
            dbc.Col(dcc.Graph(id='sla-gauge', figure=create_sla_gauge(100))),
            dbc.Col(dcc.Graph(id="log_level_counts")),
            dbc.Col(dcc.Graph(id="log-level-line-graph"))],className="p-3"
    ),
    dbc.Row([
        dbc.Col([ html.H4("Top 5 APIs with Highest Error Counts"),html.Div(id='error-apis-table-container'), dbc.Button("Show Pipeline Architecture", id="open-modal-button", color="danger", className="mt-3"),]),
        dbc.Col(dbc.Col(dcc.Graph(id="avg_response_time_by_api")), ),

    ],className="p-3"),

    dbc.Row([
        # Modal
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle("Kafka-Spark Pipeline Architecture"), close_button=True),
                dbc.ModalBody(
                    html.Img(
                        src="assets/kafka_flow.gif",  # Replace with your image URL
                        style={"width": "100%"},
                    )
                ),
                dbc.ModalFooter(
                    dbc.Button("Close", id="close-modal-button", className="ms-auto", color="secondary")
                ),
            ],
            id="image-modal",
            size="lg",
            is_open=False,  # Initially closed
        ),
    ])
])


# Define callback to update graph
@app.callback(
    [Output("log-level-line-graph", "figure"),
           Output("log_level_counts", "figure"),
           Output("avg_response_time_by_api", "figure"),
           Output('error-apis-table-container', 'children'),
         Output('sla-gauge', 'figure'),
          Output('info-card', 'children'),
         Output('warning-card', 'children'),
         Output('error-card', 'children'),
         Output('debug-card', 'children'),
         Output('success-card', 'children'),
         Output('critical-card', 'children')],
        [Input("log-level-line-graph", "id"),Input('interval-component', 'n_intervals')]
)
def update_graph(id,n_intervals):
    df = fetch_log_data()
    df_agg = df.groupby(['window_start', 'window_end', 'log_level']).size().reset_index(name='log_count')
    print(df_agg['log_count'].head())


    df_agg['window_start'] = pd.to_datetime(df_agg['window_start'])

    # Aggregate data into shorter periods (e.g., 10 minutes) for better granularity
    df_agg['window_10min'] = df_agg['window_start'].dt.floor('5min')  # 10T means 10-minute intervals

    # Group by the new 10-minute window and log_level to sum the counts
    df_agg_10min = df_agg.groupby(['window_10min', 'log_level'])['log_count'].sum().reset_index()

    # Optional: Apply a rolling average for smoothing, adjust the window size if needed
    df_agg_10min['log_count_smooth'] = df_agg_10min.groupby('log_level')['log_count'].rolling(window=3, min_periods=1).mean().reset_index(level=0, drop=True)

    log_level_colors = {
        'INFO': '#3498db',  # Blue (info)
        'WARNING': '#f39c12',  # Yellow (warning)
        'ERROR': '#e74c3c',  # Red (danger)
        'DEBUG': '#adb5bd',  # Light Gray
        'SUCCESS': '#00bc8c',  # Green (success)
        'CRITICAL': '#ff3333'  # Dark (critical)
    }

    # Map colors to each log level in the DataFrame
    df['color'] = df['log_level'].map(log_level_colors)

    fig = px.line(df_agg_10min,
                  x='window_10min',
                  y='log_count_smooth',
                  color='log_level',
                  title='Log Counts Over Time by Log Level',
                  labels={'window_start': 'Time Window', 'log_count': 'Log Count'},
                  line_group='log_level',  # This ensures each log level has its own line
                  markers=True,
                  color_discrete_map=log_level_colors)

    # Adjust line width and axis range
    fig.update_layout(xaxis_title="Time", yaxis_title="Count")

    log_level_counts = df_agg.groupby('log_level')['log_count'].sum().reset_index()
    # Convert log level counts to a dictionary
    log_level_counts_dict = log_level_counts.set_index('log_level')['log_count'].to_dict()
    info_count = log_level_counts_dict.get('INFO', 0)
    warning_count = log_level_counts_dict.get('WARNING', 0)
    error_count = log_level_counts_dict.get('ERROR', 0)
    debug_count = log_level_counts_dict.get('DEBUG', 0)
    success_count = log_level_counts_dict.get('SUCCESS', 0)
    critical_count = log_level_counts_dict.get('CRITICAL', 0)

    log_level_counts = df_agg.groupby('log_level')['log_count'].sum().reset_index()

    fig_2 = px.pie(log_level_counts,
                 names="log_level",
                 values="log_count",
                 title="Log Level Distribution",hole=0.5,
                   color="log_level",  # Set color based on log_level
                   color_discrete_map=log_level_colors)

    avg_response_time_by_api = df.groupby('api')['avg_response_time'].mean().reset_index()
    fig_3 = px.bar(avg_response_time_by_api,
                 x='api',
                 y='avg_response_time',
                 title="Average Response Time by API Endpoint",
                 labels={"api": "API Endpoint", "avg_response_time": "Average Response Time (ms)"})

    df['avg_response_time'] = df['avg_response_time'].astype(float)
    error_logs = df[df['log_level'].isin(['ERROR','CRITICAL'])]
    error_counts = error_logs.groupby('api').agg(
        avg_response_time=('avg_response_time', 'mean'),
        error_count=('log_level', 'size'),
    ).reset_index()
    top_5_error_apis = error_counts.nlargest(5, 'error_count')
    top_5_error_apis['avg_response_time'] = top_5_error_apis['avg_response_time'].round(2)
    table = dbc.Table.from_dataframe(top_5_error_apis, striped=True, bordered=True, hover=True)


    updated_sla_value = calculate_sla(df)  # Update this based on real-time data

    # Update the gauge chart with the new SLA value
    updated_gauge_figure = create_sla_gauge(updated_sla_value)


    return (
        fig,
        fig_2,
        fig_3,
        table,
        updated_gauge_figure,
        make_card('info', 'INFO', f'{info_count}'),
        make_card('warning', 'WARNING', f'{warning_count}'),
        make_card('danger', 'ERROR', f'{error_count}'),
        make_card('light', 'DEBUG', f'{debug_count}'),
        make_card('success', 'SUCCESS', f'{success_count}'),
        make_card('#ff3333', 'CRITICAL', f'{critical_count}')
    )
@app.callback(
    Output("image-modal", "is_open"),
    [Input("open-modal-button", "n_clicks"), Input("close-modal-button", "n_clicks")],
    [State("image-modal", "is_open")],
)
def toggle_modal(open_clicks, close_clicks, is_open):
    if open_clicks or close_clicks:
        return not is_open
    return is_open
if __name__ == '__main__':
    app.run(debug=True)