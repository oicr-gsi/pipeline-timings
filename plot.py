import json
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys


def load_workflow_config(config_file='workflow_config.json'):
    '''
    Loads the workflow configuration from a JSON file. This includes the workflow run order and dependencies.
    
    Parameters
    -----------
    - config_file: path to the JSON configuration file.
    
    Returns
    -------
    - workflow_run_order: List of workflows in the run order.
    - dependencies: Dictionary mapping workflow names to their dependencies.
    '''
    with open(config_file, 'r') as file:
        config = json.load(file)

    return config['workflow_run_order'], config['dependencies']


def add_arrows(metrics_df, dependencies):
    '''
    Creates a list of lines (arrows) linking the different workflows and showing the dependency between them.
    
    Parameters
    -----------
    - A pandas dataframe containing sorted workflow metrics ordered either by their start time or by their run order. 
    - A dictionary where keys are workflow names and values are lists of workflows that depend on the key workflow. 
    '''
    arrows = []
    for workflow, dependent_workflows in dependencies.items():
        for dep in dependent_workflows:
            workflow_rows = metrics_df[metrics_df['workflow_name'] == workflow]
            dep_rows = metrics_df[metrics_df['workflow_name'] == dep]
        
            for _, workflow_row in workflow_rows.iterrows():
                for _, dep_row in dep_rows.iterrows():
                    workflow_end_time = workflow_row['end_time']
                    dep_start_time = dep_row['start_time']
                
                    arrows.append(go.Scatter(
                        x=[workflow_end_time, dep_start_time], 
                        y=[workflow_row['workflow_name_id'], dep_row['workflow_name_id']], 
                        mode='lines',
                        line=dict(color='black', width=1, dash='dot'),
                        showlegend=False
                    ))

    return arrows


def update_axes(fig, metrics_df, run_order=None):
    '''
    Updates the axes of a given figure to display y axis values either based on run order or workflow start time. 
    
    Parameters
    -----------
    - The figure object to which the axes will be updated. 
    - A pandas dataframe containing sorted workflow metrics ordered either by their start time or by their run order. 
    - A list of workflow run orders.
    '''
    if run_order:
        tickvals = metrics_df['workflow_name_id'].unique()
        ticktext = [workflow for workflow in run_order for _ in range(len(metrics_df[metrics_df['workflow_name'] == workflow]))]
    else:
        tickvals = [workflow for workflow in metrics_df['workflow_name_id'].unique()]
        ticktext = [workflow.split('-')[0] for workflow in metrics_df['workflow_name_id']]

    fig.update_yaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor='lightgrey',
        autorange='reversed', 
        title="Workflow Name",
        tickmode='array',
        tickvals=tickvals,
        ticktext=ticktext
    )
    fig.update_xaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor='lightgrey',
        title="Time"
    )


def gantt_plot(workflow_metrics, config_file=None, html_file_1='wrt_gantt_v1.html', html_file_2='wrt_gantt_v2.html'):
    '''
    Generates two interactive Gantt charts of workflow runtime. 

    Note: The function saves the Gantt charts as HTML files. The first chart (`html_file_1`) shows workflows sorted by their start times, 
    while the second chart (`html_file_2`) shows them sorted by their run order. 

    Parameters
    -----------
    - A pandas dataframe containing the workflow metrics. 
    - Two HTML files where the Gantt Charts will be saved. 
    '''
    if config_file:
        workflow_run_order, dependencies = load_workflow_config(config_file)
        generate_second_chart = True  # Flag to generate second chart
    else:
        generate_second_chart = False

    # Convert start_time and end_time to datetime format and create a new column that concatenates workflow names and their run IDs. 
    workflow_metrics['start_time'] = pd.to_datetime(workflow_metrics['start_time'])
    workflow_metrics['end_time'] = pd.to_datetime(workflow_metrics['end_time'])
   

    # Sort based on start times
    metrics_sorted = workflow_metrics.sort_values(by='start_time')
    metrics_sorted['workflow_name_id'] = metrics_sorted['workflow_name'] + '-' + metrics_sorted['workflow_run_id']


    fig_1 = px.timeline(metrics_sorted, 
                        x_start='start_time', 
                        x_end='end_time', 
                        y='workflow_name_id', 
                        color='workflow_run_id',
    )
    
    if config_file:
        arrows = add_arrows(metrics_sorted, dependencies)
        fig_1.add_traces(arrows)

    update_axes(fig_1, metrics_sorted)

    fig_1.update_layout(
        title='Interactive Gantt Chart of Workflow Runtime',
        title_x=0.5,
        plot_bgcolor='white',
        showlegend=False,
        annotations=[
                {
                    'text': "Sorted by run start time", 
                    'x': 0.45, 
                    'y': 1.02, 
                    'xref': 'paper',
                    'yref': 'paper',
                    'showarrow': False,
                    'font': {'size': 14, 'color': 'grey'},  
                    'align': 'center' 
                }
            ]
        )   
    fig_1.write_html(html_file_1)

    # Modify the 'y axis' values based on the run order and sort metrics based on this order
    if generate_second_chart:
        order_map = {workflow: idx for idx, workflow in enumerate(workflow_run_order)}
        metrics_sorted['run_order_y'] = metrics_sorted['workflow_name'].map(order_map)
        metrics_sorted_run_order = metrics_sorted.sort_values(by=['run_order_y'])

        fig_2 = px.timeline(metrics_sorted_run_order, 
                        x_start='start_time', 
                        x_end='end_time', 
                        y='workflow_name_id', 
                        color='workflow_run_id',
        )
    
        arrows = add_arrows(metrics_sorted_run_order, dependencies)
        fig_2.add_traces(arrows)

        update_axes(fig_2, metrics_sorted_run_order, workflow_run_order)

        fig_2.update_layout(
            title='Interactive Gantt Chart of Workflow Runtime',
            title_x=0.5,
            plot_bgcolor='white',
            showlegend=False,
            annotations=[
                {
                    'text': "Sorted by run order", 
                    'x': 0.45, 
                    'y': 1.04, 
                    'xref': 'paper',
                    'yref': 'paper',
                    'showarrow': False,
                    'font': {'size': 14, 'color': 'grey'},  
                    'align': 'center' 
                }
            ]
        )
        fig_2.write_html(html_file_2)

        

if __name__ == "__main__":
    # Check if the user passed the CSV file and optional JSON file as arguments
    if len(sys.argv) < 2:
        print("Usage: python3 plot.py <workflow_metrics.csv> [workflow_config.json]")
        exit(1)

    csv_file = sys.argv[1]
    config_file = sys.argv[2] if len(sys.argv) > 2 else None

    # Check if the CSV file exists
    if not os.path.isfile(csv_file):
        print(f"Error: The file '{csv_file}' does not exist.")
        exit(1)

    workflow_metrics = pd.read_csv(csv_file)

    if workflow_metrics is not None:
        gantt_plot(workflow_metrics, config_file=config_file)
