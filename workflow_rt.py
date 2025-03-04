import json
import csv
import sys
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import numpy as np

def parse_json(json_file, workflow_metrics = None):
    # Load JSON data from the provided file
    with open(json_file, 'r') as file:
        data = json.load(file)

    # Identify the unique workflow and provisionFileOut iterations
    unique_workflow = None
    provision_file_out_max_time = 0

    for workflow in data:
        workflow_name = workflow.get('workflow_name')
        wallclock_seconds = workflow.get('wallclock_seconds')

        if workflow_name != 'provisionFileOut':
            unique_workflow = workflow
        else:
            if wallclock_seconds > provision_file_out_max_time:
                provision_file_out_max_time = wallclock_seconds

    if unique_workflow:
        row = {
                'workflow_name': unique_workflow.get('workflow_name'),
                'start_time': unique_workflow.get('start_time'),
                'end_time': unique_workflow.get('end_time'),
                'wallclock_seconds': unique_workflow.get('wallclock_seconds'),
                'workflow_run_id': unique_workflow.get('workflow_run_id'),
                'max_provisionFileOut_wallclock_seconds': provision_file_out_max_time
            }
    
        if workflow_metrics is None:
            columns = ['workflow_name', 'start_time', 'end_time', 'wallclock_seconds', 'workflow_run_id', 'max_provisionFileOut_wallclock_seconds']
            workflow_metrics = pd.DataFrame(columns=columns)
        
        workflow_metrics = pd.concat([workflow_metrics, pd.DataFrame([row])], ignore_index=True)

    return workflow_metrics

def gantt_plot(workflow_metrics, html_file='wrt_gantt.html', png_file='wrt_gantt.png'):
    custom_workflow_run = ['bamMergePreprocessing', 'mutect2', 'variantEffectPredictor', 'gridss', 'purple', 'delly', 'mavis', 'hrDetect', 'msisensor']
    reversed_arr_1d = np.flip(custom_workflow_run)

    # Convert start_time and end_time to datetime
    workflow_metrics['start_time'] = pd.to_datetime(workflow_metrics['start_time'])
    workflow_metrics['end_time'] = pd.to_datetime(workflow_metrics['end_time'])


    # Sort based on start times
    metrics_sorted = workflow_metrics.sort_values(by='start_time')
    reversed_arr_2d = np.flip(metrics_sorted.sort_values('start_time')['workflow_name'].tolist())

    # Define Dependencies 
    dependencies = { 
        'bamMergePreprocessing': ['mutect2', 'gridss', 'delly', 'msisensor'],
        'mutect2': ['variantEffectPredictor', 'purple', 'hrDetect'],
        'delly': ['mavis'],
        'gridss': ['purple', 'hrDetect'],
        'purple': ['hrDetect']
    }

    # Create the Gantt chart with Plotly Express
    fig = px.timeline(metrics_sorted, 
                  x_start='start_time', 
                  x_end='end_time', 
                  y='workflow_name', 
                  color='workflow_run_id',
                  title='Gantt Chart of Workflow Run Times'
    )

    # Prepare arrows based on dependencies (from one workflow to the next)
    arrows = []
    for workflow, dependent_workflows in dependencies.items():
        for dep in dependent_workflows:
            if workflow in metrics_sorted['workflow_name'].values and dep in metrics_sorted['workflow_name'].values:
                # Get the end time of the current workflow (workflow)
                workflow_end_time = metrics_sorted[metrics_sorted['workflow_name'] == workflow]['end_time'].iloc[0]
                # Get the start time of the dependent workflow (dep)
                dep_start_time = metrics_sorted[metrics_sorted['workflow_name'] == dep]['start_time'].iloc[0]
            
                # Create an arrow from the end of the workflow to the start of the dependent workflow
                arrows.append(go.Scatter(
                    x=[workflow_end_time, dep_start_time],  # From end of one to start of another
                    y=[workflow, dep],  # Corresponding workflow names on the y-axis
                    mode='lines',
                    line=dict(color='black', width=1, dash='dot'),
                    showlegend=False
                ))

    # Add arrows to the figure
    fig.add_traces(arrows)

    # Update layout to ensure proper styling and axes
    fig.update_xaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor='lightgrey',
        title="Time"
    )

    fig.update_yaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor='lightgrey',
        autorange='reversed',
        title="Workflow Name",
        ticktext=metrics_sorted['workflow_name']
    )

    # Set up dropdown menu options based on whether all custom workflows are present
    updatemenus = [
        {
            'buttons': [
                {
                    'args': [{'yaxis': {'categoryorder': 'array', 
                                    'categoryarray': reversed_arr_2d}}],
                    'label': 'By Start Time',
                    'method': 'relayout'
                }
            ],
            'direction': 'down',
            'pad': {'r': 10, 't': 10},
            'showactive': True,
            'type': 'dropdown',
            'x': 0.17,
            'xanchor': 'left',
            'y': 1.15,
            'yanchor': 'top'
        }
    ]

    # Add second dropdown option only if all custom workflows exist in the dataset
    existing_workflows = metrics_sorted['workflow_name'].isin(custom_workflow_run).all()

    if existing_workflows:
        updatemenus[0]['buttons'].append(
            {
                'args': [{'yaxis': {'categoryorder': 'array', 
                                'categoryarray': reversed_arr_1d}}],
                'label': 'By Workflow Run',
                'method': 'relayout'
            }
        )

    fig.update_layout(
        plot_bgcolor='white',
        showlegend=False,
        updatemenus=updatemenus
    )   

    # Saving plot in HTML format
    fig.write_html(html_file)

    # Saving in PNG format
    fig.write_image(png_file)
    


def generate_csv(workflow_metrics,  csv_file='workflow_report.csv'):
    
    # Check if CSV file already exists
    file_exists = os.path.isfile(csv_file)
        
    if file_exists:
        workflow_metrics.to_csv(csv_file, mode='a', header=False, index=False)
    else:
        # If the file does not exist, write the DataFrame with the header
        workflow_metrics.to_csv(csv_file, mode='w', header=True, index=False)
    
    print(f"Workflow run metrics saved to {csv_file}")

        

if __name__ == "__main__":
    json_dir = 'Extracted_Metrics'
    json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]

    workflow_metrics = None

    for file in json_files:
        json_file = os.path.join(json_dir, file)
        workflow_metrics = parse_json(json_file, workflow_metrics)

    if workflow_metrics is not None:
        gantt_plot(workflow_metrics)
        generate_csv(workflow_metrics)




