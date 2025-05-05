import os
import subprocess
import json
import re
import csv
import gzip
import sys
import argparse
import pandas as pd
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go
from datetime import datetime

def extractWfId(data):
    '''
    Extracts workflow ids from a dictionary or list and returns them as a list.
    Parameters
    ----------
    data [dict] or [list]: The dataset in the form of a dictionary or a list.
    Returns
    -------
    List[str]: A list of workflow ids.
    '''
    workflow_ids = []
    # Check if data is a dictionary or a list
    if isinstance(data, dict):
        for key, value in data.items():
            if key == 'workflow_id':
                workflow_ids.append(value)
            else:
                workflow_ids.extend(extractWfId(value))

    elif isinstance(data, list):
        for item in data:
            workflow_ids.extend(extractWfId(item))

    return list(set(workflow_ids))



def queryFpr(fp_path, workflow_ids):
    '''
    Queries workflow ids against the File Provenance Report to extract sample names.
    Parameters
    ----------
    workflow_ids List[str]: A list of workflow ids to search against the FP report.
    Returns
    -------
    pd.DataFrame: A pandas dataframe containing extracted sample names and their corresponding workflow ids.
    '''
    print("Extracting records from FPR: " + fp_path)
    sname = []
    try:
        with gzip.open(fp_path, 'rt') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                if row['Workflow Run SWID'] in workflow_ids:
                    sname.append({'sample_name': row['Root Sample Name'], 'workflow_run_id': row['Workflow Run SWID']})
        
        if not sname:
            return pd.DataFrame(columns=['sample_name', 'workflow_run_id'])
        
        df = pd.DataFrame(sname).drop_duplicates()
        return df
    
    except Exception as e:
        print(f"Unexpected error while querying FPR: {e}")
        return None


def queryMongoDB(workflow_ids):
    '''
    Queries a list of workflow ids against a MongoDB Database to return the results as a list of dictionaries.
    Parameters
    ----------
    workflow_ids List[str]: A list of workflow ids to search against the database.
    Returns
    -------
    Optional List[Dict]: A list of dictionaries containing the query results, or None if an error occurred. 
    '''
    out = []
    query_str = '{"workflow_run_id": {"$in": ' + json.dumps(workflow_ids) + '}}'
    command = [
        "mongoexport",
        "--host", "workflow-metrics-db.gsi.oicr.on.ca",
        "--port", "27017",
        "--username", "workflow_metrics_ro",
        "--config", "/.mounts/labs/gsi/secrets/workflow-metrics-db.gsi_workflow_metrics_ro",
        "--db", "workflow_metrics",
        "--collection", "production_cromwell_workflow_metrics",
        "--jsonArray",
        "--query", query_str
    ]
    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
        out = json.loads(result.stdout)

    except subprocess.CalledProcessError as e:
        print(f"Error querying {workflow_id}: {e}")

    except Exception as e:
        print(f"Unexpected error while querying {workflow_id}: {e}")

    return out



def parseJson(data, workflow_metrics=None):
    '''
    Parses a list of dictionaries to extract workflow metrics and compute the maximum wallclock_seconds for any 'provisionFileOut' step associated with each workflow id.
    Parameters
    ----------
    data List[dict]: A list of dictionaries containing workflow metrics.
    workflow_metrics Optional[pd.DataFrame]: An empty dataframe to which new rows of extracted workflow metrics will be appended.
    Returns
    -------
    pd.DataFrame: A pandas dataframe containing extracted workflow metrics.
    '''
    if workflow_metrics is None:
        columns = ['workflow_name', 'start_time', 'end_time', 'wallclock_seconds', 'workflow_run_id', 'max_provisionFileOut_wallclock_seconds']
        workflow_metrics = pd.DataFrame(columns=columns)

    grouped_by_run_id = {}

    for workflow in data:
        workflow_run_id = workflow.get('workflow_run_id')
        if workflow_run_id not in grouped_by_run_id:
            grouped_by_run_id[workflow_run_id] = {
                'workflows': [],
                'max_provisionFileOut': 0
            }
        if workflow.get('workflow_name') == 'provisionFileOut':
            grouped_by_run_id[workflow_run_id]['max_provisionFileOut'] = max(
                grouped_by_run_id[workflow_run_id]['max_provisionFileOut'],
                workflow.get('wallclock_seconds', 0)
            )
        else:
            grouped_by_run_id[workflow_run_id]['workflows'].append(workflow)

    for run_id, group in grouped_by_run_id.items():
        for wf in group['workflows']:
            row = {
                'workflow_name': wf.get('workflow_name'),
                'start_time': wf.get('start_time'),
                'end_time': wf.get('end_time'),
                'wallclock_seconds': wf.get('wallclock_seconds'),
                'workflow_run_id': run_id,
                'max_provisionFileOut_wallclock_seconds': group['max_provisionFileOut']
            }
            workflow_metrics = pd.concat([workflow_metrics, pd.DataFrame([row])], ignore_index=True)

    return workflow_metrics



def loadConfig(config_file):
    '''
    Loads the workflow run order and dependencies from a JSON file.
    Parameters
    ----------
    config_file str: A path to the JSON workflow configuration file.
    Returns
    -------
    Tuple[List[str], Dict[str, List[str]]]: A list of workflows in the specified run order and a dictionary mapping workflow names to their dependencies.
    '''
    with open(config_file, 'r') as file:
        config = json.load(file)

    return config['workflow_run_order'], config['dependencies']



def addArrows(metrics_df, dependencies):
    '''
    Creates a list of lines (arrows) linking the different workflows and showing the dependency between them.
    Parameters
    ----------
    metrics_df pd.DataFrame: A pandas dataframe containing sorted workflow metrics.
    dependencies Dict[str, List[str]]: A dictionary where keys are workflow names and values are lists of workflows that depend on the key workflow.
    Returns
    -------
    List[go.Scatter]: A list of Plotly Scatter objects representing the arrows.
    '''
    arrows = []
    for workflow, dependent_workflows in dependencies.items():
        for dep in dependent_workflows:
            workflow_rows = metrics_df[metrics_df['workflow_name'] == workflow]
            dep_rows = metrics_df[metrics_df['workflow_name'] == dep]
            for _, workflow_row in workflow_rows.iterrows():
                for _, dep_row in dep_rows.iterrows():
                    arrows.append(go.Scatter(
                        x=[workflow_row['end_time'], dep_row['start_time']],
                        y=[workflow_row['workflow_name_id'], dep_row['workflow_name_id']],
                        mode='lines',
                        line=dict(color='black', width=1, dash='dot'),
                        showlegend=False
                    ))

    return arrows



def updateAxes(fig, metrics_df, run_order=None):
    '''
    Updates the axes of a given figure to display y axis values either based on run order or workflow start time.
    Parameters
    ----------
    fig go.Figure: The figure object to which the axes will be updated.
    metrics_df pd.DataFrame: A pandas dataframe containing sorted workflow metrics.
    run_order Optional[List[str]]: A list of workflow run orders.
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



def createPlot(df, fig_ht, fig_title, out_png, arrows=None, workflow_run_order=None):
    '''
    Creates a Gantt chart from the given metrics and saves it as a PNG file.
    Parameters
    ----------
    df pd.DataFrame: The sorted dataframe containing the workflow metrics.
    fig_ht int: The height of the plot.
    fig_title str: The title of the Gantt chart.
    out_png_file str: The path where the PNG file should be saved.
    Returns
    -------
    fig plotly.graph_objects.Figure: The figure object for the Gantt chart.
    ''' 
    fig = px.timeline(df, 
                      x_start='start_time', 
                      x_end='end_time', 
                      y='workflow_name_id', 
                      color='workflow_run_id',
                      color_discrete_sequence=px.colors.qualitative.Dark24 
                    )
    
    if arrows:
        fig.addArrows(arrows)

    updateAxes(fig, df, workflow_run_order)

    fig.update_layout(
        title=fig_title,
        title_x=0.5,
        plot_bgcolor='white',
        showlegend=False,
        height=fig_ht
    )

    return fig



def ganttPlot(workflow_metrics, config_file=None, png_file_1='gantt_v1.png', png_file_2='gantt_v2.png'):
    '''
    Generates two Gantt charts of workflow runtime and saves them as PNG files.
    Parameters
    ----------
    workflow_metrics pd.DataFrame: A pandas dataframe containing the workflow metrics.
    config_file Optional[str]: Path to the workflow configuration file containing run order and dependencies. [Optional]
    png_file_1 str: Path to the first PNG file where the Gantt Chart will be saved.
    png_file_2 str: Path to the second PNG file where the Gantt Chart will be saved.
    '''
    generate_second_chart = False

    # Convert start_time and end_time to datetime format  
    workflow_metrics['start_time'] = pd.to_datetime(workflow_metrics['start_time'], errors = 'coerce')
    workflow_metrics['end_time'] = pd.to_datetime(workflow_metrics['end_time'], errors = 'coerce')

    # Sort based on start times and create a new column that concatenates workflow names and their run IDs.
    metrics_sorted = workflow_metrics.sort_values(by='start_time')
    metrics_sorted['workflow_name_id'] = metrics_sorted['workflow_name'] + '-' + metrics_sorted['workflow_run_id']

    num_workflows = len(metrics_sorted['workflow_name_id'].unique())
    ht = max(400, num_workflows * 30) if num_workflows > 1 else 200
    unique_samples = workflow_metrics['sample_name'].nunique()
    arrows_1 = None
    arrows_2 = None
    workflow_run_order = None

    if config_file:
            #Generate the second plot and add dependency links only if workflow dependencies and run order are provided
            workflow_run_order, dependencies = load_config(config_file)
            generate_second_chart = True
            order_map = {workflow: idx for idx, workflow in enumerate(workflow_run_order)}
            metrics_sorted['run_order_y'] = metrics_sorted['workflow_name'].map(order_map)
            metrics_sorted_run_order = metrics_sorted.sort_values(by=['run_order_y'])
            #Add dependency links to first plot
            arrows_1 = addArrows(metrics_sorted, dependencies) 
            arrows_2 = addArrows(metrics_sorted_run_order, dependencies)

    if unique_samples == 1:
        sample_name = workflow_metrics['sample_name'].iloc[0]
        title_1 = f'Gantt Chart of Workflow Runtime (Sample: {sample_name})'
        fig_1 = createPlot(metrics_sorted, ht, title_1, png_file_1, arrows_1)
        png = f"{png_file_1.replace('.png', f'_{sample_name}.png')}"
        fig_1.write_image(png)
        print(f"Workflow run metrics saved to {png_file_1}")
        if generate_second_chart:
            title_2 = f'Gantt Chart of Workflow Runtime (Sample: {sample_name})'
            fig_2 = createPlot(metrics_sorted_run_order, ht, title_2, png_file_2, arrows_2, workflow_run_order)
            png = f"{png_file_2.replace('.png', f'_{sample_name}.png')}"
            fig_2.write_image(png)
            print(f"Workflow run metrics by run order saved to {png_file_2}")

    else:
        for sample in workflow_metrics['sample_name'].unique():
            sample_metrics = metrics_sorted[metrics_sorted['sample_name'] == sample]
            num_workflows = len(sample_metrics['workflow_name_id'].unique())
            ht = max(400, num_workflows * 30) if num_workflows > 1 else 200
            # Chart 1
            title_sample_1 = f'Gantt Chart of Workflow Runtime (Sample: {sample})'
            sample_png = f"{png_file_1.replace('.png', f'_{sample}.png')}"
            fig_1_sample = createPlot(sample_metrics, ht, title_sample_1, sample_png, arrows_1)
            fig_1_sample.write_image(sample_png)
            print(f"Workflow run metrics for sample {sample} saved to {sample_png}")

            if generate_second_chart:
                sample_metrics = metrics_sorted_run_order[metrics_sorted_run_order['sample_name'] == sample]
                num_workflows = len(sample_metrics['workflow_name_id'].unique())
                ht = max(400, num_workflows * 30) if num_workflows > 1 else 200
                # Chart 2
                title_sample_2 = f'Gantt Chart of Workflow Runtime (Sample: {sample})'
                sample_png = f"{png_file_2.replace('.png', f'_{sample}.png')}"
                fig_2_sample = createPlot(sample_metrics,ht, title_sample_2, sample_png, arrows_2, workflow_run_order)
                fig_2_sample.write_image(sample_png)
                print(f"Workflow run metrics by run order for sample {sample} saved to {sample_png}")



def generateCSV(workflow_metrics, csv_file='workflow_report.csv'):
    '''
    Saves workflow metrics data to a CSV file.
    Parameters
    ----------
    workflow_metrics pd.DataFrame: The pandas dataframe containing workflow metrics.
    csv_file str: The CSV filename/path where the metrics are to be stored. 
    '''
    workflow_metrics.to_csv(csv_file, mode='w', header=True, index=False)
    print(f"Workflow run metrics saved to {csv_file}")

        

def processInput(input_file, config_file=None):
    '''
    Processes an input JSON or Text file to retrieve workflow run ids.
    Parameters
    ----------
    input_file str: Path to an input JSON or Text file that needs to be processed to extract workflow ids.
    config_file Optional[str]: Path to the workflow configuration file containing run order and dependencies. [Optional]
    '''
    # Check if file exists
    if not os.path.isfile(input_file):
        print(f"File {input_file} not found.")
        return

    # Check if the input is a JSON or TXT file 
    if input_file.endswith('.json'):
        print("Processing JSON for workflow ids")
        try:
            with open(input_file, 'r') as file:
                input_data = json.load(file)
            workflow_ids = extractWfId(input_data)
        except Exception as e:
            print(f"Error loading JSON file: {e}")
            return

    elif input_file.endswith('.txt'):
        print("Processing TXT for workflow ids")
        with open(input_file, 'r') as file:
            lines = file.readlines()
        header_found = False
        if lines and re.match(r'^[A-Za-z0-9\-]+$', lines[0].strip()) is None: 
            header_found = True
            lines = lines[1:]
        workflow_ids = []
        for line in lines:
            stripped_line = line.strip()
            if re.match(r'^[A-Za-z0-9\-]+$', stripped_line): 
                workflow_ids.append(stripped_line)
        workflow_ids = list(set(workflow_ids))

    else:
        print(f"Error: The input must be either '.json' or '.txt' file")
        return

    if not workflow_ids:
        print("No workflow IDs found.")
        return
    
    fp_path = "/scratch2/groups/gsi/production/vidarr/vidarr_files_report_latest.tsv.gz"
    df_sname = queryFpr(fp_path, workflow_ids)
    
    print("Extracting workflow metrics for workflow_ids")
    data = queryMongoDB(workflow_ids)

    workflow_metrics = None
    if data is not None:
        workflow_metrics = parseJson(data, workflow_metrics)

    if workflow_metrics is not None and df_sname is not None:
        workflow_metrics = pd.merge(workflow_metrics, df_sname, left_on='workflow_run_id', right_on='workflow_run_id', how='left')
        ganttPlot(workflow_metrics, config_file)
        generateCSV(workflow_metrics)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = 'Generate gantt charts from workflow run metrics'
    )

    # Add the argument for the input JSON file and the workflow configuration file
    parser.add_argument(
        '-i', '--input',
        type = str,
        help = 'Path to the input JSON or TXT file containing workflow IDs',
        required = True
    )

    parser.add_argument(
        '--config',
        type = str,
        help = 'Path to the workflow configuration file containing run order and dependencies. [Optional]',
        required = False
    )

    # Add custom message to show usage
    parser.epilog = '''
    Example Usage: pipeline-rt -i /path/to/input/JSON 
    OR
    pipeline-rt -i /path/to/input/TXT
    '''

    # Parse arguments
    args = parser.parse_args()
    input_file = args.input

    if len(vars(args)) == 0:
        parser.print_help()
        exit(0)
    else:
        print("Reading input")
        processInput(input_file, config_file = args.config)
