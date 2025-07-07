import os
import subprocess
import json
import re
import csv
import gzip
import argparse
import pandas as pd
import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from collections import defaultdict
import logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    )

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
    logging.info(f"Extracting records from FPR: {fp_path}")
    sname = []
    seen = set()
    try:
        with gzip.open(fp_path, 'rt') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                workflow_run_id = row['Workflow Run SWID']
                if workflow_run_id in workflow_ids and workflow_run_id not in seen:
                    seen.add(workflow_run_id)

                    sample_name = row['Root Sample Name']
                    sample_attrs = row.get('Sample Attributes', '')

                    tissue = 'T'  # Default tissue
                    for attr in sample_attrs.split(';'):
                        if attr.strip().startswith('geo_tissue_type='):
                            value = attr.split('=')[1].strip()
                            if value == 'R':
                                tissue = 'N'
                            break  # Stop checking after first match

                    sname.append({
                        'sample_name': sample_name,
                        'workflow_run_id': workflow_run_id,
                        'tissue_type': tissue
                    })        
        
        if not sname:
            return pd.DataFrame(columns=['sample_name', 'workflow_run_id', 'tissue_type'])
        
        df = pd.DataFrame(sname)
        return df
    
    except Exception as e:
        logging.error(f"Unexpected error while querying FPR: {e}")
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
        logging.error(f"Error querying workflow IDs: {e}")

    except Exception as e:
        logging.error(f"Unexpected error while querying workflow IDs: {e}")

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

    rows = []
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
            rows.append(row)

    return pd.DataFrame(rows)

def loadConfig(config_file):
    '''
    Loads the workflow run order and workflow list from a JSON file.
    Parameters
    ----------
    config_file str: A path to the JSON workflow configuration file.
    Returns
    -------
    Tuple[List[str], List[str]]: A list of workflows in the specified run order and a list of workflows to include in the Gantt chart.
    '''
    with open(config_file, 'r') as file:
        config = json.load(file)

    run_order = config.get('workflow_run_order', [])
    wf_list = config.get('workflow_to_include', None)
    abbreviations = config.get('workflow_abbreviations', {})

    return run_order, wf_list, abbreviations

def updateAxes(fig, metrics_df, run_order=None, grid_color='lightgrey', show_grid=True):
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
        ticktext = metrics_df['workflow_name']
    
    else:
        tickvals = [workflow for workflow in metrics_df['workflow_name_id'].unique()]
        ticktext = [workflow.split('-')[0] for workflow in metrics_df['workflow_name_id']]
    
    fig.update_yaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor=grid_color,
        showgrid=show_grid,
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
        gridcolor=grid_color,
        showgrid=show_grid,
        title="Time"
    )

def compressGaps(df, threshold=24):
    '''
    Compresses gaps in the workflow metrics dataframe based on a specified threshold.
    Parameters
    ----------
    df pd.DataFrame: The dataframe containing workflow metrics with 'start_hr' and 'end_hr' columns.
    threshold float: The threshold for compressing gaps, default is 24 hours.
    Returns
    -------
    pd.DataFrame: A new dataframe with compressed timeline.
    '''
    df = df.copy().sort_values(by='start_hr')
    shift = 0
    last_time = df.iloc[0]['end_hr']
    
    compressed_s = []
    compressed_e = []

    for idx, row in df.iterrows():
        gap = row['start_hr'] - last_time

        if gap > threshold:
            shift += gap - threshold

        compressed_s.append(row['start_hr'] - shift)
        compressed_e.append(row['end_hr'] - shift)
        last_time = row['end_hr']
    
    df['start_hr'] = compressed_s
    df['end_hr'] = compressed_e

    return df

def createPlot(df, fig_ht, fig_title, run_order=None, abbreviations=None, bar_color='#4682B4', plot_bgcolor='white', show_abbr=True, grid_color='lightgrey', show_grid=True, paper_bgcolor='white', title_col='black', axtitle_col='black', axtick_col='black', title_size=16, axtitle_size=14, axtick_size=12):
    '''
    Creates a Gantt chart from the given metrics and saves it as a html file.
    Parameters
    ----------
    df pd.DataFrame: The sorted dataframe containing the workflow metrics.
    fig_ht int: The height of the plot.
    fig_title str: The title of the Gantt chart.
    out_html_file str: The path where the html file should be saved.
    Returns
    -------
    fig plotly.graph_objects.Figure: The figure object for the Gantt chart.
    ''' 
    fig = go.Figure()

    row = 2 if (abbreviations and show_abbr) else 1

    fig = make_subplots(
        rows=row, cols=1,
        shared_xaxes=False,
        vertical_spacing=0.15 if (abbreviations and show_abbr) else 0,
        row_heights=[0.70, 0.30] if (abbreviations and show_abbr) else [1.0],
        specs=[[{"type": "xy"}]] + ([[{"type": "table"}]] if (abbreviations and show_abbr) else [])
    )

    for _, row in df.iterrows():
        duration = row['end_hr'] - row['start_hr']
        hover_text = (
            f"Workflow Name: {row['workflow_name']}<br>"
            f"Workflow_ID: {row['workflow_run_id']}<br>"
            f"Start: {row['start_time'].strftime('%Y-%m-%d %H:%M')}<br>"
            f"End: {row['end_time'].strftime('%Y-%m-%d %H:%M')}<br>"
            f"Duration: {duration:.2f} hr"
        )

        fig.add_trace(go.Bar(
            x=[duration],
            y=[row['workflow_name_id']],
            base=[row['start_hr']],
            orientation='h',
            width=0.5,
            marker=dict(color=bar_color),
            hovertext=hover_text,
            hoverinfo='text'
        ), row=1, col=1)

    updateAxes(fig, df, run_order, grid_color, show_grid)

    if abbreviations and show_abbr:
        abbr_sort = sorted(abbreviations.items(), key=lambda x: x[1])
        table_trace = go.Table(
            header=dict(
                values=["<b>Abbreviation</b>", "<b>Workflow Name</b>"],
                fill_color='#717273',
                align='center',
                font=dict(color='white', size=12, family='Arial'),
            ),
            cells=dict(
                values=[[val for key, val in abbr_sort], [key for key, val in abbr_sort]],
                fill_color='#E4E4E5',
                align='center',
                font=dict(color='black', size=11, family='Arial'),
            )
        )
        fig.add_trace(table_trace, row=2, col=1)

    fig.update_layout(
        title=dict(
            text=fig_title,
            x=0.5,
            font=dict(
                color=title_col,
                size=title_size,
                family='Arial'
            )
        ),
        plot_bgcolor=plot_bgcolor,
        paper_bgcolor=paper_bgcolor,
        showlegend=False,
        height=fig_ht + 250 if (abbreviations and show_abbr) else fig_ht,
        xaxis=dict(
            title=dict(text='Hours since start', font=dict(color=axtitle_col,size=axtitle_size,family='Arial')),
            tickfont=dict(color=axtick_col, size=axtick_size, family='Arial'),
            type='linear',
            tickformat='.1f'
        ),
        yaxis=dict(
            title=dict(text='Workflow Name', font=dict(color=axtitle_col, size=axtitle_size, family='Arial')),
            tickfont=dict(color=axtick_col, size=axtick_size, family='Arial')
        ),
        hoverlabel=dict(
            bgcolor="white",  
            font_size=10,
            font_family="Arial",
            font_color="black",
        )
    )

    return fig

def ganttPlot(workflow_metrics, config_file=None, html_file='gantt_v1.html', compress_gaps=False, threshold=24, bar_color='#4682B4', plot_bgcolor='white', show_abbr=True, grid_color='lightgrey', show_grid=True, paper_bgcolor='white', title_col='black', axtitle_col='black', axtick_col='black', title_size=16, axtitle_size=14, axtick_size=12):
    '''
    Generates Gantt chart of workflow runtime and saves them as HTML files.
    Parameters
    ----------
    workflow_metrics pd.DataFrame: A pandas dataframe containing the workflow metrics.
    config_file Optional[str]: Path to the workflow configuration file containing run order and dependencies. [Optional]
    html_file str: Path to the HTML file where the Gantt Chart will be saved.
    '''
    run_order, wf_list, abbreviations = (None, None, {})
    if config_file:
        run_order, wf_list, abbreviations = loadConfig(config_file)

    if wf_list:
        workflow_metrics = workflow_metrics[
            workflow_metrics['workflow_name'].apply(
                lambda name: any(name.split('.')[0].startswith(wf) for wf in wf_list)
            )
        ].copy()
    
    def abbr_name(name):
        for prefix, abbr in abbreviations.items():
            if name.startswith(prefix):
                suffix = name[len(prefix):]
                return f"{abbr}{suffix}"
        return name

    # Apply abbreviations to workflow names
    workflow_metrics['workflow_fname'] = workflow_metrics['workflow_name']
    workflow_metrics['workflow_name'] = workflow_metrics['workflow_name'].apply(abbr_name)
    workflow_metrics['workflow_name_id'] = workflow_metrics['workflow_name'] + '-' + workflow_metrics['workflow_run_id']

    def prepareChart(sample_df, sample_name, compress_gaps=False):
        """
        Helper function to prepare, create, and save chart for a given sample
        """
        df = sample_df.copy()

        # Ensure datetime and calculate hours
        df[['start_time', 'end_time']] = df[['start_time', 'end_time']].apply(pd.to_datetime, errors='coerce')
        start = df['start_time'].min()
        df.loc[:, 'start_hr'] = (df['start_time'] - start).dt.total_seconds() / 3600
        df.loc[:, 'end_hr'] = (df['end_time'] - start).dt.total_seconds() / 3600

        if compress_gaps:
            df = compressGaps(df, threshold)
        
        if run_order:
            order_map = {}

            for i, workflow in enumerate(run_order):
                for fullname in df['workflow_fname'].unique():
                    if fullname.startswith(workflow):
                        abbr = abbr_name(fullname)
                        order_map[abbr] = i

            df['run_order_y'] = df['workflow_name'].map(order_map).fillna(len(run_order) + 1)
            df = df.sort_values(by='run_order_y')

        num_workflows = df['workflow_name_id'].nunique()
        ht = max(400, num_workflows * 30) if num_workflows > 1 else 200

        title = f'Gantt Chart of Workflow Runtime (Sample: {sample_name})'
        fig = createPlot(df, ht, title, run_order, abbreviations, bar_color, plot_bgcolor, show_abbr, grid_color, show_grid, paper_bgcolor, title_col, axtitle_col, axtick_col, title_size, axtitle_size, axtick_size)
        html_out = f"{html_file.replace('.html', f'_{sample_name}.html')}"
        fig.write_html(html_out)
        logging.info(f"Gantt chart saved to {html_out}")

    unique_samples = workflow_metrics['sample_name'].nunique()

    if unique_samples == 1:
        sample_name = workflow_metrics['sample_name'].iloc[0]
        prepareChart(workflow_metrics, sample_name, compress_gaps)
    else:
        for sample_name in workflow_metrics['sample_name'].unique():
            sample_df = workflow_metrics[workflow_metrics['sample_name'] == sample_name]
            prepareChart(sample_df, sample_name, compress_gaps)

def generateCSV(workflow_metrics, csv_file='workflow_report.csv'):
    '''
    Saves workflow metrics data to a CSV file.
    Parameters
    ----------
    workflow_metrics pd.DataFrame: The pandas dataframe containing workflow metrics.
    csv_file str: The CSV filename/path where the metrics are to be stored. 
    '''
    workflow_metrics.to_csv(csv_file, mode='w', header=True, index=False)
    logging.info(f"Workflow run metrics saved to {csv_file}")        

def processInput(input_file, config_file=None, compress_gaps=False, bar_color='#4682B4', plot_bgcolor='white', show_abbr=True, grid_color='lightgrey', show_grid=True, paper_bgcolor='white', title_col='black', axtitle_col='black', axtick_col='black', title_size=16, axtitle_size=14, axtick_size=12):
    '''
    Processes an input JSON or Text file to retrieve workflow run ids.
    Parameters
    ----------
    input_file str: Path to an input JSON or Text file that needs to be processed to extract workflow ids.
    config_file Optional[str]: Path to the workflow configuration file containing run order and dependencies. [Optional]
    '''
    # Check if file exists
    if not os.path.isfile(input_file):
        logging.error(f"File {input_file} not found.")
        return

    # Check if the input is a JSON or TXT file 
    if input_file.endswith('.json'):
        logging.info("Processing JSON for workflow ids")
        try:
            with open(input_file, 'r') as file:
                input_data = json.load(file)
            workflow_ids = extractWfId(input_data)
        except Exception as e:
            logging.error(f"Error loading JSON file: {e}")
            return

    elif input_file.endswith('.txt'):
        logging.info("Processing TXT for workflow ids")
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
        logging.error(f"Error: The input must be either '.json' or '.txt' file")
        return

    if not workflow_ids:
        logging.warning("No workflow IDs found.")
        return
    
    fp_path = "/scratch2/groups/gsi/production/vidarr/vidarr_files_report_latest.tsv.gz"
    df_sname = queryFpr(fp_path, workflow_ids)
    
    logging.info("Extracting workflow metrics for workflow_ids")
    data = queryMongoDB(workflow_ids)

    workflow_metrics = None
    if data is not None:
        workflow_metrics = parseJson(data, workflow_metrics)

    if workflow_metrics is not None and df_sname is not None:
        workflow_metrics = pd.merge(workflow_metrics, df_sname, left_on='workflow_run_id', right_on='workflow_run_id', how='left'). drop_duplicates()
    
    def append_tissue(row):
        if row['workflow_name'] == 'bamMergePreprocessing':
            return f"{row['workflow_name']}.{row['tissue_type']}"
        else:
            return row['workflow_name']

    workflow_metrics['workflow_name'] = workflow_metrics.apply(append_tissue, axis=1)
    workflow_metrics = workflow_metrics.drop(columns=['tissue_type'])   

    if workflow_metrics is None or workflow_metrics.empty:
        logging.warning("No workflow metrics to process. Exiting.")
        return

    ganttPlot(workflow_metrics, config_file, compress_gaps=compress_gaps, bar_color=bar_color, plot_bgcolor=plot_bgcolor, show_abbr=show_abbr, grid_color=grid_color, show_grid=show_grid, paper_bgcolor=paper_bgcolor, title_col=title_col, axtitle_col=axtitle_col, axtick_col=axtick_col, title_size=title_size, axtitle_size=axtitle_size, axtick_size=axtick_size)
    generateCSV(workflow_metrics)       

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = 'Generate gantt charts from workflow run metrics',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Add the argument for the input JSON of TXT file
    required = parser.add_argument_group('Required Arguments')
    required.add_argument(
        '-i', '--input',
        type = str,
        help = 'Path to the input JSON or TXT file containing workflow IDs',
        required = True
    )

    # Optional Arguments
    parser.add_argument(
        '--config',
        type = str,
        help = 'Path to the workflow configuration file containing run order, abbreviations and dependencies',
        required = False
    )

    parser.add_argument(
        '--compress-gaps',
        action='store_true',
        help='Compress long idle periods in Gantt chart'
    )
    parser.add_argument(
        '--bar-color',
        type=str,
        default='#4682B4',
        help='Color of the bars in the Gantt chart (default: #4682B4)'
    )

    parser.add_argument(
        '--plot-bgcolor',
        type=str,
        default='white',
        help='Background color of the plot area (default: white)'
    )

    parser.add_argument(
        '--no-abbr-table',
        action='store_true',
        help='If set, hides the abbreviation table below the Gantt chart'
    )

    parser.add_argument(
        '--grid-color',
        type=str,
        default='lightgrey',
        help='Color of gridlines on the plot (default: lightgrey)'
    )

    parser.add_argument(
        '--no-grid',
        action='store_true',
        help='If set, hides gridlines from the plot'
    )

    parser.add_argument(
        '--paper-bgcolor',
        type=str,
        default='white',
        help='Background color of the full figure (default: white)'
    )

    parser.add_argument(
        '--title-col',
        type=str,
        default='black',
        help='Color of the title text (default: black)'
    )

    parser.add_argument(
        '--axtitle-col',
        type=str,
        default='black',
        help='Color of the axis title text (default: black)'
    )

    parser.add_argument(
        '--axtick-col',
        type=str,
        default='black',
        help='Color of the axis tick text (default: black)'
    )

    parser.add_argument(
        '--title-size',
        type=int,
        default=16,
        help='Font size of the title text (default: 16)'
    )

    parser.add_argument(
        '--axtitle-size',
        type=int,
        default=14,
        help='Font size of the axis title text (default: 14)'
    )

    parser.add_argument(
        '--axtick-size',
        type=int,
        default=12,
        help='Font size of the axis tick text (default: 12)'
    )

    # Add custom message to show usage
    parser.epilog = f'''
    Example Usage: 
    pipeline-rt -i /path/to/input.json --compress-gaps
    pipeline-rt -i /path/to/input.txt --config /path/to/config.json --compress-gaps
    '''

    # Parse arguments
    args = parser.parse_args()

    if len(vars(args)) == 0:
        parser.print_help()
        exit(0)
    else:
        logging.info("Reading input")
        processInput(
            input_file = args.input, 
            config_file = args.config, 
            compress_gaps=args.compress_gaps, 
            bar_color=args.bar_color, 
            plot_bgcolor=args.plot_bgcolor, 
            show_abbr=not args.no_abbr_table, 
            grid_color=args.grid_color, 
            show_grid=not args.no_grid, 
            paper_bgcolor=args.paper_bgcolor, 
            title_col=args.title_col, 
            axtitle_col=args.axtitle_col, 
            axtick_col=args.axtick_col, 
            title_size=args.title_size, 
            axtitle_size=args.axtitle_size, 
            axtick_size=args.axtick_size
        )
