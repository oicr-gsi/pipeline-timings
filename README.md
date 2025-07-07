# Pipeline Timings #

Scripts in this repo are used to generate workflow run time diagrams. 
`workflow_rt.py` is used to plot run times by extracting workflow IDs from input JSON/TXT files.  

## Getting Started ##

### Installation ###

On your command line load the module `pipeline-rt` [Current available version is 1.0.0]. 
```
module load pipeline-rt
```
This downloads the two `.py` scripts from the repository and places them in the `share/` folder. Furthermore, it loads all required python libraries and makes the `workflow_rt.py` script available in the path to generate workflow run time diagrams. 

## Usage ##

Two examples of using the script are as follows:

<b>Example 1</b>
```
pipeline-rt -i [input.json] or [input.txt]
```
This will pull workflow IDs from `input.json` or `input.txt` and, workflow metrics from `/.mounts/labs/gsi/secrets/`, to create a metrics table and plot written out to `workflow_report.csv` and `gantt_v1_<sample>.png` respectively. If the input includes multiple cases, one plot will be generated per case. 


<b>Example 2</b>

```
pipeline-rt -i [input.json] or [input.txt] --config [workflow_config.json]
```
This will pull workflow IDs from `input.json` or `input.txt` and, workflow metrics from `/.mounts/labs/gsi/secrets/`, to create a metrics table and plot `workflow_report.csv`, `gantt_v1_<sample>.png` respectively. The y-axis ordering of the plot will be based on the order defined in the config file. 

Additionally, if included in the config file, the workflows will be filtered to include only the ones specified in the `workflow_to_include` list and the workflow names will be abbreviated based on the `workflow_abbreviations` dict. 

Parameters

| argument | purpose | required/optional                                    |
| ------- | ------- | ------------------------------------------ |
| -i | Path to input JSON/TXT file  | required              |
| --config | Path to workflow run order and workflow dependency | optional              |
| --compress-gaps | Compress long idle periods in the Gantt chart | optional | 
| --bar-color | Set the color of the bars in the Gantt plot (default: #4682B4) | optional |
| --plot-bgcolor | Set the background color of the plot area (default: white) | optional |
| --paper-bgcolor | Set the background color of the full figure (default: white) | optional |
| --no-abbr-table | If set, hides the abbreviation lookup table from the plot | optional |
| --grid-color | Set color of gridlines on the plot (default: lightgrey) | optional |
| --no-grid | If set, removes gridlines | optional |
| --title-col | Set the plot's title color (default: black) | optional |
| --title-size | Set the plot's title size (default: 16) | optional |
| --axtitle-col | Set the color of the x- and y-axis titles (default: black) | optional |
| --axtitle-size | Set the size of the x- and y-axis titles (default: 14) | optional |
| --axtick-col | Set the color of the x- and y-axis tick labels (default: black) | optional |
| --axtick-size | Set the size of the x- and y-axis tick labels (default: 12) | optional |


- Input File `-i / --input`:
Required parameter. The path to the input JSON/TXT file.
Check to see the structure of the input file below.

- Config File `--config`:
Optional parameter. Workflow Run Order, Workflows to include in the plot and Workflow Name Abbreviations  
Using this flag and providing a config file will ensure y axis ordering is by workflow run order, only select workflows are included in the plot and long workflow names are abbreviated. Check to see the structure of the config file below.

#### Basic input json structure ####

The basic structure for the input file is organized with sample ids, and workflow names and ids.

```
{donor_id:
   {sample_id:
      {workflow_1:
         [{"workflow_id":"19168526", "workflow_version":"2.0.2"}],
       workflow_2:
         [{"workflow_id":"16962244", "workflow_version":"2.0.2"}]
      }
   }
}   
```

#### Basic input text structure ####

The basic structure for the input file is organized with workflow ids.

```
id_1
id_2
id_3
```

#### Basic config json structure ####

To create a config file (`workflow_config.json`) with workflow run order, workflows to include and abbreviations follow the template below:

```
{
  "workflow_run_order": [
    "bamMergePreprocessing",
    "mutect2",
    "variantEffectPredictor",
    "gridss",
    "purple",
    "arriba",
    "rsem",
    "starFusion",
    "calculateContamination",
    "sequenza",
    "strelkaSomatic",
    "mrdetect",
    "callability"
  ],
  "workflow_to_include": [
    "bamMergePreprocessing",
    "mutect2",
    "variantEffectPredictor",
    "gridss",
    "purple",
    "starFusion",
    "calculateContamination",
    "sequenza",
    "strelkaSomatic",
    "mrdetect",
    "callability"
  ],
  "workflow_abbreviations": {
    "bamMergePreprocessing": "bmpp",
    "variantEffectPredictor": "vep",
    "calculateContamination" : "cc"
  }
}  
```
The second `.py` script is `plot.py` which is made available by the module in the `share/` directory. This python script helps generate gantt plots directly from the `workflow_rt.py` csv output allowing more flexibility to the user in customization of the plot. 

