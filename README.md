# Pipeline Timings #

Scripts in this repo are used to generate workflow run time diagrams. 
`workflow_rt.py` is used to plot run times by extracting workflow IDs from input JSON files.  

## Getting Started ##

### Installation ###

On your command line load the module `pipeline-rt` [Current available version is 1.0.0]. 
```
module load pipeline-rt
```
This downloads the two `.py` scripts in the repository and places them in the `share/` folder. Furthermore, it loads all required python libraries and makes the `workflow_rt.py` script available in the path to generate workflow run time diagrams. 

## Usage ##

Two examples of using the script are as follows:

<b>Example 1</b>
```
pipeline-rt -i input.json
```
This will pull workflow IDs from `input.json` and, workflow metrics from `/.mounts/labs/gsi/secrets/`, to create a metrics table and plot written out to `workflow_report.csv` and `wrt_gantt_v1.png` respectively. 


<b>Example 2</b>

```
pipeline-rt -i input.json --config workflow_config.json
```
This will pull workflow IDs from `input.json` and, workflow metrics from `/.mounts/labs/gsi/secrets/`, to create a metrics table and two plots written out to `workflow_report.csv`, `wrt_gantt_v1.png` and `wrt_gantt_v2.png` respectively. Plot `wrt_gantt_v1.png` follows the default y axis ordering, by workflow start time, and plot `wrt_gantt_v2.png` follows the user provided y axis ordering by workflow run order. 

Parameters

| argument | purpose | required/optional                                    |
| ------- | ------- | ------------------------------------------ |
| -i | Path to input JSON file  | required              |
| --config | Path to workflow run order and workflow dependency | optional              |

- Input File `-i / --input`:
Required parameter. The path to the input JSON file.
Check to see the structure of the input file below.

- Config File `--config`:
Optional parameter. Workflow Run Order and Workflow Dependency. 
Using this flag will ensure y axis ordering is by workflow run order and links between different workflows showing workflow dependency are created.
Check to see the structure of the config file below.

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

#### Basic config json structure ####

To create a config file with workflow run order and workflow dependency follow the template below:

```
{"workflow_run_order":
        ["Workflow 1", "Workflow 2", "Workflow 3"],
 "dependencies":
        {
         "Workflow 1": ["Workflow 2", "Workflow 3"],
         "Workflow 2": ["Workflow 3"]
        }
}   
```


