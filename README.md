
# Dsml4s8e
**D**ata **S**cience / **ML** flow(**4**) **s**tandalon(**8**)**e**
**Dsml4s8e** is a python library that aims to extend Dagster to: 

 1. Simplify building of pipelines from **standalone** notebooks
 2. Standardize a structure of ML/DS pipeline projects to easy share and continuous improve them
 3. Manage pipelines data in a process of pipelines continuous improvement

Dsml4s8e designed to support the following workflow:
 1. Define a project structure for your pipeline and a structure of the pipeline data catalog, see about `StorageCatalogABC`
 2. Develop **standalone** Jupyter notebooks with **clear interface**, see specification below
 3. Build a **pipeline** and deloy in vary environments(experimental/test/prod) and on vary infrastructure
 4. Configure and execute the certain pipeline version many times in vary environments and on vary infrastructure 

Dsml4s8e proper for build a cloud agnostic DSML platform.

You can play with a demo pipeline skelet project:

https://github.com/dsml4/pipeline_skelet

## Installation of local dev container
```bash
# Create a work directory to clone a git repository. The work directory will be mounted to the container
mkdir work
#Step into the work directory
cd work
# Clone repository
git clone https://github.com/dsml4/dsml4s8e.git
# Step into a derictory with a Dockerfile
cd dsml4s8e/images/dev
# Build a image
docker build -t dsml4s8e .
# Go back into the work directory to a correct using pwd command inside the next docker run instruction
cd ../../../
# Create and run a container staying in the work directory.
docker run --rm --name my_dag -p 3000:3000 -p 8888:8888 -v $(pwd):/home/jovyan/work -e DAGSTER_HOME=/home/jovyan/work/daghome dsml4s8e bash work/dsml4s8e/setup_pipeline.sh
```

Open JupyterLab in a browser: http://localhost:8888/lab

Open Dagster in a browser: http://localhost:3000/


<img width="1387" alt="simple_pipeline_Overview" src="https://user-images.githubusercontent.com/1010096/232596393-a7da68b5-9d17-4e78-bc85-123bf756976d.png">


## The standalone notebook specification

A standalone notebook is a main building block in our pipelines building flow.

To do notebooks capable to be joined into a dagster pipeline we need to add **4** cells to a notebook. Next, we will discuss what concerns are addressed each of the cells and what library classes are responsible for each one.


<img width="945" alt="notebook_4_cell_specification" src="https://user-images.githubusercontent.com/1010096/232596564-99a1108d-cbcc-4ded-84fb-ba4068f1b24f.png">


### The 1th cell with the op_parameters tag

A cell tagged `op_parameters` responses for an integration of a standalone notebook with Dagster. In the load stage dictionary of parameters from this cell is trasformed to Dagstermill format and passed to a function `define_dagstermill_op` from dagstermill library to make parameters avalible in Dagster Launchpad to edit run configuration in the launge stage.

A definition of a Dagster op in a standalone notebook in JupyterLab:

<img width="982" alt="op_parameters" src="https://user-images.githubusercontent.com/1010096/232597083-9601c183-fbaf-4c98-ad8b-9e3d57e0b162.png">

Configure op in Dagster Launchpad:


<img width="808" alt="simple_pipeline" src="https://user-images.githubusercontent.com/1010096/232597525-36053177-5b58-4aa9-8668-6912c7c63036.png">


### The 2th cell cell with the parameters tag

The `parameters` cell responses for setting a notebook run configuration.
The function `get_context` sets default values from config_schema in cell `op_parameters` to `context`.

```python
context = op.get_context()
```

If a notebook is executed by Dagster as an Op then it replaces the `parameter` cell with the `injected-parameters` cell, thus the variable `context` used in a notebook body when it run in standalone mode as well as Dagster Op.

<img width="999" alt="runs" src="https://user-images.githubusercontent.com/1010096/232601657-a8e3788a-96a8-4043-a14d-77306f7318f4.png">

<img width="1001" alt="open_path" src="https://user-images.githubusercontent.com/1010096/232603425-b6db4e71-893d-4633-8713-f1feaf96ecbb.png">

<img width="1180" alt="out_nb_2" src="https://user-images.githubusercontent.com/1010096/232603465-41f3f647-4898-439f-95de-f00af1ba8da8.png">


### The 3th cell with a notebook data catalog initialization

A name of a path variable must be unique for each notebook namespace where the variable is used, thus a name of path variable could be shorter than the corresponding catalog path.


For each of outputs a method `op.get_catalog()` creates catalog paths. The metod `op.get_catalog()` call the catalog paths to create paths in the specific storage(the file system) by catalog paths.
LocalStorage derived from `StorageCatalogABC`, this class responsible for a structure of a data catalog in a certain storage, but a catalog structure is fully customizable using `StorageCatalogABC`.
For each output from the output list method of `op.get_catalog()` generate catalog paths and this method uses the catalog object to create paths in the specific storage(the file system) by catalog paths.


<img width="951" alt="Initialize_catalog_outs" src="https://user-images.githubusercontent.com/1010096/232607728-c01acde4-bbbd-4bab-9cb6-25ab111fe172.png">


### The 4th cell responsible for outs passing to the next step

In the 4th cell we inform of next steps of a pipeline where produced data are stored. Now we can link this notebook(step) with the next notebooks representing pipeline steps. To do this we need to declare this notebook outputs as inputs for the next netbooks. We can copy strings from the cell output and paste them to `NbOp` declarations in the cells `op_parametrs` of the next notebooks in a pipeline.

Join a `ins` dictionary from `NbOp` and paths variables from the cell `parameters` passed to `NbOp` by passing variable `locals_=locals()` 

```python
op.pass_outs_to_next_step()

```

<img width="1758" alt="copy_passte_ins_variables" src="https://user-images.githubusercontent.com/1010096/232604788-42a54dba-f098-47f5-ab5d-a8fbf90e0197.png">


## Define a Dagster job

A function `define_job` automate a Dagster job definition.

```python
# dag.py

from dsml4s8e.define_job import define_job
from dagstermill import local_output_notebook_io_manager
from dagster import job

from pathlib import Path


@job(
    name='simple_pipeline',
    tags={"cdlc_stage": "dev"},
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
    }
)
def dagstermill_pipeline():
    module_path = Path(__file__)
    define_job(
        root_path=module_path.parent.parent,
        nbs_sequence=[
            "data_load/nb_0.ipynb",
            "data_load/nb_1.ipynb",
            "data_load/nb_2.ipynb"
        ]
    )


```

As a result, a dagster pipeline will be built from standalone notebooks:
  
![Simple Pipeline](https://user-images.githubusercontent.com/1010096/232598898-b536ec12-26da-4693-a4e9-ba15858164de.svg)
