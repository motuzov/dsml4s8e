from dsml4s8e.define_job import define_job
from dagstermill import local_output_notebook_io_manager
from dagster import job, MetadataValue, config_mapping, Config, RunConfig, Definitions

from pathlib import Path

_root_path = Path(__file__).parent.parent


class SimplifiedConfig(Config):
    a: int


configs = {"Nb0Cfg": type("Nb0Cfg", (Config,), {})}
setattr(configs["Nb0Cfg"], "a", int)


@config_mapping
def simplified_config(val: SimplifiedConfig) -> RunConfig:
    return RunConfig(
        ops={
            "nb": configs["Nb0Cfg"](a=val.a),
        }
    )


@job(
    name="simple_pipeline",
    tags={
        "cdlc_stage": "dev",
    },
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
    },
    metadata={
        "nb": MetadataValue.notebook(
            _root_path.joinpath(_root_path, "data_load/nb.ipynb")
        )
    },
    config=simplified_config,
)
def dagstermill_pipeline1():
    define_job(
        root_path=_root_path,
        nbs_sequence=[
            "data_load/nb.ipynb",
            # "data_load/nb_1.ipynb",
            # "data_load/nb_2.ipynb",
        ],
    )


@job(name="simple_pipeline1")
def dagstermill_pipeline2():
    define_job(
        root_path=_root_path,
        nbs_sequence=[
            "data_load/nb_0.ipynb",
            "data_load/nb_1.ipynb",
            "data_load/nb_2.ipynb",
        ],
    )


defs = Definitions(
    jobs=[dagstermill_pipeline1, dagstermill_pipeline2],
    resources={
        "output_notebook_io_manager": local_output_notebook_io_manager,
    },
)
