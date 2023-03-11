from dagster import seven as __dm_seven
import dagstermill
import os


def load_params():
    params = {
        key: __dm_seven.json.loads(value)
        for key, value
        in {'executable_dict': '{"__class__": "ReconstructablePipeline", "asset_selection": null, "pipeline_name": "dagstermill_pipeline", "repository": {"__class__": "ReconstructableRepository", "container_context": null, "container_image": null, "entry_point": ["dagster"], "executable_path": "/opt/conda/bin/python3.10", "pointer": {"__class__": "FileCodePointer", "fn_name": "dagstermill_pipeline", "python_file": "dag.py", "working_directory": "/home/jovyan/work/dev/pipeline_example/dsml4s8e/examples/simple_pipeline/dag"}, "repository_load_data": null}, "solid_selection_str": null, "solids_to_execute": null}', 'pipeline_run_dict': '{"__class__": "PipelineRun", "asset_selection": null, "execution_plan_snapshot_id": "17af11f5af5d8ee18c7d7d115eb3d40e8b0eb64d", "external_pipeline_origin": {"__class__": "ExternalPipelineOrigin", "external_repository_origin": {"__class__": "ExternalRepositoryOrigin", "repository_location_origin": {"__class__": "ManagedGrpcPythonEnvRepositoryLocationOrigin", "loadable_target_origin": {"__class__": "LoadableTargetOrigin", "attribute": null, "executable_path": null, "module_name": null, "package_name": null, "python_file": "dag.py", "working_directory": "/home/jovyan/work/dev/pipeline_example/dsml4s8e/examples/simple_pipeline/dag"}, "location_name": "dag.py"}, "repository_name": "__repository__dagstermill_pipeline"}, "pipeline_name": "dagstermill_pipeline"}, "has_repository_load_data": false, "mode": "default", "parent_run_id": null, "pipeline_code_origin": {"__class__": "PipelinePythonOrigin", "pipeline_name": "dagstermill_pipeline", "repository_origin": {"__class__": "RepositoryPythonOrigin", "code_pointer": {"__class__": "FileCodePointer", "fn_name": "dagstermill_pipeline", "python_file": "dag.py", "working_directory": "/home/jovyan/work/dev/pipeline_example/dsml4s8e/examples/simple_pipeline/dag"}, "container_context": {}, "container_image": null, "entry_point": ["dagster"], "executable_path": "/opt/conda/bin/python3.10"}}, "pipeline_name": "dagstermill_pipeline", "pipeline_snapshot_id": "bfd9741c2425ac8838351672eb9671d72330fe22", "root_run_id": null, "run_config": {"ops": {"op_nb_1": {"config": {"a": 2020}}}}, "run_id": "d5aa0d30-6e10-46fe-8cb2-44f655748498", "solid_selection": null, "solids_to_execute": null, "status": {"__enum__": "PipelineRunStatus.STARTING"}, "step_keys_to_execute": null, "tags": {".dagster/grpc_info": "{\\"host\\": \\"localhost\\", \\"socket\\": \\"/tmp/tmpdg9x6lum\\"}", "cdlc_stage": "dev", "dagster/solid_selection": "*"}}', 'node_handle_kwargs': '{"name": "op_nb_2", "parent": null}', 'instance_ref_dict': '{"__class__": "InstanceRef", "compute_logs_data": {"__class__": "ConfigurableClassData", "class_name": "LocalComputeLogManager", "config_yaml": "base_dir: /home/jovyan/work/dev/pipeline_example/dsml4s8e/examples/simple_pipeline/dag/daghome/storage\\n", "module_name": "dagster.core.storage.local_compute_log_manager"}, "custom_instance_class_data": null, "event_storage_data": {"__class__": "ConfigurableClassData", "class_name": "SqliteEventLogStorage", "config_yaml": "base_dir: /home/jovyan/work/dev/pipeline_example/dsml4s8e/examples/simple_pipeline/dag/daghome/history/runs/\\n", "module_name": "dagster.core.storage.event_log"}, "local_artifact_storage_data": {"__class__": "ConfigurableClassData", "class_name": "LocalArtifactStorage", "config_yaml": "base_dir: /home/jovyan/work/dev/pipeline_example/dsml4s8e/examples/simple_pipeline/dag/daghome\\n", "module_name": "dagster.core.storage.root"}, "run_coordinator_data": {"__class__": "ConfigurableClassData", "class_name": "DefaultRunCoordinator", "config_yaml": "{}\\n", "module_name": "dagster.core.run_coordinator"}, "run_launcher_data": {"__class__": "ConfigurableClassData", "class_name": "DefaultRunLauncher", "config_yaml": "{}\\n", "module_name": "dagster"}, "run_storage_data": {"__class__": "ConfigurableClassData", "class_name": "SqliteRunStorage", "config_yaml": "base_dir: /home/jovyan/work/dev/pipeline_example/dsml4s8e/examples/simple_pipeline/dag/daghome/history/\\n", "module_name": "dagster.core.storage.runs"}, "schedule_storage_data": {"__class__": "ConfigurableClassData", "class_name": "SqliteScheduleStorage", "config_yaml": "base_dir: /home/jovyan/work/dev/pipeline_example/dsml4s8e/examples/simple_pipeline/dag/daghome/schedules\\n", "module_name": "dagster.core.storage.schedules"}, "scheduler_data": {"__class__": "ConfigurableClassData", "class_name": "DagsterDaemonScheduler", "config_yaml": "{}\\n", "module_name": "dagster.core.scheduler"}, "secrets_loader_data": null, "settings": {}, "storage_data": {"__class__": "ConfigurableClassData", "class_name": "DagsterSqliteStorage", "config_yaml": "base_dir: /home/jovyan/work/dev/pipeline_example/dsml4s8e/examples/simple_pipeline/dag/daghome\\n", "module_name": "dagster.core.storage.sqlite_storage"}}', 'step_key': '"op_nb_2"', 'output_log_path': '"/tmp/tmpz_nj4ne4"', 'marshal_dir': '"/tmp/dagstermill/d5aa0d30-6e10-46fe-8cb2-44f655748498/marshal"', 'run_config': '{"ops": {"op_nb_1": {"config": {"a": 2020}}}}'}.items()
    }
    return params


def reconstitute_pipeline_contex(params):
    context = dagstermill._reconstitute_pipeline_context(**params)
    return context


if __name__ == '__main__':
    params = load_params()
    os.chdir(params['executable_dict']['repository']['pointer']['working_directory'])
    print(os.getcwd())
    context = reconstitute_pipeline_contex(params)
    nb_1_data1 = dagstermill._load_input_parameter('nb_1_data1')
    print(nb_1_data1)
