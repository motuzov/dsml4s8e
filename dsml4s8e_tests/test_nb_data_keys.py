from dsml4s8e import data_keys


def test_nb_data_keys():
    nb_data_keys_ = data_keys.NotebookData(
        ins={"data_key": "alias_data_key"},
        out_var_names=["data1"],
        op_id="pipeline_example.data_load.dagstermill",
    )
    assert nb_data_keys_.outs.keys[0] == "pipeline_example.data_load.dagstermill.data1"
