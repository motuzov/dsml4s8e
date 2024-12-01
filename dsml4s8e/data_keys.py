from dataclasses import dataclass
import json
from typing import List


@dataclass(frozen=True)
class DataKyes:
    """
    list of catalog data paths
    data key format: <pipeline>.<component>.<notebook>.<data_obj_name>
    """

    keys: tuple[str]


def _make_data_key(op_id: str, var_name: str):
    return f"{op_id}.{var_name}"


def kye2pathvar_name(key: str):
    return "_".join(["path"] + key.split(".")[-2:])


class NotebookData:
    def __init__(self, ins: List[str], out_var_names: List[str], op_id: str):
        """
        ins is a list of input data keys
        out_vars  is a list of local variable names
        format of keys:
        <pipeline>.<component>.<netebook>.<data_obj_name>
        """
        self.ins = DataKyes(ins)
        self.outs = DataKyes(
            [_make_data_key(op_id, var_name) for var_name in out_var_names]
        )

    def __str__(self):
        interface_info = {"ins": self.ins, "outs": self.outs}
        return json.dumps(interface_info, indent=4)
