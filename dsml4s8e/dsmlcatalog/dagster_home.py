from dsml4s8e import StorageCatalog, DataKyes
from typing import Dict

import os


def _key2storage_path(key: str, prefix: str) -> str:
    """
    format of key: <pipeline>.<component>.<notebook>.<name_data_opj>
    cdlc_stage is a stage of component development life cycle: dev, test, ops
    return a storage path
    fromat of a starge path: <prefix>/<pipeline>/<component>/<notebook>/<name_data_opj>
    """
    nb, name = key.split(".")[-2:]
    return f"{prefix}/{nb}/{name}"


class DagsterStorageCatalog(StorageCatalog):
    def __init__(self, dagster_context):
        dgd_home = os.environ["DAGSTER_HOME"]
        self._prefix = f"{dgd_home}/storage/{dagster_context.run.run_id}"

    def is_valid(self) -> bool:
        return True

    def get_outs_data_paths(self, data_keys: DataKyes) -> Dict[str, str]:
        return dict(
            [
                (
                    key,
                    _key2storage_path(key=key, prefix=self._prefix),
                )
                for key in data_keys.keys
            ]
        )
