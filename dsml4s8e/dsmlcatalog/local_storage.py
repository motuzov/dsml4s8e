from dsml4s8e import DataKyes, StorageCatalog
from typing import Dict


def _k2storage_path(key: str, run_id: str, prefix: str) -> str:
    """
    format of key: <pipeline>.<component>.<notebook>.<name_data_opj>
    cdlc_stage is a stage of component development life cycle: dev, test, ops
    return a storage path
    format of a storagr path: <prefix>/<pipeline>/<component>/<notebook>/<name_data_opj>
    """
    p, c, nb, e = key.split(".")
    return f"{prefix}/{p}/{c}/{run_id}/{nb}/{e}"


class LoacalStorageCatalog(StorageCatalog):
    def __init__(self, prefix: str, dagster_context):
        self.cdlc_stage = dagster_context.op_def.tags.get("cdlc_stage", "dev")
        self._prefix = f"{prefix}/{self.cdlc_stage}"
        self.run_id = dagster_context.run.run_id

    @property
    def url_prefix(self):
        return self._prefix

    def is_valid(self) -> bool:
        return True

    def get_outs_data_paths(self, data_keys: DataKyes) -> Dict[str, str]:
        return dict(
            [
                (k, _k2storage_path(k, self.run_id, self.url_prefix))
                for k in data_keys.keys
            ]
        )
