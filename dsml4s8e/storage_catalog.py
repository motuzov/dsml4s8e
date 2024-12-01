from abc import ABC, abstractmethod
from typing import Dict
from dsml4s8e.data_keys import DataKyes


class StorageCatalog(ABC):
    @abstractmethod
    def __init__(self, runid):
        self.runid = runid

    @abstractmethod
    def is_valid(self) -> bool: ...

    @abstractmethod
    def get_outs_data_paths(self, data_keys: DataKyes) -> Dict[str, str]:
        """
        Mapping data_keys to storage paths
        """
        ...
