from abc import ABC, abstractmethod
from typing import List, Dict
from .nb_data_keys import DataKeys


class StorageCatalogABC(ABC):
    @property
    @abstractmethod
    def url_prefix(self):
        ...

    @abstractmethod
    def __init__(self, runid):
        self.runid = runid

    @abstractmethod
    def is_valid(self) -> bool:
        ...

    @abstractmethod
    def get_out_urls(self,
                     data_kyes: DataKeys
                     ) -> Dict[str, str]:
        """
        urls which have the prefix
        """
        ...
