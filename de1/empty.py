import os
from copy import deepcopy
from typing import Any, Dict, Callable, Union, Type, List
from kedro.io import PartitionedDataSet, AbstractDataSet


class EmptyPartitionedDataSet(PartitionedDataSet):
    """
    EmptyPartitionedDataSet is exactly the same as a PartitionedDataSet
    except that it allows reading from a location that has no files or data.

    If a location does not exist, this dataset will create the path.
    If a location is empty, this dataset will return an empty dictionary.
    """

    def __init__(
            self,
            path: str,
            dataset: Union[str, Type[AbstractDataSet], Dict[str, Any]],
            filepath_arg: str = "filepath",
            filename_suffix: str = "",
            credentials: Dict[str, Any] = None,
            load_args: Dict[str, Any] = None,
    ):

        os.makedirs(path, exist_ok=True)

        super().__init__(path, dataset, filepath_arg, filename_suffix, credentials, load_args)

    def _load(self) -> Dict[str, Callable[[], Any]]:
        partitions = {}

        for partition in self._list_partitions():
            kwargs = deepcopy(self._dataset_config)
            # join the protocol back since PySpark may rely on it
            kwargs[self._filepath_arg] = self._join_protocol(partition)
            dataset = self._dataset_type(**kwargs)  # type: ignore
            partition_id = self._path_to_partition(partition)
            partitions[partition_id] = dataset.load

        return partitions

    def _list_partitions(self) -> List[str]:
        return [
            path
            for path in self._filesystem.find(self._normalized_path, **self._load_args)
            if path.endswith(self._filename_suffix)
        ]
