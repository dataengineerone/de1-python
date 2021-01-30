from typing import Any, Dict, Callable
from de1.empty import EmptyPartitionedDataSet


class LazyPartitionedDataSet(EmptyPartitionedDataSet):
    """
    LazyPartitionedDataSet allows users to save data in an incremental way,
    without requiring all data to be loaded at the same time in order to be saved.

    Similar to how PartitionedDataSet loads things lazily, LazyPartitionedDataSet also saves things lazily.
    """

    def _save(self, data: Dict[str, Callable[[], Any]]) -> None:
        """
        :param data: A dictionary of functions that returns the data to be saved for the particular key
        """
        from copy import deepcopy
        for partition_id, partition_function in sorted(data.items()):
            kwargs = deepcopy(self._dataset_config)
            partition = self._partition_to_path(partition_id)
            # join the protocol back since tools like PySpark may rely on it
            kwargs[self._filepath_arg] = self._join_protocol(partition)
            dataset = self._dataset_type(**kwargs)  # type: ignore
            data = partition_function()
            dataset.save(data)
            del data
        self._invalidate_caches()
