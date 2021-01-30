import os
from pathlib import PurePath

import pytest
from de1.lazy import LazyPartitionedDataSet


@pytest.fixture
def path_lazy(tmp_path):
    return (tmp_path / "lazy_dataset_folder").as_posix()


@pytest.fixture
def lazy_data_set(path_lazy):
    return LazyPartitionedDataSet(path=path_lazy, dataset="json.JSONDataSet")


@pytest.fixture
def dummy_data():
    return {"col1": 1, "col2": 2, "col3": 3}


class TestLazyPartitionedDataSet:

    def test_save_and_load(self, lazy_data_set, dummy_data, path_lazy: PurePath):
        """Test saving and reloading the data set."""
        assert os.path.exists(str(path_lazy))
        assert len(lazy_data_set.load()) == 0
        lazy_data_set.save({'one': lambda: dummy_data})
        assert len(lazy_data_set.load()) == 1
        assert lazy_data_set.load()['one']() == dummy_data
