import os
from pathlib import PurePath

import pytest
from de1.empty import EmptyPartitionedDataSet


@pytest.fixture
def path_empty(tmp_path):
    return (tmp_path / "empty_dataset_folder").as_posix()


@pytest.fixture
def empty_data_set(path_empty):
    return EmptyPartitionedDataSet(path=path_empty, dataset="json.JSONDataSet")


@pytest.fixture
def dummy_data():
    return {"col1": 1, "col2": 2, "col3": 3}


class TestEmptyPartitionedDataSet:

    def test_save_and_load(self, empty_data_set, dummy_data, path_empty: PurePath):
        """Test saving and reloading the data set."""
        assert os.path.exists(str(path_empty))
        assert len(empty_data_set.load()) == 0
        empty_data_set.save({'one': dummy_data})
        assert len(empty_data_set.load()) == 1
        assert empty_data_set.load()['one']() == dummy_data
