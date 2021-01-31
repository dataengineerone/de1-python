from pathlib import Path

import pytest
from kedro.io import DataSetError

from de1.zip import ZipFileDataSet


@pytest.fixture
def simple_zip() -> str:
    source_path = Path(__file__).parent / "data/simple_pdf.zip"
    return source_path.as_posix()


@pytest.fixture
def ignored_zip() -> str:
    source_path = Path(__file__).parent / "data/ignored_file.zip"
    return source_path.as_posix()


@pytest.fixture
def two_pdfs_zip() -> str:
    source_path = Path(__file__).parent / "data/two_pdfs.zip"
    return source_path.as_posix()


@pytest.fixture
def csv_zip() -> str:
    source_path = Path(__file__).parent / "data/csv.zip"
    return source_path.as_posix()


@pytest.fixture
def simple_zip_data_set(simple_zip):
    return ZipFileDataSet(filepath=simple_zip)


@pytest.fixture
def ignored_zip_data_set(ignored_zip):
    return ZipFileDataSet(filepath=ignored_zip)


@pytest.fixture
def invalid_two_pdfs_zip_data_set(two_pdfs_zip):
    return ZipFileDataSet(filepath=two_pdfs_zip)


@pytest.fixture
def valid_two_pdfs_zip_data_set(two_pdfs_zip):
    return ZipFileDataSet(filepath=two_pdfs_zip, filename="simple.pdf")


@pytest.fixture
def csv_zip_data_set(csv_zip):
    return ZipFileDataSet(filepath=csv_zip, dataset="pandas.CSVDataSet")


class TestZipFileDataSet:

    def test_simple_load(
            self,
            simple_zip_data_set: ZipFileDataSet,
    ):
        simple_pdf = simple_zip_data_set.load()
        assert len(simple_pdf) == 3028

    def test_ignored_load(
            self,
            simple_zip_data_set: ZipFileDataSet,
            ignored_zip_data_set: ZipFileDataSet,
    ):
        simple_pdf = simple_zip_data_set.load()
        ignored_pdf = ignored_zip_data_set.load()
        assert simple_pdf == ignored_pdf

    def test_valid_two_pdfs(
            self,
            simple_zip_data_set: ZipFileDataSet,
            valid_two_pdfs_zip_data_set: ZipFileDataSet,
    ):
        simple_pdf = simple_zip_data_set.load()
        valid_two_pdfs = valid_two_pdfs_zip_data_set.load()
        assert simple_pdf == valid_two_pdfs

    def test_invalid_two_pdfs(
            self,
            invalid_two_pdfs_zip_data_set: ZipFileDataSet,
    ):
        with pytest.raises(DataSetError):
            assert invalid_two_pdfs_zip_data_set.load()

    def test_csv_dataset(
            self,
            csv_zip_data_set: ZipFileDataSet,
    ):
        csv_data = csv_zip_data_set.load()
        assert len(csv_data) == 2
