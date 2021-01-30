from pathlib import Path
from typing import List

import pytest
from de1.pdf import PDFDataSet, PDFPage


@pytest.fixture
def simple_pdf() -> str:
    source_path = Path(__file__).parent / "data/simple.pdf"
    return source_path.as_posix()


@pytest.fixture
def table_pdf() -> str:
    source_path = Path(__file__).parent / "data/table.pdf"
    return source_path.as_posix()


@pytest.fixture
def simple_pdf_data_set(simple_pdf):
    return PDFDataSet(filepath=simple_pdf)


@pytest.fixture
def table_pdf_data_set(table_pdf):
    return PDFDataSet(filepath=table_pdf)


class TestPDFDataSet:

    def test_simple_load(self, simple_pdf_data_set: PDFDataSet):
        pdf_pages: List[PDFPage] = simple_pdf_data_set.load()
        assert len(pdf_pages) == 2
        for page in pdf_pages:
            assert page.table == []
        assert pdf_pages[0].text.split('\n')[0].strip() == 'A Simple PDF File'

    def test_table_load(self, table_pdf_data_set: PDFDataSet):
        pdf_pages: List[PDFPage] = table_pdf_data_set.load()
        assert len(pdf_pages) == 1
        assert pdf_pages[0].table[0][13][0] == 'Mobility'
