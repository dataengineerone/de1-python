from typing import Dict, Any

from kedro.io import AbstractDataSet, DataSetError

from typing import NamedTuple, List


try:
    import pdfplumber
except ModuleNotFoundError:
    raise DataSetError("PDFDataSet requires pdfplumber to be installed.")


class PDFPage(NamedTuple):
    """
    PDFPage to store read PDF data
    """
    text: str
    table: List


class PDFDataSet(AbstractDataSet):
    """
    PDFFileDataSet returns extracted text and tables from
    given pdf files
    """

    def __init__(
            self,
            filepath: str,
    ):

        self._filepath = filepath

    def _load(self) -> List[PDFPage]:
        with pdfplumber.open(self._filepath) as pdf:
            pages = []
            for page in range(len(pdf.pages)):
                first_page = pdf.pages[page]
                tbl = first_page.extract_tables()
                txt = first_page.extract_text()
                pages.append(PDFPage(txt, tbl))
            return pages

    def _save(self, data: Any) -> None:
        raise DataSetError(f'Saving is unsupported')

    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._filepath,
        )
