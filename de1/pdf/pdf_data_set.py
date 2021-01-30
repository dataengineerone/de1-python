from typing import Dict, Any
from kedro.io import AbstractDataSet, DataSetError
from typing import List

from .pdf_page import PDFPage


class PDFDataSet(AbstractDataSet):
    """
    PDFDataSet returns extracted text and tables from
    given pdf files, wrapping them in a PDFPage object.
    """

    def __init__(
            self,
            filepath: str,
    ):
        try:
            import pdfplumber
        except ModuleNotFoundError:
            raise DataSetError("PDFDataSet requires pdfplumber to be installed.")

        self._filepath = filepath

    def _load(self) -> List[PDFPage]:
        """
        Loads a list of PDFPage objects, with each index corresponding
        to the particular page that was loaded.
        :return:
        """
        try:
            import pdfplumber
        except ModuleNotFoundError:
            raise DataSetError("PDFDataSet requires pdfplumber to be installed.")

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
