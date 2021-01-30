from typing import NamedTuple, List


class PDFPage(NamedTuple):
    """
    PDFPage to store read PDF data
    """
    text: str
    table: List
