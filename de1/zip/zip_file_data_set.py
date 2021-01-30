from typing import Any, Dict
from kedro.io import AbstractDataSet, DataSetError


class ZipFileDataSet(AbstractDataSet):
    """
    ZipFileDataSet decompresses and extracts files from zip files.
    Expects to return a single file from the unzipped dataset,
    and supports multiple methods for filtering sets of files.
    """

    def __init__(
            self,
            filepath: str,
            filename: str = None,
            filename_suffix: str = None,
            ignored_prefixes: str = None,
            credentials: Dict[str, str] = None,
    ):
        self._filepath = filepath
        self._filename = filename
        self._filename_suffix = filename_suffix
        self._ignored_prefixes = ignored_prefixes or ['_', '.']
        credentials = credentials or {}
        self._password = credentials.get('password', credentials.get('pwd'))

    def _is_ignored_prefix(self, name):
        for ignored_prefix in self._ignored_prefixes:
            if name.startswith(ignored_prefix):
                return True
        else:
            return False

    def _load(self) -> bytes:
        import zipfile
        with zipfile.ZipFile(self._filepath) as zipped:
            namelist = zipped.namelist()
            if self._filename_suffix is not None:
                namelist = [
                    name for name in namelist if name.lower().endswith(self._filename_suffix)
                ]
            namelist = [
                name
                for name in namelist if not self._is_ignored_prefix(name)
            ]
            if len(namelist) > 1 and self._filename is None:
                raise DataSetError(f'Multiple files found! Please specify which file to extract: {namelist}')
            if len(namelist) <= 0:
                raise DataSetError(f'No files found in the archive!')

            target_filename = namelist[0]
            if self._filename is not None:
                target_filename = self._filename

            with zipped.open(target_filename, pwd=self._password) as zipped_file:
                return zipped_file.read()

    def _save(self, data: Any) -> None:
        raise DataSetError(f'Saving is unsupported')

    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._filepath,
            filename=self._filename,
            filename_suffix=self._filename_suffix,
            ignored_prefixes=self._ignored_prefixes,
        )

