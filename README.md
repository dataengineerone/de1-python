# DE1 Python Package

![image](images/logo.png)

Curated collection of DE1's favorite kedro utilities.

## Installation

`pip install de1`


### DataSets

#### EmptyPartitionedDataSet

For those times when data is not yet available in a particular folder, or if no data is a valid value.

Particularly useful when doing sub-node parallelization.

##### Example usage

```yaml
# catalog.yml
empty_json_collection:
    type: de1.empty.EmptyPartitionedDataSet
    path: data/02_intermediate/json_collection
    dataset: json.JSONDataSet
```

```python

 empty_json = catalog.load('empty_json_collection')
 assert empty_json.load() == {}

```


#### LazyPartitionedDataSet

For when the data is too big to calculate all at once, and requires at least some clean-up in the process.

##### Example Usage

```yaml
# catalog.yml
lazy_json_collection:
    type: de1.lazy.LazyPartitionedDataSet
    path: data/02_intermediate/json_collection
    dataset: json.JSONDataSet
```

```python
data = {
    'key1': lambda: 'HI',
    'key2': lambda: 'BYE',
}
catalog.save('lazy_json_collection', data)
lazy_json_collection = catalog.load('lazy_json_collection')
assert lazy_json_collection['key1']() == 'HI'
```


#### PDFDataSet

A dataset that uses `pdfplumber` to extract text and tables from pdf files.

Data gets returned as a `PDFPage` object.

##### Example Usage

```yaml
# catalog.yml
invoice_pdf:
    type: de1.pdf.PDFDataSet
    filepath: data/01_raw/invoice.pdf
```

```python
from de1.pdf import PDFPage

pdf_page: PDFPage = catalog.load('invoice_pdf')
assert type(pdf_page.table) is list
assert type(pdf_page.text) is str
```


#### ZipFileDataSet

A dataset that extracts a single file from a zip file and returns the bytes.
By default will return a byte array, but a dataset can be passed in to change unzip behavior.

##### Example Usage

Check out the video: [Handling Zip Files in Kedro Using the de1 python package!](https://youtu.be/sIQzL6Ca_io)

```yaml
invoice_zip:
    type: de1.zip.ZipFileDataSet
    filepath: data/01_raw/invoice.zip
    zipped_filename: invoice.pdf
    dataset: de1.pdf.PDFDataSet
```

```python
from de1.pdf import PDFPage

pdf_page: PDFPage = catalog.load('invoice_zip')
assert type(pdf_page.table) is list
assert type(pdf_page.text) is str
```
