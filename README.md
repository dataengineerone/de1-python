# de1
Curated collection of DE1's favorite kedro utilities.


## EmptyPartitionedDataSet

For those times when data is not yet available in a particular folder, or if no data is a valid value.

Particularly useful when doing sub-node parallelization.

```
empty_json_collection:
    type: de1.empty.EmptyPartitionedDataSet
    path: data/02_intermediate/json_collection
    dataset: json.JSONDataSet
```


## LazyPartitionedDataSet

For when the data is too big to calculate all at once, and requires at least some clean-up in the process.

```
lazy_json_collection:
    type: de1.lazy.LazyPartitionedDataSet
    path: data/02_intermediate/json_collection
    dataset: json.JSONDataSet
```


## PDFDataSet

A dataset that uses `pdfplumber` to extract text and tables from pdf files.

Data gets returned as a `PDFPage` object.

```
invoice_pdf:
    type: de1.pdf.PDFDataSet
    filepath: data/01_raw/invoice.pdf
```


## ZipFileDataSet

A dataset that extracts a single file from a zip file and returns the bytes.
By default will return a byte array, but a dataset can be passed in to change unzip behavior.

```
invoice_pdf:
    type: de1.zip.ZipFileDataSet
    filepath: data/01_raw/invoice.zip
    filename: invoice.pdf
```

