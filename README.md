# Process Mining SAP extractor (Python)

This project implements some connectors for the mainstream processes in SAP.

### Install

The project is available as a Python package in Pypi.
It can be installed under a Python >=3.6 using:

**pip install -U sapextractor**

### Included resources

The root folder of the project includes a SQLIte dump of
a SAP IDES instance, which can be used to test the extraction.

### Basic usage (command line interface)

The project can be easily used from the command line:

**import sapextractor**

**sapextractor.cli()**

The command line interface asks to insert the connection parameters to the database,
and the details about the extraction.

### Supported processes

We support different processes, and modalities of extraction:

##### Order to Cash

For the Order-to-Cash process, we can extract a dataframe (that can be stored in CSV/Parquet files),
a XES log, and object-centric logs (in the MDL and JMD formats).

The two classic log modalities asks for a central document type (for each document of the type, a case is created
with all the operations on the connected documents).

##### Accounting

For the processes related to the accounting (such as the Accounts-Payable and the Accounts-Receivable processes),
we offer different extraction possibilities:
* Dataframe (Parquet/CSV) containing a case per document. Each case contains all the transactions that are executed on the document.
* XES log containing a case per document. Each case contains all the transactions that are executed on the document.
* **Document Flow**: given a central document type, provide as many cases as many documents of such type. Each case contains, as events,
the connected documents to the 'central' document of the case. It is possible to extract both a dataframe (Parquet/CSV) and a XES log.
* **Transactions for the documents in a Document Flow**: given a central document type, provide as many cases as many documents
of such type. Each case contains, as events, the transactions executed on the connected documents to the 'central' document of the case.
It is possible to extract both a dataframe (Parquet/CSV) and a XES log.


##### Procurement

For the procurement, we can extract a dataframe (that can be stored in CSV/Parquet files),
a XES log, and object-centric logs (in the MDL and JMD formats).

The two classic log modalities asks for a central document type (for each document of the type, a case is created
with all the operations on the connected documents).
