# Project Title

Spark and HBase based HApache Access Log Analyzer

## Project Description

Apache log analyzer will analyze the Apache access log files, and generates the ordered list of urls-method-count pair based with following specifications: <br />
* List the duplicate visits from apache access log
* Seperate them according to HTTP Methods
* Count the duplicates for each URL-Method Pair
* Sort them according to URL and then with HTTP Methods
* Output format: (REQUEST_URL, REQUEST_METHOD, COUNT)

#### Output Example:
((/mailman,GET),6)<br />
((/mailman/admin,GET),2)<br />
((/mailman/admin/ppwc,GET),6)<br />
((/mailman/admin/ppwc,POST),6)<br />

## Input
Place Apache Access Log into input/input

## Working Procedure

### Spark
* Seperate URL and HTTP Method from each line of input
* Create Record<URL:String, Method:String> for each pair
* Count repetitions for each pair
* Filter out non-duplicate pairs
* Sort according to Record object
* Partition on 5 reducers with HashPartiton of Record object
* Save as a text file
* Prepare output data into format supported by HBase
* Configure HBase
* Save into HBase

### HBase
* A simple CRUD for HBase data
* TODO: Create CRUD for Apache Spark output data

## Getting Started

Change current directory to project source directory and run `./run.sh`.

### Prerequisites

Prerequisites

* Install platform for Cloudera. This may be VMWare, Docker or VirtualBox.
* Install Cloudera CDH 5.8 into the platform (Download Link : https://www.cloudera.com/downloads/quickstart_vms/5-8.html )

## Author

* **Bishal Paudel** - [BishalPaudel](https://github.com/bishalpaudel)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details