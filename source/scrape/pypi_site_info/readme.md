### Overview
This folder contains code to collect webpage information for PyPi libraries

### Source
Chris Liao 

### When/where obtained & original form of files
The original files are downloaded via `curl` from the PyPi website in `json` form. They were downloaded on either October 27, 2023 or September 29, 2024. 

### Description
- `code/get_library_json.sh` downloads the `json` files
- `code/link_pypi_github.py` obtains github homepages from the `json` files. 
- `docs/get_library_json.log` contains the log from the September 29, 2024 download run
- `docs/pypi_tou.pdf` contains the TOU for using PyPi services. 
- `drive/output/scrape/pypi_site_info/*.json` contains the downloaded `.json` pages for each website

### Terms of Use
The terms of use from PyPi says that I am allowed to use the package specific information and I am just grabbing GitHub homepages for each python package. 

### Run Order
1. `source/scrape/pypi_site_info/code/get_library_json.sh`
2. `source/scrape/pypi_site_info/code/link_pypi_github.py`
