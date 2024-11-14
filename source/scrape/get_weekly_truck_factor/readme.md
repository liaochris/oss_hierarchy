### Overview
This folder contains code to get [TruckFactor](https://github.com/HelgeCPH/truckfactor) data on GitHub projects in my sample

### Source
The project data was obtained by cloning GitHub projects. The data on a project's Truck Factor was obtained from [TruckFactor](https://github.com/HelgeCPH/truckfactor) using a local build of the software. 

### When/where obtained & original form of files
Chris Liao cloned projects and then ran [TruckFactor](https://github.com/HelgeCPH/truckfactor) on weekly changes detected in the git log to obtain time series data on risky practices by OSS projects. 

### Description
- `docs/get_truck_factor.py` clones GitHub projects and then runs [TruckFactor](https://github.com/HelgeCPH/truckfactor) (in parallel simultaneously on several projects) to retrieve weekly information on risky practices
- `docs/truckfactor_license.txt`

### Terms of Use
[TruckFactor](https://github.com/HelgeCPH/truckfactor) uses the GNU GPL v3.0 License from June 29, 2007 which says in Section 2. Basic Permissions that

>  You may make, run and propagate covered works that you do not
convey, without conditions so long as your license otherwise remains
in force.  