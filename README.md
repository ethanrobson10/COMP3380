# COMP 3380 Project - NHL Database

## Running Program on Aviary
1. Navigate to the interface directory:
  ```bash
    cd interface
  ``` 

2. Run the program:
  ```bash
    make run
  ``` 

## Populating Database
- The database is already pre-populated using our 'REPOP' interface command.
- Deleting and repopulating the database is possible through the main interface using 'DELETE' and 'REPOP'.
- Our insertions are broken into 36 SQL file 'chunks' of 50,000 lines each (in the sql_chunks directory) for insertion efficiency.

## Additional Info
- The userid and password are stored in the data directory in the auth.cfg file.


### Folder Reference
```bash
Group54_Submission
│
├───data (contains .csv files and auth.cfg)
│
├───interface (files to run the interface)
│  
└───populate_data (files to create SQL inserts from .csv data)
    │
    └───sql_chunks (contains each SQL chunk)
```
*Note: the data folder is not present in the GitHub repository due to the large files it contains*
