# Movies-ETL

## CHALLENGE

For this challenge we perform an ETL on three files of movie data. 
We refactor the code from Module 8's classwork, the Jupyter Notebook "Module 8 ETL Movies.ipynb" which was created to parse, transform and load movies data to a database.
The "Module 8 challenge.py" script works as a standalone script, allowing it to run from the commandline independently of a Jupyter server.

## OBJECTIVES
1. Create a function that takes in three arguments:
- Wikipedia data
- Kaggle metadata
- MovieLens rating data (from Kaggle)
2. Use the code from your Jupyter Notebook so that the function performs all of the transformation steps
- See the original Notebook in the repository
- Remove any exploratory data analysis and redundant code
3. Add the load steps from the Jupyter Notebook to the function
- Remove the existing data from SQL, but keep the empty tables
4. Check that the function works correctly on the current Wikipedia and Kaggle data
5. Document any assumptions that are being made. 
- Use try-except blocks to account for unforeseen problems that may arise with new data


## MAIN ASSUMPTIONS
1. The source data files will stay in the formats used by the original notebook code
- If for whatever reason the format of any of the files change, the script may fail to execute in an unexpected way, e.g. if expected columns are missing
- If new columns are introduced the script will ignore them, and the related data will be not be loaded into the database
- If any data is introduced that interferes with datatype conversion, that may also cause the script to fail. E.g. a column with an expected numeric data type has non-numeric data introduced that would cause a type conversion to fail
- New "bad" data is always a possibility, which may or may not interfere with with script execution, and if it passes to the database, may or may not interfere with later analysis
2. New data "patterns" introduced in the data will not be detected by the script
- If any significant new "patterns" of data are introduced, e.g. some new variation of a date format, then the existing parsing logic may fail to capture it. 
- Depending on the volumes concerned this may result in expected new data being unavailable for the final analysis.
- Adapting to these changes will require revisiting the exporatory data analysis step for the new data, and crafting more code to adapt to the new patterns if the exploration deems it worth the effort.
3. The script will be executed on three files of updated data, available in a prefdefined location
- Wikipedia data in JSON format and Kaggle metadata and rating data in CSV formats
- If any of the files are not found the script will report the error and abort execution
4. The postgres database server will be available at the predefined location
- If for whatever reason the script is unable to connect to the database server then the script will report an error and abort execution
- If the script is unable to write data it will also report and abort
5. The movies & rating tables will be available and empty before the script begins to start loading the tables 
- The script will attempt to delete data from the movies & ratings tables before attemptiong to load the updated data
    - if either of the tables do not exist then the script will warn that the tables do not exist before proceeding with trying to load the data
    - the tables will be automatically recreated if they do not exist when the script attempts to load the data

