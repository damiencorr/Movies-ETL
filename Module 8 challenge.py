# MODULE 8 CHALLENGE
# VS Code used to help develop this script
# The goals of this challenge are for you to:
# - Create an automated ETL pipeline
# - Extract data from multiple sources
# - Clean and transform the data automatically using Pandas and regular expressions
# - Load new data into PostgreSQL

# Instructions
# For this task, assume that the updated data will stay in the same formats
# - Wikipedia data in JSON format and Kaggle metadata and rating data in CSV formats
# 1. Create a function that takes in three arguments:
# - Wikipedia data
# - Kaggle metadata
# - MovieLens rating data (from Kaggle)
# 2. Use the code from your Jupyter Notebook so that the function performs all of the transformation steps
# - Remove any exploratory data analysis and redundant code
# 3. Add the load steps from the Jupyter Notebook to the function
# - You’ll need to remove the existing data from SQL, but keep the empty tables
# 4. Check that the function works correctly on the current Wikipedia and Kaggle data
# 5. Document any assumptions that are being made. 
# - Use try-except blocks to account for unforeseen problems that may arise with new data

# IMPORT DEPENDENCIES
import json
import logging
import numpy as np
import pandas as pd
import re
import sys
import time
from config import db_password
from pandas.io import sql
from sqlalchemy import create_engine

# 1. Create a function that takes in three arguments:
# - Wikipedia data
# - Kaggle metadata
# - MovieLens rating data (from Kaggle)
def movies_ETL(f1, f2, f3):

    # Load the JSON into a List of Dictionaries
    try:
        with open(f'{f1}', mode='r') as file:
            wiki_movies_raw = json.load(file)
    except FileNotFoundError as fnf:
        logging.info(f'File {f1} not found (error = {fnf}) at the expected location, aborting...')
        sys.exit(1)

    # Modify JSON data by
    # - restricting it to only those entries that have a director and an IMDb link
    # - removing TV shows to leave only movies 
    # Use List Comprehensions to filter the data
    wiki_movies = [movie for movie in wiki_movies_raw
                if ('Director' in movie or 'Directed by' in movie)
                    and 'imdb_link' in movie
                    and 'No. of episodes' not in movie]

    # Create a Function to Clean the wiki Data
    # There are quite a few columns with slightly different names but the same data, such as “Directed by” and “Director.”
    # Consolidate columns with the same data into one column, use pop() method to change the name of a dictionary key (pop() returns the value from the removed key-value pair)
    def clean_movie(movie):
        movie = dict(movie) #create a non-destructive copy
        alt_titles = {}
        # combine alternate titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune-Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        # merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)

        # List column names to be merged into a new column name.
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie

    logging.info (f'Parse wiki movies data...')
    # Run list comprehension to clean wiki_movies and create wiki_movies_df dataframe
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)
    #print(wiki_movies_df)

    # Remove Duplicate Rows
    # Use IMDb ID to merge with the Kaggle data, making sure there are no duplicate rows by using the IMDb ID
    # Extract the IMDb ID from the IMDb link using Regular Expressions (RE)
    # Use Pandas’ built-in string methods on a Series object accessed with the str property
    # Use str.extract() to take in a RE pattern
    # IMDb links generally look like “https://www.imdb.com/title/tt1234567/,” with “tt1234567” as the IMDb ID
    # The RE for a group of characters that start with “tt” and has seven digits is "(tt\d{7})"
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    #print(len(wiki_movies_df))

    # Drop any duplicates of IMDb IDs by using the drop_duplicates() method. 
    # To specify that we only want to consider the IMDb ID, use the subset argument, and set inplace equal to "True." 
    # We also want to see the new number of rows and how many rows were dropped.
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    #print(len(wiki_movies_df))
    #print(wiki_movies_df.head())

    # Remove Mostly Null Columns
    # Now that we’ve consolidated redundant columns, remove columns that don’t contain much useful data
    # Make a list of columns with less than 90% null values, i.e. the columns that we want to keep, select from our Pandas DataFrame
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    #print(wiki_movies_df)

    # Convert and Parse the Data

    # The Wikipedia data is now structured in tabular form but it needs to have the right data types in the SQL table
    # The following columns need to be converted
    # - Box office should be numeric (currency)
    # - Budget should be numeric (currency)
    # - Release date should be a date object
    # - Running time should be numeric

    # - Box office should be numeric (currency)
    box_office = wiki_movies_df['Box office'].dropna() 
    box_office
    # The box office and budget amounts aren’t written in a consistent style, we will need to parse their data correctly using RE
    # Use a simple space as the joining character and apply the join() function only when the data points are lists
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    #print(box_office)
    # After initial inspection parsing the Box Office Data there are two main forms the box office data is written in
    # - “$123.4 million” (or billion)
    # - “$123,456,789.” 
    # - Build a regular expression for each form, and then see what forms are left over.
    # First form - Pattern match string will include six elements in the following order:
    # - A dollar sign
    # - An arbitrary (but non-zero) number of digits
    # - An optional decimal point
    # - An arbitrary (but possibly zero) number of more digits
    # - A space (maybe more than one)
    # - The word “million” or “billion”
    form_one = r'\$\d+\.?\d*\s*[mb]illion'
    # Uppercase letters? - use contains() method, specify an option to ignore case
    box_office.str.contains(form_one, flags=re.IGNORECASE).sum()
    # FINDING: There are 3,896 box office values that match the form “$123.4 million/billion.”
    # Create second form - Pattern match string will include the following elements:
    # - A dollar sign
    # - A group of one to three digits
    # - At least one group starting with a comma and followed by exactly three digits
    form_two = r'\$\d{1,3}(?:,\d{3})+'
    box_office.str.contains(form_two, flags=re.IGNORECASE).sum()
    # Fix Pattern Matches
    # Capture more values by addressing these issues:
    # - values with spaces in between the dollar sign and the number.
    # - values use a period as a thousands separator, not a comma.
    # - values are given as a range.
    # - “Million” is sometimes misspelled as “millon.”
    # The rest of the box office values make up such a small percentage of the dataset and would require too much time and effort to parse correctly, so we’ll just ignore them
    # 1. Some values have spaces in between the dollar sign and the number.
    # - Just add \s* after the dollar signs in both forms
    # 2. Some values use a period as a thousands separator, not a comma.
    # - change form_two to allow for either a comma or period as a thousands separator
    # - The results will also match values like 1.234 billion, 
    # - but we’re trying to capture & change raw numbers like $123.456.789. 
    # - We don’t want to capture any values like 1.234 billion, so we need to add a negative lookahead group that looks ahead for 
    # - “million” or “billion” after the number and rejects the match if it finds those strings. 
    # 3. Some values are given as a range.
    # - search for any string that starts with a dollar sign and ends with a hyphen, 
    # - then replace it with just a dollar sign using the replace() method. 
    # - The first argument in the replace() method is the substring that will be replaced, and the second argument in the replace() method is the string to replace it with. 
    # - Use regular expressions in the first argument by sending the parameter regex=True
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    # 4. “Million” is sometimes misspelled as “millon.”
    # - make the second “i” optional in our match string with a question mark
    # FINAL FORMS TAKING ACCOUNT OF THE ABOVE
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'

    # Extract and Convert the Box Office Values
    # Use the form expressions to match almost all the box office values and to extract only the parts of the strings that match using the str.extract() method.
    # This method takes in a regular expression string, and returns a DataFrame where every column is the data that matches a capture group. 
    # We create a regular expression that captures data when it matches either form_one or form_two by using an f-string.
    # f-string f'{form_one}|{form_two}' creates a regular expression that matches either form_one or form_two
    # Put the whole thing in parentheses to create a capture group.
    box_office.str.extract(f'({form_one}|{form_two})')

    # We create a function to turn the extracted values into a numeric value, called parse_dollars
    # parse_dollars will take in a string and return a floating-point number
    def parse_dollars(s):
        
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # Use re.match(pattern, string) to see if our string matches a pattern
        # Split the million and billion matches from form one
        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " million"
            # use re.sub(pattern, replacement_string, string) to remove dollar signs, spaces, commas, and letters, if necessary
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a million
            value = float(s) * 10**6
            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a billion
            value = float(s) * 10**9
            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
            # remove dollar sign and commas
            s = re.sub('\$|,','', s)
            # convert to float
            value = float(s)
            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan


    # Parse BOX OFFICE data
    # Apply parse_dollars to the first column in the DataFrame returned by str.extract
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    # We no longer need the Box Office column, so we’ll just drop it:
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    #print(wiki_movies_df)

    # Parse BUDGET Data
    # Use the same pattern matches as for Box Office and see how many budget values are in a different form.
    # Preprocess the budget data, creating a budget variable
    budget = wiki_movies_df['Budget'].dropna()

    # Convert any lists to strings
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)

    # Remove any values between a dollar sign and a hyphen (for budgets given in ranges)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    # Noting a new issue with the budget data: citation references (the numbers in square brackets)
    # Remove the citation references with the following
    budget = budget.str.replace(r'\[\d+\]\s*', '')

    # Parse the budget values. Reuse the code to parse the box office values, changing “box_office” to “budget”
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    # Drop the original Budget column.
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    #print(wiki_movies_df)

    # Parse RELEASE DATE

    # Parsing the release date will follow a similar pattern to parsing box office and budget, but with different forms.
    # Make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    # The forms for parsing are
    # - Full month name, one- to two-digit day, four-digit year (i.e., January 1, 2000)
    # - Four-digit year, two-digit month, two-digit day, with any separator (i.e., 2000-01-01)
    # - Full month name, four-digit year (i.e., January 2000)
    # - Four-digit year
    # - Full month name, one- to two-digit day, four-digit year (i.e., January 1, 2000)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    # - Four-digit year, two-digit month, two-digit day, with any separator (i.e., 2000-01-01)
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    # - Full month name, four-digit year (i.e., January 2000)
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    # - Four-digit year
    date_form_four = r'\d{4}'

    # Extract the dates with
    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)
    # Use the built-in to_datetime() method in Pandas
    # Since there are different date formats, set the infer_datetime_format option to True.
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)[0], infer_datetime_format=True)
    #print(wiki_movies_df)

    # Parse RUNNING TIME

    # First, make a variable that holds the non-null values of RUNNING TIME in the DataFrame, converting lists to strings
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    # We can match all of the hour + minute patterns with one regular expression pattern. Our pattern follows:
    # - Start with one digit.
    # - Have an optional space after the digit and before the letter “h.”
    # - Capture all the possible abbreviations of “hour(s).” To do this, we’ll make every letter in “hours” optional except the “h.”
    # - Have an optional space after the “hours” marker.
    # - Have an optional number of digits for minutes.
    # - As a pattern, this looks like "\d+\s*ho?u?r?s?\s*\d*".
    # Extract values
    # Only extract digits allowing for both possible patterns. 
    # Add capture groups around the \d instances as well as add an alternating character
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

    # The new DataFrame is all strings, we’ll convert to numeric values
    # Because we may have captured empty strings, use the to_numeric() method and set the errors argument to 'coerce'
    # Coercing the errors will turn the empty strings into Not a Number (NaN), then we can use fillna() to change all the NaNs to zeros.
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

    # Now we can apply a function that will convert the hour capture groups and minute capture groups to minutes if the pure minutes capture group is zero, and save the output to wiki_movies_df
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

    # Finally drop Running time from the dataset
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    #print(f'\n\n\n\n\n1 wiki_movies_df\n\n\n\n\n {wiki_movies_df}')

    logging.info(f'Parse KAGGLE movies data...')
    # Extract the Kaggle Data
    try:
        kaggle_metadata = pd.read_csv(f'{f2}')
    except FileNotFoundError as fnf:
        logging.info(f'File {f2} not found at the expected location, aborting...')
        sys.exit(1)

    # Clean the KAGGLE Data - Movie metadata
    # We don’t want to include adult movies in the hackathon dataset, we’ll only keep rows where adult is False, 
    # and then drop the “adult” column.    
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    # In the video column there are only False and True values, convert video easily
    # Create the Boolean column, assign it back to video
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    # For the numeric columns, use the to_numeric() method from Pandas
    # Set errors= argument to 'raise', so we’ll know if there’s any data that can’t be converted to numbers
    try:
        # If this code runs without errors, everything converted fine!
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    except Exception as e:
        # There was a problem with a datatype conversion, need to abort and be investigated 
        logging.info(f"Aborting due to the following, please investigate.......\nException type - {type(e)}, \nException name - {type(e).__name__}, \nException description - {e}")
        sys.exit(1)

    # Convert release_date to datetime using Pandas built-in function to_datetime()
    # Since release_date is in a standard format, to_datetime() will convert it without error
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    #print(f'\n\n\n\n\n2 kaggle_metadata\n\n\n\n\n {kaggle_metadata}')

    # Reasonability Checks on Ratings Data
    # For this analysis, we won’t be using the timestamp column
    # However, we will be storing the rating data as its own table in SQL, so we’ll need to convert it to a datetime data type. 
    # From the MovieLens documentation, the timestamp is the number of seconds since midnight of January 1, 1970.
    # We’ll specify in to_datetime() that the origin is 'unix' and the time unit is seconds, assign it to the timestamp column.
    try:
        ratings = pd.read_csv(f'{f3}')
    except FileNotFoundError as fnf:
        logging.info(f'File {f3} not found at the expected location, aborting.....')
        sys.exit(1)
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    # NOTE: THIS DATA IS NOT WRITTEN BACK TO DISK, HENCE THE LATER REREAD OF AND LOAD OF RATINGS TO THE DB DOES NOT INCLUDE THIS DATA FORMATTING!!

    logging.info (f'Merge WIKIPEDIA and KAGGLE movies data...')
    # Merge Wikipedia and Kaggle Metadata
    # Now that the Wikipedia data and Kaggle data are cleaned up and in tabular formats with the right data types for each column we can join them together
    # However, after they’re joined, the data still needs to be cleaned up a bit, especially where Kaggle and Wikipedia data overlap
    # Merge them by IMDb ID. then after we’ve merged data look out for redundant columns
    # Print out a list of the columns so we can identify which ones are redundant
    # Use the suffixes parameter to make it easier to identify which table each column came from
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    movies_df.columns

    # Options when dealing with redundant data
    # - Drop one of the competing columns - sometimes that means a loss of good information
    # - Fill in the gaps using both columns - Sometimes, one column will have data where the other has missing data, and vice versa
    # Below is the list of competing columns with resolution
    # Competing data:
    # Wiki                     Movielens                Resolution
    #--------------------------------------------------------------------------
    # title_wiki               title_kaggle				Drop wikipedia   
    # running_time             runtime					Keep Kaggle; fill in zeros with Wikipedia data.
    # budget_wiki              budget_kaggle			Keep Kaggle; fill in zeros with Wikipedia data.
    # box_office               revenue					Keep Kaggle; fill in zeros with Wikipedia data.
    # release_date_wiki        release_date_kaggle		Drop Wikipedia
    # Language                 original_language		Drop Wikipedia
    # Production company(s)    production_companies		Drop Wikipedia

    # It looks like somehow The Holiday in the Wikipedia data got merged with From Here to Eternity
    # Drop that row from our DataFrame, get the index of that row
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
    # Convert the lists in Language to tuples so that the value_counts() method will work
    movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)
    # Put It All Together
    # Drop the title_wiki, release_date_wiki, Language, and Production company(s) columns.
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

    # Next, make a function that fills in missing data for a column pair and then drops the redundant column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)
        
    #Now we can run the function for the three column pairs that we decided to fill in zeros.
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    movies_df

    # 'video' column is the only column that has only one value
    # Since it’s false for every row, we don’t need to include this column
    # Reorder the columns to make the dataset easier to read for the hackathon participants
    # Having similar columns near each other helps people looking through the data get a better sense of what information is available
    # One way to reorder them would be to consider the columns roughly in groups, like this
    # - Identifying information (IDs, titles, URLs, etc.)
    # - Quantitative facts (runtime, budget, revenue, etc.)
    # - Qualitative facts (genres, languages, country, etc.)
    # - Business data (production companies, distributors, etc.)
    # - People (producers, director, cast, writers, etc.)
    # Reorder the columns
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                        'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                        'genres','original_language','overview','spoken_languages','Country',
                        'production_companies','production_countries','Distributor',
                        'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                        ]]

    # Finally, rename the columns to be consistent.

    movies_df.rename({'id':'kaggle_id',
                    'title_kaggle':'title',
                    'url':'wikipedia_url',
                    'budget_kaggle':'budget',
                    'release_date_kaggle':'release_date',
                    'Country':'country',
                    'Distributor':'distributor',
                    'Producer(s)':'producers',
                    'Director':'director',
                    'Starring':'starring',
                    'Cinematography':'cinematography',
                    'Editor(s)':'editors',
                    'Writer(s)':'writers',
                    'Composer(s)':'composers',
                    'Based on':'based_on'
                    }, axis='columns', inplace=True)
    # NOTE: If you did not use .loc to reorder the columns and instead passed a list of column names to the indexing operator 
    # (i.e. movies_df = movies_df[[‘imdb_id’, ‘title_kaggle’, … ]]), you may receive a SettingWithCopyWarning. 
    # This isn’t an error, so your code will continue to work, but it is a warning that your code may not behave as you expect. 
    # In this case, your code will work fine, but for best practices, use .loc instead to avoid this warning.

    # Transform and Merge Rating Data
    # The rating data is a very large dataset
    # Reduce the ratings data to a useful summary of rating information for each movie, 
    # and then make the full dataset available to the hackathon participants if they decide they need more granular rating information

    # Use a groupby on the “movieId” and “rating” columns and take the count for each group.
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()

    # Rename the “userId” column to “count.”
    # NOTE - The choice of renaming “userId” to “count” is arbitrary
    # - Both “userId” and “timestamp” have the same information, so we could use either one
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count().rename({'userId':'count'}, axis=1) 

    # Pivot this data so that movieId is the index
    # Columns will be all the rating values,
    # Rows will be the counts for each rating value
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                 .rename({'userId':'count'}, axis=1)                 .pivot(index='movieId',columns='rating', values='count')

    # Rename the columns so they’re easier to understand, prepend rating_ to each column with a list comprehension
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    rating_counts

    # Merge the rating counts into movies_df.
    # Use a left merge to keep everything in movies_df
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

    # Finally, because not every movie got a rating for each rating level, there will be missing values instead of zeros, fill those in
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    #print(f'\n\n\n\n\nmovies_with_ratings_df\n\n\n\n\n {movies_with_ratings_df}')

    # Done - finished the Transform step in ETL!


    logging.info (f'Dump merged data to SQL DB...')
    # Connect Pandas and SQL
    # Create a config.py file containing plaintext DB passwword, update .gitignore to ignore the file 
    # Create the Database Engine
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"

    # SQLAlchemy creates a database engine, handles connections to the SQL database, manages the conversion between data types
    engine = create_engine(db_string)

    # Import the Movie Data
    # To save the movies_df DataFrame to a SQL table specify the name of the table and the engine
    start_time = time.time()
    # Assuming the table already exists, then clear existing contents before loading
    logging.info(f'Update Movies - try to delete current contents before loading.')
    try:
        sql.execute('DELETE FROM movies',engine)
    except Exception as e:
        logging.info (f"Exception type - {type(e)}, \nException name - {type(e).__name__}, \nException description - {e}")
    try:
        movies_with_ratings_df.to_sql(name='movies', con=engine, if_exists='append')
    except Exception as e:
        logging.info (f"There was a problem writting movies data to the database, please investigate the following, aborting....\nException type - {type(e)}, \nException name - {type(e).__name__}, \nException description - {e}")
        sys.exit(1)
    logging.info(f'Update Movies - Done. {int(time.time() - start_time):,} total seconds elapsed')

    # Import the Ratings Data
    # The ratings data has to be divided into “chunks” of data as it is too large to be imported in one statement
    # Reimport the CSV using the chunksize= parameter in read_csv()
    # This creates an iterable object, so we can make a for loop and append the chunks of data to the new rows to the target SQL table
    # CAUTION: to_sql() method also has a chunksize= parameter, but that won’t help us with memory concerns
    # chunksize= parameter in to_sql() creates smaller transactions sent to SQL to prevent the SQL instance from getting locked up with a large transaction
    # Print out some information about how it’s running
    # - How many rows have been imported
    # - How much time has elapsed
    rows_imported = 0
    # Get the start_time from time.time(), initializing with the current time.
    start_time = time.time()
    # Assuming the table already exists, then clear existing contents before loading
    # Assuming the table already exists, then clear existing contents before loading
    logging.info(f'Update Ratings - try to delete current contents before loading')
    try:
        sql.execute('DELETE FROM ratings',engine)
    except Exception as e:
        logging.info(f"Exception type - {type(e)}, \nException name - {type(e).__name__}, \nException description - {e}")
    logging.info(f'Update Ratings - delete done. {int(time.time() - start_time):,} total seconds elapsed')

    for data in pd.read_csv(f'{f3}', chunksize=1000000):
        logging.info(f'Update Ratings - importing rows {rows_imported} to {rows_imported + len(data)}...')
        data.to_sql(name='ratings', con=engine, if_exists='append')
        rows_imported += len(data)

    # add elapsed time to final print out
    # Total elapsed time is simply time.time() - start_time
    logging.info(f'Update Ratings - Done. {int(time.time() - start_time):,} total seconds elapsed')


#
#
# CODE EXECUTION STARTS HERE
#
#

# Define logging level and timestamp format
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

# Define the WINDOWS PATH to the data files
file_dir = 'C:/Users/damie/Documents/movies data/'

# Define the expected files
# - Wikipedia data
wikipedia_data_json = f'{file_dir}wikipedia.movies.json'
# - Kaggle metadata
kaggle_metadata_raw = f'{file_dir}movies_metadata.csv'
# - MovieLens rating data (from Kaggle)
ratings_raw = f'{file_dir}ratings.csv'

# Fetch, transform & load the movies data from the three files
movies_ETL(wikipedia_data_json, kaggle_metadata_raw, ratings_raw)

