# online_chessql Documentation

This is a program designed to create and analyze a PostgreSQL database of online chess game data made available by Ulrik Thyge Pedersen on Kaggle. You can find his dataset on Kaggle [here](https://www.kaggle.com/datasets/ulrikthygepedersen/online-chess-games). This dataset provides rating, openings, and game outcome data for over 20,000 online chess games via CSV.

The program consists of the following files:

- main.py: Controls the database functions and handles user error inputs.
- db.py: Contains functions to interact with the PostgreSQL database, including creating tables, dropping tables, loading data from a CSV file, adding columns, inserting data, and more.
- auth.json: A simple JSON file that contains user-input psycopg connection string parameters.
- chess_games.csv: A CSV file containing the dataset.

## File Structure

```
- main.py
- db.py
- auth.json
- chess_games.csv
```

## Dependencies

The following dependencies are required to run the program:

- psycopg
- sqlalchemy
- numpy
- pandas

Make sure to install these dependencies before running the program.

## Usage

To use the program, follow these steps:

1. Create and connect to a PostgreSQL database locally.
2. Enter your default user authentication details in the `auth.json` file.
3. Run the `main.py` script on an up-to-date version of Python.
4. The database tables will be created if they don't already exist.
5. The chess game data from the `chess_games.csv` file will be loaded into the `games` table.
6. Additional columns (for average rating, rating difference, and 'upset' Y/N) will be added to the `games` table.
7. The stats (`stats`) and openings (`openings`) tables will populate with summarizing data for each 100-rating range, beginning with 800-899.99.
8. Once the program completes, the database will be ready for analysis and querying.

Please ensure that you have the necessary permissions to create tables, insert data, and execute SQL queries on the PostgreSQL database.

## Code Overview

'main.py' contains all execution flow of the program. Below is an overview of the important functions in the `db.py` module:

- `create_conn_str(auth_json_path: str) -> str`: Reads the authentication details from the `auth.json` file and returns the connection string for psycopg.
- `create_games_table(conn_str: str) -> None`: Creates the `games` table in the database if it doesn't already exist.
- `create_stats_table(conn_str: str) -> None`: Creates the `stats` table in the database if it doesn't already exist.
- `create_openings_table(conn_str: str) -> None`: Creates the `openings` table in the database if it doesn't already exist.
- `drop_tables(conn_str: str) -> None`: Drops the `games`, `stats`, and `openings` tables from the database.
- `load_rated_games_df() -> pd.DataFrame`: Loads the rated chess game data from the `chess_games.csv` file into a Pandas DataFrame.
- `add_cols(rated_df: pd.DataFrame) -> pd.DataFrame`: Adds additional columns to the DataFrame containing calculated values.
- `df_to_db(rated_df: pd.DataFrame, engine: Engine) -> None`: Transfers the DataFrame data to the `games` table in the database using SQLAlchemy.
- `insert_stats(conn_str: str) -> None`: Populates the `stats` table with statistics based on game ratings.
- `insert_opens(conn_str: str) -> None`: Populates the `openings` table with data related to specific chess openings.