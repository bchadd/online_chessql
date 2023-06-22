# online_chessql Documentation

This is a program designed to create and analyze a PostgreSQL database of online chess game data made available by Ulrik Thyge Pedersen on Kaggle. You can find his dataset on Kaggle [here](https://www.kaggle.com/datasets/ulrikthygepedersen/online-chess-games). This dataset provides rating, openings, and game outcome data for over 20,000 online chess games via CSV.

The program consists of the following files:

- main.py: Contains the main execution function and handles user error inputs.
- db.py: Contains functions to interact with the PostgreSQL database, including creating tables, dropping tables, loading data from a CSV file, adding columns, inserting data, etc.
- auth.json: A simple JSON file that contains user-input psycopg connection string parameters.
- chess_games.csv: The CSV file containing the dataset.

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

## Usage

Basic usage and notes on the program can be found below:

1. **Create and connect to a PostgreSQL database locally.**
2. **Enter your default user authentication details in the `auth.json` file.** Ensure that the user has the necessary permissions to execute create table, insert into table, and create view commands within the given PostgreSQL database.
3. **Run the `main.py` script on an up-to-date version of Python.**
4. The database tables will be created if they don't already exist.
5. The raw chess game data from the `chess_games.csv` file will be loaded into the `games` table.
6. Additional columns (for average rating, rating difference, and 'upset' bool) will be added to the `games` table.
7. The stats (`stats`) and openings (`openings`) tables will populate with summarizing data for each 100-rating range, from 800 to 2400.
8. Lastly, the program will create a 'summary' view to condense all of the winrate statistics into one view and then exports the view as a csv named 'rated_summary.csv' into the project directory. This view places these winrate statistics next to the number of games in the rating range specified, so that conclusions on the data are not made without considering the sample size of the range (admittedly very small for some of these ranges).
8. After the program runs, you can also use your preference of a database GUI tool (PGAdmin), the command line, and/or a session of Python in a compatible environment to continue viewing and analyzing the data however you choose.

## Code Overview

'main.py' contains all execution flow of the program. Below is an overview of the important functions in the `db.py` module:

- `create_conn_str(auth_json_path: str) -> str`: Reads the authentication details from the `auth.json` file and returns the connection string for psycopg.
- `create_games_table(conn_str: str) -> None`: Creates the `games` table in the database if it doesn't already exist.
- `create_stats_table(conn_str: str) -> None`: Creates the `stats` table in the database if it doesn't already exist.
- `create_openings_table(conn_str: str) -> None`: Creates the `openings` table in the database if it doesn't already exist.
- `drop_tables(conn_str: str) -> None`: Drops the `games`, `stats`, and `openings` tables from the database.
- `load_rated_games_df() -> pd.DataFrame`: Loads the rated chess game data from the `chess_games.csv` file into a Pandas DataFrame.
- `add_cols(rated_df: pd.DataFrame) -> pd.DataFrame`: Adds additional columns to the DataFrame containing calculated aggregated values (average rating 'avg_rating', rating disparity 'rating_diff', and upset 'upset').
- `df_to_db(rated_df: pd.DataFrame, engine: Engine) -> None`: Transfers the DataFrame data to the `games` table in the database using SQLAlchemy.
- `insert_stats(conn_str: str) -> None`: Populates the `stats` table with statistics based on game ratings.
- `insert_opens(conn_str: str) -> None`: Populates the `openings` table with data related to specific chess openings.
- `create_summary_view(conn_str: str) -> None`: Creates a view in the database named 'summary'. Note that **the view will always be overwritten if the function is only being used within the main() function**.
- `export_view_csv(conn_str: str) -> None`: Writes the 'summary' view to a CSV named 'rated_summary.csv' in the project directory. **This CSV will be overwritten without warning**, so the data in the created 'rated_summary.csv' will always refer only back to the state of the database after the most recent time the main() function ran.