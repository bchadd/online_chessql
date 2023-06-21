import db
from psycopg.errors import OperationalError, UniqueViolation

def main() -> None:
    conn_str = db.create_conn_str('auth.json')
    engine = db.create_engine(conn_str)

    # create tables
    try:
        db.create_games_table(conn_str)
        db.create_stats_table(conn_str)
        db.create_openings_table(conn_str)
    except OperationalError:
        print('''Connection was not established. Verify your\ninformation in auth.json and retry.''')
        return
    
    # load csv as dataframe
    df = db.load_rated_games_df()

    # append avg_rating, rating_diff, and upset columns to dataframe
    complete_df = db.add_cols(df)
    
    # use sqlalchemy Engine as Connectable for DataFrame.to_sql()
    db.df_to_db(complete_df,engine)

    # insert summary rows to stats and openings tables
    try:
        db.insert_stats(conn_str)
        db.insert_opens(conn_str)
    except UniqueViolation:
        print('''Row(s) not inserted; one or more of the provided\nprimary keys already exists in the stats and/or the\nopenings table. Drop tables and run main.py again.''')

if __name__ == '__main__':
    main()