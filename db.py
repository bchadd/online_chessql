import json
import psycopg
from sqlalchemy import create_engine, Engine
import numpy as np
import pandas as pd

def create_conn_str(auth_json_path: str) -> str:
    with open(auth_json_path,'r') as file:
        json_file = json.load(file)
        user = json_file['user']
        password = json_file['password']
        port = json_file['port']
        dbname = json_file['dbname']
        return f'postgresql://{user}:{password}@localhost:{port}/{dbname}'

def create_games_table(conn_str: str) -> None:
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            sql = '''CREATE TABLE IF NOT EXISTS games (
                game_id INT PRIMARY KEY,
                rated BOOLEAN,
                turns INT,
                victory_status VARCHAR,
                winner VARCHAR,
                time_increment VARCHAR,
                white_id VARCHAR,
                white_rating INT,
                black_id VARCHAR,
                black_rating INT,
                moves VARCHAR,
                opening_code VARCHAR,
                opening_moves VARCHAR,
                opening_fullname VARCHAR,
                opening_shortname VARCHAR,
                opening_response VARCHAR,
                opening_variation VARCHAR,
                avg_rating INT,
                rating_diff INT,
                upset BOOLEAN);'''
            cur.execute(sql)

def create_stats_table(conn_str: str) -> None:
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            sql = '''CREATE TABLE IF NOT EXISTS stats (
                rating_range_min INTEGER PRIMARY KEY,
                label VARCHAR,
                games INT,
                white_wr FLOAT,
                black_wr FLOAT,
                avg_rating_disparity FLOAT,
                avg_turns FLOAT,
                upset_rate FLOAT);'''
            cur.execute(sql)

def create_openings_table(conn_str: str) -> None:
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            sql = '''CREATE TABLE IF NOT EXISTS openings (
                rating_range_min INTEGER PRIMARY KEY,
                label VARCHAR,
                white_wr_sd FLOAT,
                black_wr_sd FLOAT,
                white_wr_ck FLOAT,
                black_wr_ck FLOAT,
                white_wr_fd FLOAT,
                black_wr_fd FLOAT);'''
            cur.execute(sql)

def drop_tables(conn_str: str) -> None:
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            sql = 'DROP TABLE games, stats, openings;'
            cur.execute(sql)

def load_rated_games_df() -> pd.DataFrame:
    with open('chess_games.csv','r') as csv:
        df = pd.read_csv(csv,index_col='game_id')
        return df.loc[df['rated'] == True]

def add_cols(rated_df: pd.DataFrame) -> pd.DataFrame:
    rated_df['avg_rating'] = (rated_df['white_rating']+rated_df['black_rating'])/2
    rated_df['rating_diff'] = abs(rated_df['white_rating']-rated_df['black_rating'])

    conditions = [
        (rated_df['white_rating'] > rated_df['black_rating']) & (rated_df['winner'] == 'White'), ## incumbent
        (rated_df['black_rating'] > rated_df['white_rating']) & (rated_df['winner'] == 'Black'), ## incumbent
        (rated_df['winner'] == 'Draw'), ## draw
        (rated_df['white_rating'] > rated_df['black_rating']) & (rated_df['winner'] == 'Black'), ## upset
        (rated_df['black_rating'] > rated_df['white_rating']) & (rated_df['winner'] == 'White')] ## upset
    values = [False, False, False, True, True]

    rated_df['upset'] = np.select(conditions, values)

    return rated_df

def df_to_db(rated_df: pd.DataFrame,engine: Engine) -> None:
    with engine.connect() as conn, conn.begin():
        rated_df.to_sql(name='games',con=conn,index='game_id',if_exists='replace')

def insert_stats(conn_str: str) -> None:
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            ranges = [
                (800,899.99,'800-899'),
                (900,999.99,'900-999'),
                (1000,1099.99,'1000-1099'),
                (1100,1199.99,'1100-1199'),
                (1200,1299.99,'1200-1299'),
                (1300,1399.99,'1300-1399'),
                (1400,1499.99,'1400-1499'),
                (1500,1599.99,'1500-1599'),
                (1600,1699.99,'1600-1699'),
                (1700,1799.99,'1700-1799'),
                (1800,1899.99,'1800-1899'),
                (1900,1999.99,'1900-1999'),
                (2000,2099.99,'2000-2099'),
                (2100,2199.99,'2100-2199'),
                (2200,2299.99,'2200-2299'),
                (2300,2399.99,'2300-2399'),
                (2400,2499.99,'2400-2499'),
                (0,2499.99,'total')]
            for min,max,label in ranges:
                sql = f'''
                    SELECT COUNT(*)
                    FROM games
                    WHERE games.avg_rating BETWEEN {min} AND {max};'''
                cur.execute(sql)
                games = cur.fetchone()[0]

                sql = f'''
                    SELECT
                        CASE
                            WHEN (SELECT COUNT(*) 
                                FROM games 
                                WHERE games.avg_rating BETWEEN {min} AND {max}) = 0
                            THEN NULL
                            ELSE ROUND((SELECT COUNT(*)
                                    FROM games
                                    WHERE games.winner = 'White'
                                    AND games.avg_rating BETWEEN {min} AND {max})::NUMERIC
                                / (SELECT COUNT(*)
                                    FROM games
                                    WHERE games.avg_rating BETWEEN {min} AND {max})::NUMERIC, 3)
                        END;'''
                cur.execute(sql)
                white_wr = cur.fetchone()[0]

                sql = f'''
                    SELECT 
                        CASE
                            WHEN (SELECT COUNT(*) 
                                FROM games 
                                WHERE games.avg_rating BETWEEN {min} AND {max}) = 0
                            THEN NULL
                            ELSE ROUND((SELECT COUNT(*)
                                    FROM games
                                    WHERE games.winner = 'Black'
                                    AND games.avg_rating BETWEEN {min} AND {max})::NUMERIC
                                / (SELECT COUNT(*)
                                    FROM games
                                    WHERE games.avg_rating BETWEEN {min} AND {max})::NUMERIC, 3)
                        END;'''
                cur.execute(sql)
                black_wr = cur.fetchone()[0]

                sql = f'''
                    SELECT ROUND(AVG(games.rating_diff),2)
                    FROM games
                    WHERE games.avg_rating BETWEEN {min} AND {max};'''
                cur.execute(sql)
                avg_rating_disparity = cur.fetchone()[0]

                sql = f'''
                    SELECT ROUND(AVG(games.turns),2)
                    FROM games
                    WHERE games.avg_rating BETWEEN {min} AND {max};'''
                cur.execute(sql)
                avg_turns = cur.fetchone()[0]

                sql = f'''
                    SELECT 
                        CASE
                            WHEN (SELECT COUNT(*) 
                                FROM games 
                                WHERE games.avg_rating BETWEEN {min} AND {max}) = 0
                            THEN NULL
                            ELSE ROUND((SELECT COUNT(*)
                                    FROM games
                                    WHERE games.avg_rating BETWEEN {min} AND {max}
                                    AND games.upset = 1)::NUMERIC
                                / (SELECT COUNT(*)
                                        FROM games
                                        WHERE games.avg_rating BETWEEN {min} AND {max})::NUMERIC, 3)
                        END;'''
                cur.execute(sql)
                upset_rate = cur.fetchone()[0]

                row = (min,label,games,white_wr,black_wr,avg_rating_disparity,avg_turns,upset_rate)
                sql = '''
                    INSERT INTO stats (
                        rating_range_min,
                        label,
                        games,
                        white_wr,
                        black_wr,
                        avg_rating_disparity,
                        avg_turns,
                        upset_rate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'''
                cur.execute(sql,row)

def insert_opens(conn_str: str) -> None:
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            ranges = [
                (800,899.99,'800-899'),
                (900,999.99,'900-999'),
                (1000,1099.99,'1000-1099'),
                (1100,1199.99,'1100-1199'),
                (1200,1299.99,'1200-1299'),
                (1300,1399.99,'1300-1399'),
                (1400,1499.99,'1400-1499'),
                (1500,1599.99,'1500-1599'),
                (1600,1699.99,'1600-1699'),
                (1700,1799.99,'1700-1799'),
                (1800,1899.99,'1800-1899'),
                (1900,1999.99,'1900-1999'),
                (2000,2099.99,'2000-2099'),
                (2100,2199.99,'2100-2199'),
                (2200,2299.99,'2200-2299'),
                (2300,2399.99,'2300-2399'),
                (2400,2499.99,'2400-2499'),
                (0,2499.99,'total')]
            for min,max,label in ranges:
                sql = f'''
                    SELECT 
                        CASE
                            WHEN (SELECT COUNT(*) 
                                FROM games 
                                WHERE games.avg_rating BETWEEN {min} AND {max}
                                AND games.opening_shortname = 'Sicilian Defense') = 0
                            THEN NULL
                            ELSE ROUND((SELECT COUNT(*) 
                                    FROM games 
                                    WHERE games.avg_rating BETWEEN {min} AND {max}
                                    AND games.opening_shortname = 'Sicilian Defense' 
                                    AND games.winner = 'White')::NUMERIC
                                / (SELECT COUNT(*) 
                                        FROM games 
                                        WHERE games.avg_rating BETWEEN {min} AND {max}
                                        AND games.opening_shortname = 'Sicilian Defense')::NUMERIC, 3)
                        END;'''
                cur.execute(sql)
                white_wr_sd = cur.fetchone()[0]

                sql = f'''
                    SELECT 
                        CASE
                            WHEN (SELECT COUNT(*) 
                                FROM games 
                                WHERE games.avg_rating BETWEEN {min} AND {max}
                                AND games.opening_shortname = 'Sicilian Defense') = 0
                            THEN NULL
                            ELSE ROUND((SELECT COUNT(*) 
                                    FROM games 
                                    WHERE games.avg_rating BETWEEN {min} AND {max}
                                    AND games.opening_shortname = 'Sicilian Defense' 
                                    AND games.winner = 'Black')::NUMERIC
                                / (SELECT COUNT(*) 
                                        FROM games 
                                        WHERE games.avg_rating BETWEEN {min} AND {max}
                                        AND games.opening_shortname = 'Sicilian Defense')::NUMERIC, 3)
                        END;'''
                cur.execute(sql)
                black_wr_sd = cur.fetchone()[0]

                sql = f'''
                    SELECT 
                        CASE
                            WHEN (SELECT COUNT(*) 
                                FROM games 
                                WHERE games.avg_rating BETWEEN {min} AND {max}
                                AND games.opening_shortname = 'Caro-Kann Defense') = 0
                            THEN NULL
                            ELSE ROUND((SELECT COUNT(*) 
                                    FROM games 
                                    WHERE games.avg_rating BETWEEN {min} AND {max}
                                    AND games.opening_shortname = 'Caro-Kann Defense'
                                    AND games.winner = 'White')::NUMERIC
                                / (SELECT COUNT(*) 
                                        FROM games 
                                        WHERE games.avg_rating BETWEEN {min} AND {max}
                                        AND games.opening_shortname = 'Caro-Kann Defense')::NUMERIC, 3)
                        END;'''
                cur.execute(sql)
                white_wr_ck = cur.fetchone()[0]

                sql = f'''
                    SELECT 
                        CASE
                            WHEN (SELECT COUNT(*) 
                                FROM games 
                                WHERE games.avg_rating BETWEEN {min} AND {max}
                                AND games.opening_shortname = 'Caro-Kann Defense') = 0
                            THEN NULL
                            ELSE ROUND((SELECT COUNT(*) 
                                    FROM games 
                                    WHERE games.avg_rating BETWEEN {min} AND {max}
                                    AND games.opening_shortname = 'Caro-Kann Defense'
                                    AND games.winner = 'Black')::NUMERIC
                                / (SELECT COUNT(*) 
                                        FROM games 
                                        WHERE games.avg_rating BETWEEN {min} AND {max}
                                        AND games.opening_shortname = 'Caro-Kann Defense')::NUMERIC, 3)
                        END;'''
                cur.execute(sql)
                black_wr_ck = cur.fetchone()[0]

                sql = f'''
                    SELECT 
                        CASE
                            WHEN (SELECT COUNT(*) 
                                FROM games 
                                WHERE games.avg_rating BETWEEN {min} AND {max}
                                AND games.opening_shortname = 'French Defense') = 0
                            THEN NULL
                            ELSE ROUND((SELECT COUNT(*) 
                                    FROM games 
                                    WHERE games.avg_rating BETWEEN {min} AND {max}
                                    AND games.opening_shortname = 'French Defense'
                                    AND games.winner = 'White')::NUMERIC
                                / (SELECT COUNT(*) 
                                        FROM games 
                                        WHERE games.avg_rating BETWEEN {min} AND {max}
                                        AND games.opening_shortname = 'French Defense')::NUMERIC, 3)
                        END;'''
                cur.execute(sql)
                white_wr_fd = cur.fetchone()[0]

                sql = f'''
                    SELECT 
                        CASE
                            WHEN (SELECT COUNT(*) 
                                FROM games 
                                WHERE games.avg_rating BETWEEN {min} AND {max}
                                AND games.opening_shortname = 'French Defense') = 0
                            THEN NULL
                            ELSE ROUND((SELECT COUNT(*) 
                                    FROM games 
                                    WHERE games.avg_rating BETWEEN {min} AND {max}
                                    AND games.opening_shortname = 'French Defense'
                                    AND games.winner = 'Black')::NUMERIC
                                / (SELECT COUNT(*) 
                                        FROM games 
                                        WHERE games.avg_rating BETWEEN {min} AND {max}
                                        AND games.opening_shortname = 'French Defense')::NUMERIC, 3)
                        END;'''
                cur.execute(sql)
                black_wr_fd = cur.fetchone()[0]

                row = (min,label,white_wr_sd,black_wr_sd,white_wr_ck,black_wr_ck,white_wr_fd,black_wr_fd)
                sql = '''
                    INSERT INTO openings (
                        rating_range_min,
                        label,
                        white_wr_sd,
                        black_wr_sd,
                        white_wr_ck,
                        black_wr_ck,
                        white_wr_fd,
                        black_wr_fd)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'''
                cur.execute(sql,row)

def create_summary_view(conn_str: str) -> None:
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            sql = '''
                CREATE OR REPLACE VIEW summary AS
                    SELECT stats.label,
                        stats.games,
                        stats.white_wr,
                        stats.black_wr,
                        openings.white_wr_sd,
                        openings.black_wr_sd,
                        openings.white_wr_ck,
                        openings.black_wr_ck,
                        openings.white_wr_fd,
                        openings.black_wr_fd
                    FROM stats
                    JOIN openings ON stats.label = openings.label;'''
            cur.execute(sql)

def export_view_csv(conn_str: str) -> None:
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            sql = 'SELECT * FROM summary;'
            data = cur.execute(sql).fetchall()
            pd.DataFrame(data).to_csv('rated_summary.csv')