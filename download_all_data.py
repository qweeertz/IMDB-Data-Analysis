from tools import initiate_db, TMDB_data, save_relevant_dataset


db_path = 'Data/IMDB.duckdb'

if __name__ == '__main__':
    initiate_db(db_path=db_path, download=True)
    TMDB = TMDB_data()
    TMDB.download_TMDB_data(minIMDBVotes=100)
    save_relevant_dataset(db_path)