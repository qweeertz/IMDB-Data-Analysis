from src.download_data import initiate_db, TMDB_data
from src.process_data import process_general_data, process_data_for_pages, Graph

db_path = 'raw_data/data.duckdb'

if __name__ == '__main__':
    print('Initiating the database, downloading IMDB data... This may take a couple of minutes.')
    initiate_db(download=False)
    print('Initiation of database complete.')
    print()

    print('Downloading TMDB data...')
    TMDB = TMDB_data()
    TMDB.download_TMDB_data(minIMDBVotes=100)
    print('Download of TMDB data complete.')
    print()

    print('Processing the data for the streamlit app... This may take a couple of minutes.')
    process_general_data(small_dataset=True)
    pdfp = process_data_for_pages()
    pdfp.prepare_all()
    print('Processing complete.')
    print()

    print('Building graph...')
    graph = Graph()
    graph.build_network()
    graph.build_graph()
    print('Building of graph complete.')
