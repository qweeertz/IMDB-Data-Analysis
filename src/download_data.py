import os
import duckdb
import asyncio
import aiohttp
from aiolimiter import AsyncLimiter
import time
from dotenv import load_dotenv


# =================================================================================================

files = {
    'name_basics': 'https://datasets.imdbws.com/name.basics.tsv.gz',
    'title_basics': 'https://datasets.imdbws.com/title.basics.tsv.gz',
    'title_principals': 'https://datasets.imdbws.com/title.principals.tsv.gz',
    'title_ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz'
}

db_path = 'raw_data/data.duckdb'

# =================================================================================================

def initiate_db(db_path=db_path, files=files, download=False):

    '''
    Downloads IMDB .tsv.gz files and writes them into a SQLite database.
    Creates tables automatically based on the TSV header.
    '''

    raw_data_path = db_path.split('/')[-2]
    if not os.path.exists(raw_data_path):
        os.makedirs(raw_data_path)

    con = duckdb.connect(db_path)
    if download:
        for table_name, url in files.items():
            print(f'Creating table {table_name} from {url}...')
            con.execute(f'''
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{url}', compression='gzip', delim='\t', header=True);
            ''')

    con.execute('''
        CREATE OR REPLACE VIEW view_people AS
        SELECT
            tp.tconst,
            coalesce(list(nb.primaryName) FILTER (WHERE category = 'director'), []) AS Director,
            coalesce(list(nb.primaryName) FILTER (WHERE category = 'writer'), []) AS Writer,
            coalesce(list(nb.primaryName) FILTER (WHERE category = 'actor'), []) AS Actor,
            coalesce(list(nb.primaryName) FILTER (WHERE category = 'actress'), []) AS Actress,
            coalesce(list(nb.primaryName) FILTER (WHERE category = 'cinematographer'), []) AS Cinematographer,
            coalesce(list(nb.primaryName) FILTER (WHERE category = 'composer'), []) AS Composer
        FROM title_principals AS tp
        JOIN name_basics AS nb ON tp.nconst = nb.nconst
        GROUP BY tconst;
    ''')

    con.execute(
            '''
            CREATE TABLE IF NOT EXISTS TMDB_Data (
                tconst VARCHAR PRIMARY KEY,
                originalLanguage VARCHAR,
                originCountry VARCHAR[],
                Budget BIGINT,
                Revenue BIGINT,
                TMDBVotes BIGINT,
                TMDBRating REAL
            )
            '''
        )

    con.execute('''
        CREATE OR REPLACE VIEW view_titles_100 AS
        SELECT
            tb.tconst AS tconst,
            tb.primaryTitle AS PrimaryTitle,
            tb.originalTitle AS OriginalTitle,
            TD.originalLanguage AS OriginalLanguage,
            TD.originCountry AS OriginCountry,
            tb.titleType AS Type,
            tb.startYear AS StartYear,
            tb.endYear AS EndYear,
            tb.genres AS Genres,
            tb.runtimeMinutes AS Runtime,
            TD.Budget AS Budget,
            TD.Revenue AS Revenue,
            tb.isAdult AS Adult,
            tr.averageRating AS IMDBRating,
            tr.numVotes AS IMDBVotes,
            TD.TMDBRating AS TMDBRating,
            TD.TMDBVotes AS TMDBVotes,
            vp.Director AS Directors,
            vp.Writer AS Writers,
            vp.Actor AS Actors,
            vp.Actress AS Actresses,
            vp.Cinematographer AS Cinematographers,
            vp.Composer AS Composers
        FROM title_ratings AS tr
        LEFT JOIN title_basics AS tb ON tb.tconst = tr.tconst
        LEFT JOIN view_people AS vp ON vp.tconst = tr.tconst
        LEFT JOIN TMDB_Data AS TD ON TD.tconst = tr.tconst
        WHERE tr.numVotes >= 100;
    ''')

    con.close()


class TMDB_data():

    '''
    Class to handle TMDB data. This involves API calls of the TMDB API with a private API key stored in .env to extract the TMDB ID from an IMDB ID and retrieve relevant data
    from that, namely the original language, the country of origin, the budget and revenue and the TMDB rating and the number of TMDB votes.
    Uses an asynchronous architecture with a throttle to 30 calls per second to stay within the rate limits of TMDB (around 40 calls per second,
    according to https://developer.themoviedb.org/docs/rate-limiting as of March, 2026).
    '''

    def __init__(self, db_path=db_path):
        load_dotenv()
        self.TMDB_API_KEY = os.getenv('API_KEY')
        self.TMDB_BASE = 'https://api.themoviedb.org/3'
        self.db_path = db_path

    async def resolve_tmdb_object(self, session, IMDB_ID):
        url = f'{self.TMDB_BASE}/find/{IMDB_ID}'
        params = {
            'external_source': 'imdb_id',
            'language': 'en-US',
            'api_key': self.TMDB_API_KEY
        }

        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                if resp.status == 429:
                    resp.raise_for_status()
                return None, None, None

            data = await resp.json()

            if data.get('movie_results'):
                obj = data['movie_results'][0]
                return 'movie', obj['id'], {}

            if data.get('tv_results'):
                obj = data['tv_results'][0]
                return 'tv', obj['id'], {}

            if data.get('tv_episode_results'):
                obj = data['tv_episode_results'][0]
                return 'episode', obj['show_id'], {
                    'season': obj['season_number'],
                    'episode': obj['episode_number']
                }

            return None, None, None

    async def fetch_details(self, session, type_, TMDB_ID, extra):
        if TMDB_ID is None:
            return {}

        if type_ == 'movie':
            url = f'{self.TMDB_BASE}/movie/{TMDB_ID}'

        elif type_ == 'tv':
            url = f'{self.TMDB_BASE}/tv/{TMDB_ID}'

        elif type_ == 'episode':
            url = f'{self.TMDB_BASE}/tv/{TMDB_ID}/season/{extra['season']}/episode/{extra['episode']}'

        else:
            return {}

        params = {'language': 'en-US', 'api_key': self.TMDB_API_KEY}

        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                if resp.status == 429:
                    resp.raise_for_status()
                return {}

            return await resp.json()

    def normalize_entry(self, type_, data):
        if type_ is None:
            return (None, [], None, None, None, None)

        else:
            return (
                data.get('original_language'),
                data.get('origin_country', []),
                data.get('budget'),
                data.get('revenue'),
                data.get('vote_count'),
                data.get('vote_average'),
            )

    async def fetch_entry_for_imdb(self, session, limiter, IMDB_ID):
        async with limiter:
            type_, TMDB_ID, extra = await self.resolve_tmdb_object(session, IMDB_ID)

        async with limiter:
            details = await self.fetch_details(session, type_, TMDB_ID, extra)

        entry = self.normalize_entry(type_, details)
        return IMDB_ID, entry

    async def fetch_many(self, IMDB_IDs, IMDB_IDs_old, concurrency=10, max_rps=30):
        limiter = AsyncLimiter(max_rps, time_period=1)
        semaphore = asyncio.Semaphore(concurrency)

        IMDB_IDs_to_do = list(set(IMDB_IDs) - set(IMDB_IDs_old))

        results = []

        async with aiohttp.ClientSession() as session:

            async def worker(IMDB_ID):
                async with semaphore:
                    IMDB_ID, entry = await self.fetch_entry_for_imdb(session, limiter, IMDB_ID)
                    results.append((IMDB_ID, *entry))

            tasks = [asyncio.create_task(worker(i)) for i in IMDB_IDs_to_do]
            await asyncio.gather(*tasks)

        return results

    def download_TMDB_data(self, minIMDBVotes):

        """
        Function to download the TMDB data for titles in the data.duckdb database with at least minIMDBVotes.
        Creates a new empty table "TMDB_Data" in the data.duckdb if it does not exist fills it with TMDB data.
        
        The parameter minIMDBVotes can be used as a filter to not download all titles, but a subset.
        """

        if not os.path.exists(db_path):
            initiate_db(db_path, download=True)

        con = duckdb.connect(self.db_path)

        IMDB_IDs_old = [ID[0] for ID in con.execute('SELECT tconst FROM TMDB_Data').fetchall()]

        IMDB_IDs = con.execute(
            f'''
            SELECT tconst FROM VIEW_TITLES_100
            WHERE IMDBVotes >= {minIMDBVotes}
            '''
        ).fetchall()
        IMDB_IDs = [x[0] for x in IMDB_IDs]

        print(f'{max(0,len(IMDB_IDs) - len(IMDB_IDs_old))} entries to add... Expected time approximately {2 * max(0,len(IMDB_IDs) - len(IMDB_IDs_old)) / 30:.0f} seconds.')
        time0 = time.time()
        entries = asyncio.run(self.fetch_many(IMDB_IDs, IMDB_IDs_old))
        print(f'Downloading TMDB data took {round(time.time() - time0, 2)} seconds.')

        if isinstance(entries, list) and entries:
            con.executemany(
                '''
                INSERT OR REPLACE INTO TMDB_Data
                (tconst, originalLanguage, originCountry, Budget, Revenue, TMDBVotes, TMDBRating)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', entries
            )


if __name__ == '__main__':
    initiate_db(download=True)  # Call to download the newest IMDB data. Does not overwrite the TMDB data.
    TMDB_data = TMDB_data()
    TMDB_data.download_TMDB_data(minIMDBVotes=100)
