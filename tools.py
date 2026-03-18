import pandas as pd
import os
import duckdb
import asyncio
import aiohttp
from aiolimiter import AsyncLimiter
import time
from dotenv import load_dotenv
import ast
import itertools
from collections import defaultdict
import networkx as nx
from pyvis.network import Network

files = {
    'name_basics': 'https://datasets.imdbws.com/name.basics.tsv.gz',
    # 'title_akas': 'https://datasets.imdbws.com/title.akas.tsv.gz',   # Maybe Alternatively Known AS? Has data on alternative titles, including titles in other languages, but also things like rumored titles.
    'title_basics': 'https://datasets.imdbws.com/title.basics.tsv.gz',
    # 'title_crew': 'https://datasets.imdbws.com/title.crew.tsv.gz',   # Everything in title_principals already.
    'title_principals': 'https://datasets.imdbws.com/title.principals.tsv.gz',
    'title_ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz'
}

db_path = 'Data/IMDB.duckdb'


def initiate_db(db_path, files=files, download=False):

    '''
    Downloads IMDb .tsv.gz files and writes them into a SQLite database.
    Creates tables automatically based on the TSV header.
    '''

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

    con.execute('''
        CREATE OR REPLACE VIEW view_titles AS
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
        LEFT JOIN TMDB_Data AS TD ON TD.tconst = tr.tconst;
    ''')

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
    def __init__(self):
        load_dotenv()
        self.TMDB_API_KEY = os.getenv('API_KEY')
        self.TMDB_BASE = 'https://api.themoviedb.org/3'

        self.db_path = db_path
        return

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

        con = duckdb.connect(self.db_path)

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
        IMDB_IDs_old = [ID[0] for ID in con.execute('SELECT tconst FROM TMDB_Data').fetchall()]

        IMDB_IDs = con.execute(
            f'''
            SELECT tconst FROM VIEW_TITLES_100
            WHERE IMDBVotes >= {minIMDBVotes}
            '''
        ).fetchall()
        IMDB_IDs = [x[0] for x in IMDB_IDs]

        print(str(len(IMDB_IDs)) + '-' + str(len(IMDB_IDs_old)) + '=' + str(len(IMDB_IDs) - len(IMDB_IDs_old)) + ' entries to add...')
        time0 = time.time()
        entries = asyncio.run(self.fetch_many(IMDB_IDs, IMDB_IDs_old))
        print(f'Downloading TMDB data took {round(time.time() - time0, 4)} seconds.')

        if isinstance(entries, list) and entries:
            con.executemany(
                '''
                INSERT OR REPLACE INTO TMDB_Data
                (tconst, originalLanguage, originCountry, Budget, Revenue, TMDBVotes, TMDBRating)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', entries
            )


def save_relevant_dataset(db_path):
    initiate_db(db_path, download=False)
    con = duckdb.connect(db_path)
    df = con.execute('SELECT * FROM view_titles_100').fetchdf()
    
    columns_to_change_NA_type = ['PrimaryTitle', 'OriginalTitle', 'OriginalLanguage', 'Type', 'StartYear', 'EndYear', 'Genres', 'Runtime', 'Budget', 'Revenue', 'Adult', 'IMDBRating', 'IMDBVotes', 'TMDBRating', 'TMDBVotes']
    df[columns_to_change_NA_type] = df[columns_to_change_NA_type].replace('\\N', pd.NA)
    df['Genres'] = df['Genres'].fillna('').str.split(',').apply(lambda x: x or [])

    df[['StartYear', 'EndYear', 'Runtime', 'Adult', 'IMDBVotes', 'TMDBVotes']] = df[['StartYear', 'EndYear', 'Runtime', 'Adult', 'IMDBVotes', 'TMDBVotes']].astype('Int64')
    df[['Budget', 'Revenue', 'IMDBRating', 'TMDBRating']] = df[['Budget', 'Revenue', 'IMDBRating', 'TMDBRating']].astype('Float64')

    str_columns = ['tconst', 'PrimaryTitle', 'OriginalTitle', 'OriginalLanguage', 'Type']
    # num_columns = ['StartYear', 'EndYear', 'Runtime', 'Budget', 'Revenue', 'Adult', 'IMDBRating', 'IMDBVotes', 'TMDBRating', 'TMDBVotes']
    list_columns = ['OriginCountry', 'Genres', 'Directors', 'Writers', 'Actors', 'Actresses', 'Cinematographers', 'Composers']

    df[str_columns] = df[str_columns].fillna(pd.NA)
    df[list_columns] = df[list_columns].fillna(100)  # fillna can not fill with an empty list, so we use this workaround
    df[list_columns] = df[list_columns].map(lambda x: [] if (isinstance(x, int) and x == 100) else x)
    df[list_columns] = df[list_columns].map(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
    df[list_columns] = df[list_columns].map(lambda x: tuple(x))

    df = df[~(df['Type'].isin(['video', 'tvSpecial']))]
    types = {
        'movie': 'Movies',
        'tvMovie': 'Movies',
        'tvSeries': 'Series',
        'tvMiniSeries': 'Series',
        'short': 'Shorts',
        'tvShort': 'Shorts',
        'tvEpisode': 'Episodes',
        'videoGame': 'Games'
    }
    df.loc[:, 'Type'] = df['Type'].map(lambda x: types[x])

    df.to_parquet('Data/data.parquet')


def load_data():
    df = pd.read_parquet('Data/data.parquet')
    return df


def build_network():
    df = load_data()
    types = ['movie', 'tvMovie', 'tvSeries', 'tvMiniSeries']
    types = ['movie', 'tvSeries', 'tvMiniSeries']
    roles = {'Directors': 150, 'Actors': 200, 'Actresses': 200}
    df = df[(df['Type'].isin(types)) & (df['IMDBVotes'] >= 25000)]

    # Expand all roles
    rows = []
    for role in roles:
        df_temp = df[['tconst', 'PrimaryTitle', role]].explode(role)
        df_temp = df_temp[df_temp[role].notna()]
        df_temp = df_temp.rename(columns={role: 'Person'})
        df_temp['Role'] = role
        rows.append(df_temp)
    df_exploded = pd.concat(rows, ignore_index=True)

    # Build nodes dataframe
    nodes = df_exploded.groupby(['Person', 'Role']).size().reset_index(name='NumberTitles')

    Directors = nodes[nodes['Role'] == 'Directors'].nlargest(roles['Directors'], 'NumberTitles')
    Actors = nodes[nodes['Role'] == 'Actors'].nlargest(roles['Actors'], 'NumberTitles')
    Actresses = nodes[nodes['Role'] == 'Actresses'].nlargest(roles['Actresses'], 'NumberTitles')

    nodes = pd.concat([Directors, Actors, Actresses], ignore_index=True)
    filtered_people = set(nodes['Person'])

    # Build edges dataframe
    df_exploded = df_exploded[df_exploded['Person'].isin(filtered_people)]
    title_groups = df_exploded.groupby('tconst')['Person'].apply(list)
    pair_titles = defaultdict(list)
    tconst_to_title = df.set_index('tconst')['PrimaryTitle'].to_dict()

    for tconst, people in title_groups.items():
        unique_people = sorted(set(people))
        for a, b in itertools.combinations(unique_people, 2):
            pair_titles[(a, b)].append(tconst_to_title[tconst])

    edges = pd.DataFrame([(a, b, len(titles), titles) for (a, b), titles in pair_titles.items()], columns=['Person_A', 'Person_B', 'NumberSharedTitles', 'Titles'])

    # Save results

    nodes.to_parquet('Data/Nodes.parquet', index=False)
    edges.to_parquet('Data/Edges.parquet', index=False)


def build_graph():
    nodes = pd.read_parquet('Data/Nodes.parquet')
    edges = pd.read_parquet('Data/Edges.parquet')

    # Build NetworkX graph to obtain the graphs layout (i.e. positions of nodes and edges)
    G = nx.Graph()

    for _, row in nodes.iterrows():
        G.add_node(
            row['Person'],
            Role=row['Role'],
            NumberTitles=row['NumberTitles']
        )

    for _, row in edges.iterrows():
        G.add_edge(
            row['Person_A'],
            row['Person_B'],
            Titles=row['Titles'],
            weight=row['NumberSharedTitles'] * 0.05
        )

    pos = nx.spring_layout(G, seed=42, k=5, iterations=200)

    # Build PyVis graph for visualization
    net = Network(
        height='750px',
        width='100%',
        bgcolor='#222222',
        font_color='white',
        notebook=False
    )

    net.set_options('''
        {
        'interaction': {
            'tooltipDelay': 50
        },
        'edges': {
            'color': {
                'inherit': 'from'
            },
            'smooth': {
                'enabled': true,
                'type': 'dynamic'
            },
            'font': {
                'multi': 'html'
            }
        },
        'nodes': {
            'font': {
                'size': 10,
                'multi': 'html'
            }
        },
        'physics': {
            'enabled': false
        }
        }
        ''')

    # Role and edge colors
    role_colors = {
        'Directors': 'yellow',
        'Actors': 'blue',
        'Actresses': 'red'
    }

    def get_edge_color(a, b):
        role_a = G.nodes[a]['Role']
        role_b = G.nodes[b]['Role']
        roles = {role_a, role_b}

        if roles == {'Directors'}:
            return 'yellow'
        if roles == {'Actors'}:
            return 'blue'
        if roles == {'Actresses'}:
            return 'red'

        if 'Directors' in roles and 'Actors' in roles:
            return 'green'
        if 'Directors' in roles and 'Actresses' in roles:
            return 'orange'
        if 'Actors' in roles and 'Actresses' in roles:
            return 'purple'

        return 'gray'

    scale = 10000
    for node, (x, y) in pos.items():
        data = G.nodes[node]
        net.add_node(
            node,
            label=node,
            title=f'{data['Role'][:-2]}\n{data['NumberTitles']} titles' if data['Role'] == 'Actresses' else f'{data['Role'][:-1]}\n{data['NumberTitles']} titles',
            color=role_colors.get(data['Role'], 'gray'),
            size=10 + data['NumberTitles'] * 0.05,
            x=float(x * scale),
            y=float(y * scale),
            fixed=True
        )

    for a, b, data in G.edges(data=True):
        hover = 'Shared titles:\n' + '\n'.join(data['Titles'])
        net.add_edge(
            a,
            b,
            width=0.01 * (1 + data['weight']),
            color=get_edge_color(a, b),
            title=hover
        )

    # Render in Streamlit
    net.save_graph('Data/Network.html')
