import pandas as pd
import os
import duckdb
import ast
import itertools
from collections import defaultdict
import networkx as nx
from pyvis.network import Network
from utils.helper_functions import country_name, make_hovertext


# =================================================================================================

processed_data_path = 'processed_data'
db_path = 'raw_data/data.duckdb'

# =================================================================================================

def process_general_data(small_dataset=False, db_path=db_path, processed_data_path=processed_data_path):

    if not os.path.exists(db_path):
        raise FileNotFoundError('Required file ' + db_path + ' not found. Download data first!')
    
    if not os.path.exists(processed_data_path):
        os.makedirs(processed_data_path)

    con = duckdb.connect(db_path)
    df = con.execute('SELECT * FROM view_titles_100').fetchdf()

    if small_dataset:
        df = df[df['IMDBVotes'] >= 10000]
    
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

    df.insert(1, 'Link', 'https://www.imdb.com/title/' + df['tconst'])

    df.to_parquet(processed_data_path + '/full_data.parquet')


def load_data(processed_data_path=processed_data_path):
    df = pd.read_parquet(processed_data_path + '/full_data.parquet')
    return df


class process_data_for_pages():
    def __init__(self):
        self.df = load_data()
    

    def Ratings(self):
        dir = 'processed_data/2_Ratings'
        os.makedirs(dir, exist_ok=True)

        df_countries = self.df[['OriginCountry', 'IMDBRating']].explode('OriginCountry')
        filtered_countries = df_countries['OriginCountry'].value_counts().head(10)
        df_countries = df_countries[df_countries['OriginCountry'].isin(filtered_countries.index)]
        df_countries.to_parquet(dir + '/df_countries.parquet')

        median_countries = df_countries.groupby('OriginCountry').median().sort_values('IMDBRating', ascending=False).reset_index()
        median_countries['OriginCountry'] = median_countries['OriginCountry'].apply(country_name)
        median_countries.to_parquet(dir + '/median_countries.parquet')

        df_years = self.df[['Type', 'StartYear', 'IMDBRating']].groupby(['Type', 'StartYear'], observed=True)['IMDBRating'].mean().reset_index()
        df_years.to_parquet(dir + '/df_years.parquet')

        df_runtime = self.df.loc[(self.df['Runtime'] <= 240) & self.df['Type'].isin(['Movies', 'Episodes', 'Shorts']), ['IMDBRating', 'Runtime', 'Type']].groupby(['Type', 'Runtime'], observed=True).mean('IMDBRating').reset_index()
        df_runtime.to_parquet(dir + '/df_runtime.parquet')


    def Genres(self):
        dir = 'processed_data/3_Genres'
        os.makedirs(dir, exist_ok=True)

        df_genres = self.df[self.df['Type'] != 'Episodes'].explode('Genres')
        df_genres = df_genres[df_genres['Genres'] != '']
        df_genres.to_parquet(dir + '/df_genres.parquet')

        genres_by_type = df_genres[['Genres', 'Type']].groupby(['Genres', 'Type'], observed=True).size().reset_index(name='Count')
        genres_by_type.to_parquet(dir + '/df_genres_by_type.parquet')

        blocked_genres = ['Game-Show', 'Reality-TV', 'Talk-Show', 'Adult', 'Short']
        df_genres = df_genres[~df_genres['Genres'].isin(blocked_genres)]
        genres_count = df_genres['Genres'].value_counts()

        N_countries = 25
        df_countries = df_genres[['Genres', 'OriginCountry']].explode('OriginCountry')
        countries_count = self.df['OriginCountry'].explode('OriginCountry').value_counts().iloc[:N_countries]
        countries_count.to_frame().to_parquet(dir + '/countries_count.parquet')

        df_countries = df_countries[df_countries['OriginCountry'].isin(countries_count.index)].groupby(['OriginCountry', 'Genres'], observed=True).size().reset_index(name='Count')
        df_countries['Count'] = df_countries['Count'] / df_countries['OriginCountry'].map(countries_count)
        df_countries['OriginCountry'] = pd.Categorical(df_countries['OriginCountry'], categories=countries_count.index, ordered=True)
        df_countries['Genres'] = pd.Categorical(df_countries['Genres'], categories=genres_count.index, ordered=True)
        df_countries['OriginCountryNames'] = df_countries['OriginCountry'].map(country_name)
        df_countries.to_parquet(dir + '/df_countries.parquet')

        genres_mean = df_countries[['Genres', 'Count']].groupby('Genres', observed=True).mean().reset_index()
        genres_mean['Genres'] = pd.Categorical(genres_mean['Genres'], categories=genres_count.index, ordered=True)
        genres_mean = genres_mean.sort_values('Genres')
        genres_mean.to_parquet(dir + '/genres_mean.parquet')

        df_ratings = df_genres[['Genres', 'IMDBRating']]
        ratings_mean = df_ratings.groupby('Genres').median().sort_values('IMDBRating', ascending=False)
        ratings_mean.to_parquet(dir + '/ratings_mean.parquet')

        years_count = df_genres['StartYear'].value_counts()
        df_years = df_genres[['Genres', 'StartYear']].groupby(['Genres', 'StartYear']).size().reset_index(name='Count')
        df_years['Count'] = df_years['Count'] / df_years['StartYear'].map(years_count)
        df_years.to_parquet(dir + '/df_years.parquet')


    def People(self):
        dir = 'processed_data/4_People'
        os.makedirs(dir, exist_ok=True)

        def compute_role_stats(df_exploded, role):
            df_titles = (
                df_exploded.groupby(role)
                .agg(
                    MeanRating=('IMDBRating', 'mean'),
                    MeanVotes=('IMDBVotes', 'mean'),
                    Count=('PrimaryTitle', 'count'),
                    Movies_Rating=('PrimaryTitle', lambda titles: sorted(
                        list(zip(df_exploded.loc[titles.index].drop_duplicates('PrimaryTitle')['PrimaryTitle'], df_exploded.loc[titles.index].drop_duplicates('PrimaryTitle')['IMDBRating'])),
                        key=lambda t: t[1], reverse=True
                    )),
                    Movies_Votes=('PrimaryTitle', lambda titles: sorted(
                        list(zip(df_exploded.loc[titles.index].drop_duplicates('PrimaryTitle')['PrimaryTitle'], df_exploded.loc[titles.index].drop_duplicates('PrimaryTitle')['IMDBVotes'])),
                        key=lambda t: t[1], reverse=True
                    ))
                )
                .reset_index()
                .copy()
            )
            df_titles['hovertext_rating'] = df_titles['Movies_Rating'].map(make_hovertext)
            df_titles['hovertext_votes'] = df_titles['Movies_Votes'].map(make_hovertext)
            df_titles.drop(columns=['Movies_Rating', 'Movies_Votes'], inplace=True)
            return df_titles

        roles = ['Directors', 'Writers', 'Actors', 'Actresses', 'Cinematographers', 'Composers']
        for r,role in enumerate(roles):
            df_exploded = self.df.loc[(self.df['IMDBVotes'] >= 25000) & (self.df['Type'] == 'Movies'), [role, 'PrimaryTitle', 'StartYear', 'IMDBRating', 'IMDBVotes', 'OriginCountry', 'Genres']].explode(role)
            df_exploded.to_parquet(dir + '/df_' + role.lower() + '.parquet')
            df_titles = compute_role_stats(df_exploded, role)
            df_titles = df_titles[df_titles['Count'] >= 4]
            df_titles.to_parquet(dir + '/df_titles_' + role.lower() + '.parquet')


    def Finances(self):
        dir = 'processed_data/5_Finances'
        os.makedirs(dir, exist_ok=True)

        df_finances = self.df[(self.df['Type'] == 'Movies') & (self.df['IMDBVotes'] >= 10000)]
        budget_filter, revenue_filter = (df_finances['Budget'] >= 1000), (df_finances['Revenue'] >= 1000)
        df_roi = df_finances.loc[budget_filter & revenue_filter, ['tconst', 'PrimaryTitle', 'Budget', 'Revenue', 'StartYear', 'IMDBVotes', 'IMDBRating', 'Genres', 'Directors', 'Actors', 'Actresses']]
        df_roi['ROI'] = df_roi['Revenue'] / df_roi['Budget']
        df_roi['Profit'] = df_roi['Revenue'] - df_roi['Budget']
        df_roi.sort_values('ROI', ascending=False, inplace=True)
        df_roi.to_parquet(dir + '/df_roi.parquet')

        df_years = df_roi.groupby('StartYear')[['Budget', 'Revenue', 'Profit']].sum().reset_index()
        df_years.to_parquet(dir + '/df_years.parquet')

        df_genres = df_roi[['Budget', 'Revenue', 'ROI', 'Genres']].explode('Genres').groupby('Genres')[['Budget', 'Revenue', 'ROI']].mean().reset_index()
        df_genres.to_parquet(dir + '/df_genres.parquet')

        def compute_role_stats(df_role, role):
            # Build movie list per person
            def movie_list(group):
                g = df_role.loc[group.index, ['PrimaryTitle']].drop_duplicates('PrimaryTitle')
                return list(g['PrimaryTitle'])

            return (
                df_role.groupby(role, observed=True)
                .agg(
                    Count=('PrimaryTitle', lambda x: x.nunique()),
                    MeanBudget=('Budget', 'mean'),
                    MeanRevenue=('Revenue', 'mean'),
                    MeanROI=('ROI', 'mean'),
                    Movies=('PrimaryTitle', movie_list)
                )
                .reset_index()
            )
        
        roles = ['Directors', 'Actors', 'Actresses']
        exploded = {}
        for role in roles:
            exploded[role] = df_roi[['PrimaryTitle', 'Budget', 'Revenue', 'ROI', role]].dropna(subset=[role]).explode(role)
        role_stats = {
            role: compute_role_stats(exploded[role], role)
            for role in ['Directors', 'Actors', 'Actresses']
        }
        for role in roles:
            min_count = 2 if role == 'Directors' else 5
            role_stats[role] = role_stats[role][role_stats[role]['Count'] >= min_count]
            role_stats[role].to_parquet(dir + '/df_' + role.lower() + '.parquet')


    def prepare_all(self):
        self.Ratings()
        self.Genres()
        self.People()
        self.Finances()


class Graph():
    def __init__(self, processed_data_path=processed_data_path):
        self.graph_path = processed_data_path + '/4_People'
        os.makedirs(self.graph_path, exist_ok=True)


    def build_network(self):
        df = load_data()
        types = ['Movies', 'Series']
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

        nodes.to_parquet(self.graph_path + '/Nodes.parquet', index=False)
        edges.to_parquet(self.graph_path + '/Edges.parquet', index=False)


    def build_graph(self):
        nodes = pd.read_parquet(self.graph_path + '/Nodes.parquet')
        edges = pd.read_parquet(self.graph_path + '/Edges.parquet')

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

        net.set_options("""
            {
            "interaction": {
                "tooltipDelay": 50
            },
            "edges": {
                "color": {
                    "inherit": "from"
                },
                "smooth": {
                    "enabled": true,
                    "type": "dynamic"
                },
                "font": {
                    "multi": "html"
                }
            },
            "nodes": {
                "font": {
                    "size": 10,
                    "multi": "html"
                }
            },
            "physics": {
                "enabled": false
            }
            }
            """)

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
                size=10 + data['NumberTitles'] * 0.1,
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
        net.save_graph(self.graph_path + '/Network.html')

if __name__ == '__main__':
    pdfp = process_data_for_pages()
    pdfp.Finances()
    graph = Graph()