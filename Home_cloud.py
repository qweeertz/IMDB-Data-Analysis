import streamlit as st
import pandas as pd
import os
import requests


# =================================================================================================

def ensure_data_availability(data_path):
    if not os.path.exists(data_path):
        url = st.secrets['PRIVATE_DATA_URL']
        os.makedirs('Data', exist_ok=True)
        resp = requests.get(url)
        resp.raise_for_status()
        with open(data_path, 'wb') as f:
            f.write(resp.content)

@st.cache_data
def import_data(data_path):
    ensure_data_availability(data_path)
    return pd.read_parquet(data_path)

# =================================================================================================

# Preliminary set-up. Loading the dataframe df using import_data with @st.cache_data and initializing it in the session_state.

st.set_page_config(
    page_title='(Mostly) Movie Data Analysis',
    layout='wide'
)

data_path = 'Data/data.parquet'
if 'df' not in st.session_state:
    df = import_data(data_path)
    st.session_state.df = df
df = st.session_state.df

# Defining some variables and initializing them in the session_state for later usage on other pages.

default_colors = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
]
st.session_state.types = sorted(df['Type'].unique())
st.session_state.type_colors = {t: default_colors[i % len(default_colors)] for i, t in enumerate(st.session_state.types)}

st.session_state.str_columns = ['tconst', 'PrimaryTitle', 'OriginalTitle', 'OriginalLanguage', 'Type']
st.session_state.num_columns = ['StartYear', 'EndYear', 'Runtime', 'Budget', 'Revenue', 'Adult', 'IMDBRating', 'IMDBVotes', 'TMDBRating', 'TMDBVotes']
st.session_state.list_columns = ['OriginCountry', 'Genres', 'Directors', 'Writers', 'Actors', 'Actresses', 'Cinematographers', 'Composers']

# =================================================================================================

# Contents of the main page called 'Home'. Due to streamlits structure, appearantely, this has to go into the main file.

st.title('Overview & Introduction')

# =================================================================================================

st.subheader('Data Sources')

st.markdown(
    '''
    This project uses :yellow[two sources] for its data.
    The majority is from the [IMDB](https://www.imdb.com/) (Information courtesy of IMDb (https://www.imdb.com).
    Used with permission.)
    with additional data from the [TMDB](https://www.themoviedb.org/) [API](https://developer.themoviedb.org/docs/roadmap).
    '''
)

st.markdown(
    '''
    The IMDB via [URL](https://developer.imdb.com/non-commercial-datasets/) provides .gzip data files with information about "titles"
    including movies, TV series, short movies, but also video games, individual episodes, or TV specials, etc.
    The unpacked files altogether are quite large (approximately 9GB) so we create a SQL database located in 'Data/IMDB.duckdb' using [duckdb](https://duckdb.org/)
    of approximately 2.5GB creating a table for each .gzip file. From these, we create views of the relevant data streamlining everything into one structured dataset.
    '''
)

st.markdown(
    '''
    However, some interesting data is missing. So, using the unique IMDB ID, we use the TMDB API to extract further information including the :yellow[budget],
    :yellow[revenue], :yellow[original language] and the :yellow[country of origin] and add it as another table to the duckdb database.
    We do that via the class TMDB_data in tools.py that uses asynchronous API calls with a limiter to stay within the rate limits of the TMDB API.
    It needs to be executed independently (see tools.py) and is not part of the streamlit app.
    '''
)

st.write(
    '''
    To get a grasp on the vast IMDB data, we focus on titles with :yellow[at least 100 IMDB votes]. This is still very low and includes around :yellow[400.000 titles].
    In our later analysis, we at times restrict this further or consider only movies or TV series, etc.
    '''
)

# =================================================================================================

st.subheader('Data Cleaning & Preparation')

st.write(
    '''
    The file tools.py provides some functions to prepare the data. This includes merging the data from the different SQL tables into one, merging the IMDB and the TMDB data,
    unifying missing values, etc.  
    After cleaning and preparation, we :yellow[save a .parquet file] of the data that is really needed in this project. Instead of approximately 2.5 GB, the file is
    :yellow[67 MB] large.
    '''
)

# =================================================================================================

st.divider()

st.subheader('The Data')

df_display = df[(df['Type'].isin(['Movies', 'Series', 'Shorts'])) & (df['IMDBVotes'] >= 10000)].sort_values(by='IMDBRating', ascending=False).head(100).copy()
df_display.insert(1, 'Link', 'https://www.imdb.com/title/' + df_display['tconst'])
df_display[st.session_state.list_columns] = df_display[st.session_state.list_columns].astype(str)

st.write(
    '''
    Below is an excerpt of :yellow[100 titles] of the data focusing on titles (movies, TV series and shorts) with at least 10000 IMDB Votes sorted by IMDB Rating
    to give an idea of the data.
    '''
)

st.dataframe(df_display, hide_index=True, column_config={'Link': st.column_config.LinkColumn('Link', display_text='IMDB')})

st.markdown('#### Information about the data')

st.write(
    '''
    _Disclaimer_: Generally, missing values in columns having tuples are represented by empty tuples (), while all others are represented by pd.NA.
    '''
)

col1, col2, empty_col = st.columns([0.2, 0.2, 0.6])
with col1:
    st.metric('Rows (i.e. titles)', df.shape[0])
with col2:
    st.metric('Columns', df.shape[1])

st.write(
    '''
    _tconst_ (str) : unique IMDB ID \n
    _PrimaryTitle_ (str) : (english) primary title \n
    _OriginalTitle_ (str) : original title \n
    _OriginalLanguage_ (str) : original language using ISO 639-1 from TMDB \n
    _OriginCountry_ (tuple of str) : Tuple of countries of origin using ISO-3166 Alpha-2 from TMDB \n
    _Type_ (str) : Type of the title with values ['movie', 'tvMovie', 'tvSeries', 'tvMiniSeries', 'tvEpisode', 'short', 'tvShort', 'videoGame', 'video', 'tvSpecial'] \n
    _StartYear_ (pd.Int64) : Year of release. Since the data includes TV series that stretch over years, this is called StartYear \n
    _EndYear_ (pd.Int64) : Year of end. Almost always None (or rather pd.NA), except for TV series (and maybe few other exceptions) \n
    _Genres_ (tuple of str) : Tuple of genres \n
    _Runtime_ (pd.Int64) : Runtime in minutes \n
    _Budget_ (pd.Float64) : Budget obtained from TMDB \n
    _Revenue_ (pd.Float64) : Revenue obtained from TMDB \n
    _Adult_ (pd.Int64) : Whether or not the title is marked adult. 1=True, 0=False \n
    _IMDBRating_ (pd.Float64) : Average IMDB rating \n
    _IMDBVotes_ (pd.Int64) : Number of IMDB votes \n
    _TMDBRating_ (pd.Float64) : Average TMDB rating from TMDB \n
    _TMDBVotes_ (pd.Int64) : Number of TMDB votes from TMDB \n
    _Directors_ (tuple of str) : Tuple of directors \n
    _Writers_ (tuple of str) : Tuple of writers \n
    _Actors_ (tuple of str) : Tuple of actors \n
    _Actresses_ (tuple of str) : Tuple of actresses \n
    _Cinematographers_ (tuple of str) : Tuple of cinematographers \n
    _Composers_ (tuple of str) : Tuple of composers
    '''
)
