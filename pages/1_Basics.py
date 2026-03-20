import streamlit as st
import plotly.express as px
import pandas as pd
from utils.helper_functions import votes_slider, country_name, lang_name


# =================================================================================================

st.title('Basic statistics about the dataset')

df = st.session_state.df

# =================================================================================================

st.header('What types of media are in the dataset?')

total_titles = df.shape[0]

st.write(f'In total, there are :yellow[{total_titles} titles] in the dataset.')

votes = votes_slider('slider_basics_type')
votes_filter = (df['IMDBVotes'] >= votes)
df_type = df.loc[votes_filter, 'Type'].value_counts().reset_index()
votes_titles = df_type['count'].sum()

st.write(f'There are :yellow[{votes_titles} titles] with at least :yellow[{votes}] IMDB votes left.')

fig = px.pie(df_type, names='Type', values='count', color='Type', color_discrete_map=st.session_state.type_colors)
st.plotly_chart(fig)

st.markdown(
    '''
    The majority of titles in the :yellow[entire dataset] are individual episodes of TV series. This is, of course, due to the mass of episodes present.
    TV series themselves make up approximately 10% of the data, or around 42k titles. :yellow[Individual TV episodes dominate] the dataset for
    :yellow[small number of minimum IMDB votes], however, they quickly become less relevant when increasing the number of minimum IMDB votes.
    At :yellow[very high numbers of minimum IMDB votes] (20.000), :yellow[movies clearly dominate] making up approximately 75% of the dataset.
    '''
)

# =================================================================================================

st.divider()

st.header('How many titles were released each year?')

st.write(
    '''
    _Disclaimer_: Different seasons of TV series may of course be released over some years.
    Since we only have data of the starting and ending year (so we do not know which/how many seasons were released in between these years),
    we :yellow[only consider the starting year] - even for TV series.
    '''
)

df_years = df[['StartYear', 'Type']]
total_titles = df_years.dropna(subset='StartYear').shape[0]

fig = px.histogram(df_years, x='StartYear', color='Type', color_discrete_map=st.session_state.type_colors)
fig.update_layout(bargap=0.05, xaxis_title='Release Year', yaxis_title='Count')
fig.update_xaxes(range=[1870, 2030], tickmode='array', tickvals=list(range(1870, 2030, 10)))
st.plotly_chart(fig)

st.write(
    '''
    A few notes:
    - The data is :yellow[not completely up-to-date]. As far as I could tell, the IMDB is not specific about how up-to-date their publicly available files are.
    Of course, this affects the data for the more recent years, in particular 2026.
    - In the earliest years, roughly :yellow[before 1920], the :yellow[large majority of titles] released were :yellow[short movies].
    - Also, :yellow[video games] only appeared in :yellow[more recent years] (earliest title listed in 1974) while :yellow[TV series] roughly appeared :yellow[after 1950].
    - One can clearly see an :yellow[incision due to Covid] in 2020 :yellow[most crucial] for :yellow[movies].
    Interestingly, which is pretty hard to see in this plot, this incision is :yellow[not present for TV series], but for TV episodes.
    This :yellow[may indicate] that many TV series were :yellow[cut short] due to Covid but were :yellow[still released].
    - Whether or not the :yellow[recession] after more or less 2022 is really due to less titles being produced, or due to the data being not up-to-date is an open question.
    '''
)

# =================================================================================================

st.divider()

st.header('Which countries produce these titles in which language?')

st.write(
    '''
    _Disclaimer_: This data is :yellow[based on TMDB]. The TMDB data is not as complete as the IMDB and maybe, not all available TMDB data has been downloaded.
    In fact, the search engine of the TMDB page only lists "movies" And "Series", however, using IMDB IDs with the TMDB API reveals some titles of other type as well.
    Nevertheless, video games and TV episodes are very sparse.
    '''
)

votes = votes_slider('slider_basics_countrylanguage')
votes_filter = (df['IMDBVotes'] >= votes)
df_filtered = df[votes_filter]

df_countries = df_filtered[['OriginCountry']].explode('OriginCountry').dropna(subset='OriginCountry').value_counts().reset_index()
df_countries['CountryNames'] = df_countries['OriginCountry'].map(country_name)
df_languages = df_filtered[['OriginalLanguage']].value_counts().reset_index()
df_languages['LanguageNames'] = df_languages['OriginalLanguage'].map(lang_name)

nonna_titles, total_titles = df_filtered['OriginCountry'].apply(lambda x: len(x) > 0).sum(), df_filtered[['OriginCountry']].shape[0]

st.write(
    f'''
    (Fractional) number of titles depicted: :yellow[{nonna_titles}/{total_titles}]
    '''
)

col1, col2 = st.columns(2)
with col1:
    fig = px.pie(df_countries, names='CountryNames', values='count', color='OriginCountry', color_discrete_map=st.session_state.type_colors, title='Countries of Origin')
    st.plotly_chart(fig)
with col2:
    fig = px.pie(df_languages, names='LanguageNames', values='count', color='OriginalLanguage', color_discrete_map=st.session_state.type_colors, title='Original Language')
    st.plotly_chart(fig)

st.write(
    '''
    A few notes:
    - The :yellow[colors] between the 2 plots generally :yellow[do not match]: The biggest orange area on the left is India while it is japanese on the right.
    - Since some languages are spoken in more than one country, e.g. english in the US and the UK, the two plots differ.
    - In addition, for example indian movies come in different languages, including Hindi and Tamil and thus, India makes up a large portion in the left plot
    while these titles are distributed to smaller areas on the right.
    - The large majority of movies come from the US.
    Also, :yellow[when filtering] the data for titles with a larger number of minimum IMDB votes, the :yellow[US] (and more so english on the right) becomes :yellow[much more dominant].
    So, the US (and english) dominates the most popular titles.
    '''
)

# =================================================================================================

st.divider()

st.header('What is the distribution of IMDB votes (more precisely)?')

bins = [100] + list(range(1000, 5000, 1000)) + list(range(5000, 30000, 5000)) + [float('inf')]
labels = ['100-1000'] + [f'{i}-{i + 1000}' for i in range(1000, 5000, 1000)] + [f'{i}-{i + 5000}' for i in range(5000, 25000, 5000)] + ['>25k']
df_votes = df[['tconst', 'Type', 'IMDBVotes']].copy()
df_votes['IMDBVotes_bins'] = pd.cut(df_votes['IMDBVotes'], bins=bins, labels=labels)

fig = px.histogram(df_votes, x='IMDBVotes_bins', color='Type', color_discrete_map=st.session_state.type_colors)
fig.update_layout(bargap=0.05, xaxis_title='IMDB Votes', yaxis_title='Count')
fig.update_xaxes(categoryorder='array', categoryarray=labels)
st.plotly_chart(fig)

st.write(
    '''
    We see that the :yellow[majority] of titles (more than 300k) have :yellow[100-1000 IMDB votes]. These titles are less known and we have employed and will employ
    the number of :yellow[IMDB votes as a metric for relevance]. \n
    '''
)

counts = df_votes.groupby(['IMDBVotes_bins', 'Type'], observed=True).size().reset_index(name='Count')
counts['Proportion'] = counts['Count'] / counts.groupby('IMDBVotes_bins', observed=True)['Count'].transform('sum')

fig = px.bar(counts, x='IMDBVotes_bins', y='Proportion', color='Type', color_discrete_map=st.session_state.type_colors)
st.plotly_chart(fig)

st.write(
    '''
    In the plot above, which shows the relative portion of the media types in each IMDB votes bin, we can see that the more relevant titles with
    many IMDB votes are mostly movies. In fact, movies become more and more relevant while individual episodes of TV series become less and less relevant.
    This reflects the fact that :yellow[movies are the most relevant type of media for the IMDB community]. Also, individual episodes of TV series are quite rarely rated.
    Note, that the relevance of TV series stays pretty much constant with a small peak around 20k-25k IMDB votes.
    '''
)

# =================================================================================================

st.divider()

st.header('What can we learn about the runtime?')

st.write(
    '''
    First, in the plot below, we depict the :yellow[distribution of runtime] for the different types of media.
    Some series are listed with thousands of minutes of runtime, so we restrict ourselves to runtimes of :yellow[at most 240 minutes] (4 hours).
    '''
)

df_runtime = df.loc[df['Runtime'] <= 240, ['Runtime', 'StartYear', 'Type']]

fig = px.histogram(df_runtime, x='Runtime', color='Type', color_discrete_map=st.session_state.type_colors)
fig.update_layout(bargap=0.5)
st.plotly_chart(fig)

st.write(
    '''
    A few notes:
    - Obviously, there is a :yellow[clear distinction] between the types. Shorts last only a few minutes, episodes usually range from 20 to 60 minutes and
    movies mostly range over approximately 60 to 150 minutes with quite a lot of exceptions which are shorter or much longer.
    - Here, we also see that :yellow[series] are quite :yellow[inconsistent]. Sometimes their total runtime is being displayed (thousands of minutes; excluded here),
    sometimes the average runtime of an episode (which is reflected by the fact that series and episodes overlap quite a lot).
    - Video games are sparse and the claim of a runtime is very arbitrary. After all, games do not have a well-defined runtime, but that depends on the players.
    '''
)

st.subheader('So how does the (average) runtime develop over time? Is there a trend?')

fig = px.line(df_runtime.groupby(['Type', 'StartYear']).mean('Runtime').reset_index(), x='StartYear', y='Runtime', color='Type', color_discrete_map=st.session_state.type_colors)
fig.update_xaxes(range=[1870, 2030], tickmode='array', tickvals=list(range(1870, 2030, 10)))
st.plotly_chart(fig)

st.write(
    '''
    A few notes:
    - The data for :yellow[video games] is :yellow[very volatile]. This is mostly due to few datapoints mixed with the high inconsistence in setting a fixed runtime for a game.
    - For about a hundred years, the runtime of :yellow[Shorts] has remained :yellow[quite consistent] while at the very first years :yellow[pre 1910],
    shorts were usually just a :yellow[few seconds] long (which is listed as 1 minute).
    - :yellow[Series/Episodes] (again, the runtime of series is listed quite inconsistently; Here, the runtime probably refers to the average length of an episode)
    stayed :yellow[fairly constant] over time, perhaps with a :yellow[slight tendency] in :yellow[recent years] to :yellow[longer episodes].
    In the last 1-2 decades, series became much more popular and better produced. It would be no surprise that the average episode length increased with it.
    - At the first few decades movies were quite short well below 90 minutes. Then, over decades, from :yellow[1960-2020], they remained :yellow[very consistently]
    at :yellow[100 minutes], but in the :yellow[last few years], the runtime of movies seems to have :yellow[slightly increased].
    Again, the last year of 2026 should be excluded due to incomplete data.
    '''
)
