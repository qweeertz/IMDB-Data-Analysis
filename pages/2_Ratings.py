import streamlit as st
import plotly.express as px
from utils.helper_functions import votes_slider, type_select, country_name


# =================================================================================================

st.title('Analysis of ratings')

df = st.session_state.df

# =================================================================================================

st.subheader('What is the general distribution of IMDB ratings?')

allowed_types = type_select('select_ratings')
votes = votes = votes_slider('slider_ratings')
votes_filter = (df['IMDBVotes'] >= votes)
df_ratings = df.loc[votes_filter & df['Type'].isin(allowed_types), ['IMDBRating']]

ratings_filter = df_ratings['IMDBRating'].describe()
ratings_all = df['IMDBRating'].describe()
ratings_20k = df[df['IMDBVotes'] >= 20000]['IMDBRating'].describe()

fig = px.histogram(df_ratings, x='IMDBRating')
if not df_ratings.empty:
    fig.add_vline(x=ratings_filter['mean'], line_width=2, line_color='red', annotation_text=f'Average = {ratings_filter['mean']:.2f}')
    fig.add_vline(x=ratings_filter['25%'], line_width=2, line_color='red', annotation_text=f'25% Quantile = {ratings_filter['25%']:.2f}')
    fig.add_vline(x=ratings_filter['75%'], line_width=2, line_color='red', annotation_text=f'75% Quantile = {ratings_filter['75%']:.2f}')
fig.update_traces(xbins=dict(start=1.0 - 0.05, end=10.0 + 0.05, size=0.1))
fig.update_layout(bargap=0.05, xaxis_title='IMDB Rating', yaxis_title='Count')
fig.update_xaxes(range=[1 - 0.1, 10 + 0.1], tickmode='array', tickvals=list(range(1, 11)), ticktext=[str(i) for i in range(1, 11)])

st.plotly_chart(fig)

st.write(
    f'''
    A few notes:
    - The distribution for the entire dataset is a :yellow[bit skewed] with a median of {ratings_all['50%']:.2f} and an average of {ratings_all['mean']:.2f}.
    - When increasing the number of minimum IMDB votes, the distribution becomes :yellow[slightly more concentrated around a slightly higher average].
    In the entire dataset, 50% of the titles are rated between ({ratings_all['25%']}, {ratings_all['75%']})
    while with a minimum of 20.000 IMDB votes, 50% of the titles are rated between ({ratings_20k['25%']},{ratings_20k['75%']}).
    The averages change from {ratings_all['mean']:.2f} to {ratings_20k['mean']:.2f}.
    - Some datasets (most notably games with more than 20.000 IMDB votes) can be :yellow[quite extreme] (with an average of approximately 9).
    Another example are TV episodes with more than 20.000 IMDB votes which also strongly tends to very high ratings.
    '''
)

N_topbot = 50

st.write(f'_Disclaimer_: The below Top and Bottom {N_topbot} :yellow[respects the minimum number of IMDB votes] from aboves slider.')

df_filtered = df[votes_filter & (df['Type'].isin(allowed_types))][['tconst', 'PrimaryTitle', 'Type', 'IMDBRating', 'IMDBVotes']]
df_top = df_filtered.sort_values('IMDBRating', ascending=False).head(N_topbot)
df_bot = df_filtered.sort_values('IMDBRating', ascending=True).head(N_topbot)

df_top.insert(2, 'Link', 'https://www.imdb.com/title/' + df_top['tconst'])
df_bot.insert(2, 'Link', 'https://www.imdb.com/title/' + df_bot['tconst'])

df_top.drop(columns=['tconst'], inplace=True)
df_bot.drop(columns=['tconst'], inplace=True)

top, bot = st.columns(2)
with top:
    st.markdown(f'#### Top {N_topbot}')
    st.dataframe(df_top, hide_index=True, column_config={'Link': st.column_config.LinkColumn('Link', display_text='IMDB')})
with bot:
    st.markdown(f'#### Bottom {N_topbot}')
    st.dataframe(df_bot, hide_index=True, column_config={'Link': st.column_config.LinkColumn('Link', display_text='IMDB')})

st.write(
    '''
    Playing around with the slider and checkboxes, we see the :yellow[importance of these filters] in the Top and Bottom 50 very well.
    Titles which are :yellow[relatively new] and have :yellow[few ratings] achieve quite :yellow[extreme ratings]. If we compare some of these entries,
    for example movies with at least 100 IMDB votes, with their entries on the IMDB page, we see differences in the IMDB Rating and the number of IMDB votes
    indicating that the data is not up-to-date.

    Also, our Top 50 of movies with at least 20.000 IMDB votes :yellow[does not exactly match] the first 50 entries of the :yellow[IMDB Top 250].
    Whether or not the IMDB Top 250 simply uses a stronger condition on the number of IMDB votes or uses some more sophisticated mechanisms is not clear.
    '''
)

# =================================================================================================

st.divider()

st.subheader('Ratings vs types')

st.write(
    '''
    The boxplots below show our previous assessments about the IMDB rating distribution for the different types of media more rigorously.

    In fact, we clearly see that :yellow[movies] are generally rated the :yellow[most critically]
    (both involving the median and the width) while TV episodes and games are generally rated highest.

    This reflects the fact that the IMDB community is most active and broad concerning movies.
    '''
)

votes = votes = votes_slider('slider_ratings_type')
votes_filter = (df['IMDBVotes'] >= votes)

df_type = df.loc[votes_filter, ['Type', 'IMDBRating']]
median_types = df_type.groupby('Type')['IMDBRating'].median().sort_values(ascending=False)

fig = px.box(df_type, x='Type', y='IMDBRating')
fig.update_xaxes(categoryorder='array', categoryarray=median_types.index)
fig.update_layout(xaxis_title='', yaxis_title='IMDB Rating')
st.plotly_chart(fig)

# =================================================================================================

st.divider()

st.subheader('Ratings vs country of origin')

st.write(
    '''
    Are there specific countries that produce higher rated titles?

    Below, we show :yellow[boxplots of the IMDB ratings] for different :yellow[countries of origin].
    We limit the depicted countries to the (at most) :yellow[10 countries with the most titles] in the dataset.
    ''')

st.write(
    '''
    _Disclaimer_: This data is :yellow[based on TMDB]. The TMDB data is not as complete as the IMDB. In fact, the search engine of the TMDB page only lists
    "movies" And "series", however, using IMDB IDs with the TMDB API reveals some titles of other type as well.
    '''
)

allowed_types = type_select('select_ratings_countries', default=[True, True, False, False, True])
votes = votes = votes_slider('slider_ratings_countries')
votes_filter = (df['IMDBVotes'] >= votes)

N_countries = 10

df_countries = df[votes_filter & (df['Type'].isin(allowed_types))][['OriginCountry', 'IMDBRating']].explode('OriginCountry')

filtered_countries = df_countries['OriginCountry'].value_counts().head(N_countries)

df_countries = df_countries[df_countries['OriginCountry'].isin(filtered_countries.index)]

median_countries = df_countries.groupby('OriginCountry').median().sort_values('IMDBRating', ascending=False).reset_index()
median_countries['OriginCountry'] = median_countries['OriginCountry'].apply(country_name)

fig = px.box(df_countries, x=df_countries['OriginCountry'].apply(country_name), y='IMDBRating')
fig.update_xaxes(categoryorder='array', categoryarray=median_countries['OriginCountry'])
fig.update_layout(xaxis_title='', yaxis_title='IMDB Rating')
st.plotly_chart(fig)

st.write(
    f'''
    We see that :yellow[Japan] has the highest median rating. With :yellow[{median_countries[median_countries['OriginCountry'] == 'Japan'].iloc[0, 1]}]
    it is :yellow[significantly higher] than :yellow[{median_countries[median_countries['OriginCountry'] == 'United States'].iloc[0, 1]}]
    of the :yellow[United States] (as the country with the largest movie industry). However, the distribution for the United States is :yellow[very broad]
    (similar to India's) and includes more highly rated titles.
    '''
)

# =================================================================================================

st.divider()

st.subheader('Ratings over time')

st.write('Are :yellow[older titles rated higher or lower]? Is there any relation at all?')

st.write('Since plotting boxplots for each year becomes quite busy, below, we only plot the :yellow[median (white)] for the different types.')

votes = votes = votes_slider('slider_ratings_years')
votes_filter = (df['IMDBVotes'] >= votes)

df_years = df.loc[votes_filter, ['Type', 'StartYear', 'IMDBRating']].groupby(['Type', 'StartYear'], observed=True)['IMDBRating'].mean().reset_index()

fig = px.line(df_years, x='StartYear', y='IMDBRating', color='Type', color_discrete_map=st.session_state.type_colors)
fig.update_xaxes(range=[1870, 2030], tickmode='array', tickvals=list(range(1870, 2030, 10)))
st.plotly_chart(fig)

st.markdown(
    '''
    A few notes:
    - The :yellow[sharp spikes], most significantly for shorts and movies, in 2026 is simply due to :yellow[imcomplete data]. These should be disregarded in this analysis.
    - In this plot, one can clearly see in which year each type :yellow[first appears].
    Note, that we only consider titles with :yellow[at the very least 100 IMDB votes], so this is not to be taken at face value.
    In fact, the first short listed on the IMDB in 1874, the [Passage de Venus](https://www.imdb.com/title/tt3155794), can be seen here.
    However, the first series listed in the IMDB Web Search, [BYU Weekly](https://www.imdb.com/title/tt32252746) is not listed in our dataset (as it only has 6 IMDB Votes).
    Still, we get an overview and see that shorts were released first, then movies, followed by series and episodes while games only appeared in 1971 in our dataset
    (while the first video game listed in the entire IMDB database [Turochamp](https://www.imdb.com/title/tt9324640) was released in 1948).
    - :yellow[Movies], at all minimum number of IMDB votes, show a :yellow[decreasing trend] over time. However, the median itself highly depends on the minimum number of
    IMDB votes selected.
    - The same is true for :yellow[series] while, interestingly, :yellow[episodes] show an :yellow[upward trend] when considering the :yellow[entire dataset].
    Lesser known titles, in general, are :yellow[more vulnerable] to :yellow[skewed ratings]. This is true :yellow[more so for episodes] which get rated way less than
    their corresponding series.
    - :yellow[Shorts at 100 minimum IMDB votes] show an :yellow[upward trend], while shorts with :yellow[at least 20.000 IMDB votes] show a rather :yellow[stagnant trend].
    - :yellow[Games] are quite :yellow[constantly rated highly].
    - Again, overall, we see that :yellow[movies] are generally :yellow[rated the lowest], followed by shorts and series while episodes and games are on top.
    '''
)

# =================================================================================================

st.divider()

st.subheader('Compare IMDB rating to TMDB rating')

st.write('Are there "global" :yellow[differences] in the :yellow[ratings] between the :yellow[IMDB] and the :yellow[TMDB] community?')

df_tmdb = df[['IMDBRating', 'IMDBVotes', 'TMDBRating', 'TMDBVotes']]

ratings_titles_IMDB = df_tmdb['IMDBRating'].notna().sum()
ratings_titles_TMDB = df_tmdb['TMDBRating'].notna().sum()
ratings_medians_Votes_IMDB = df_tmdb['IMDBVotes'].median()
ratings_medians_Votes_TMDB = df_tmdb['TMDBVotes'].median()

st.write(
    f'''
    In the dataset, there are :yellow[{ratings_titles_IMDB} titles] with an :yellow[IMDB rating] (which is the whole dataset) and :yellow[{ratings_titles_TMDB} titles]
    with a :yellow[TMDB rating].
    '''
)
st.markdown(
    rf'''
    First, let us look at the :yellow[number of votes] on both platforms.
    In the boxplot below (note the :yellow[logarithmic y-axis] which also causes the lower fence to be cut off (as $\lim_{{x\rightarrow 0}}\ln(x)\rightarrow-\infty$)),
    we see that the :yellow[median] of :yellow[IMDB votes] is {ratings_medians_Votes_IMDB} which is :yellow[more than an order of magnitude higher] than
    the :yellow[median] of :yellow[TMDB votes] {ratings_medians_Votes_TMDB}. Remember, that we limit our dataset to titles with at least 100 IMDB votes.
    The median over the entire IMDB database would be much lower. Nonetheless, we are comparing the same titles between the IMDB and the TMDB.
    After all, the :yellow[IMDB community] is simply :yellow[much larger] than the TMDB community as IMDB is the most popular movie database.
    '''
)

fig = px.box(df_tmdb, y=['IMDBVotes', 'TMDBVotes'])
fig.update_yaxes(type='log')
fig.update_layout(xaxis_title='', yaxis_title='Votes')
st.plotly_chart(fig)

ratings_titles_Ratings_IMDB = df_tmdb['IMDBRating'].median()
ratings_titles_Ratings_TMDB = df_tmdb['TMDBRating'].median()

st.write(
    f'''
    So what is the distribution of :yellow[IMDB ratings vs TMDB ratings]? Below, we see boxplots that are :yellow[quite similar].
    The :yellow[median IMDB rating] of :yellow[{ratings_titles_Ratings_IMDB}] is :yellow[generally higher] than that of the :yellow[TMDB {ratings_titles_Ratings_TMDB}]
    and the distribution of TMDB ratings is a bit broader.
    '''
)

fig = px.box(df_tmdb, y=['IMDBRating', 'TMDBRating'])
fig.update_layout(xaxis_title='', yaxis_title='Rating')
st.plotly_chart(fig)

st.write('The main take away is that the :yellow[TMDB community] is :yellow[much smaller] and (maybe because of that) a :yellow[bit more critical] overall.')

# =================================================================================================

st.divider()

st.header('Is there some correlation between the IMDB ratings and the titles runtime?')

st.write(
    '''
    Again, to exclude the very high runtimes of series, we focus on titles of :yellow[at most 240 minutes].  
    Since the individual lines are quite noisy, we supply the filtering below.
    ''')

allowed_types = type_select('ratings_runtime', default=[True, False, True, False, True])

df_runtime = df.loc[(df['Type'].isin(allowed_types)) & (df['Runtime'] <= 240), ['IMDBRating', 'Runtime', 'Type']]

fig = px.line(df_runtime.groupby(['Type', 'Runtime'], observed=True).mean('IMDBRating').reset_index(), x='Runtime', y='IMDBRating', color='Type', color_discrete_map=st.session_state.type_colors)
st.plotly_chart(fig)

st.write(
    '''
    A few notes:
    - Again, :yellow[games should be excluded], due to the fact that a well-defined Runtime is not really applicable.
    - :yellow[Series] and :yellow[episodes] are similarly :yellow[noisy] and both show :yellow[no real trend] between runtime and the IMDB rating.
    - :yellow[Shorts] are :yellow[elusive]. There seems to be one short listed with a runtime of 89 minutes which hardly can be classified as a short.
    This classificiation even above, say, 30 minutes is somewhat questionable.  
    In any case, disregarding these high runtimes, shorts are :yellow[quite consistent except] for :yellow[very short ones] with runtimes of 1-2 minutes.
    Note again, that the runtime is at least 1 minute. So, shorts of a few seconds are listed as 1 minute.
    - :yellow[Movies] show a :yellow[very clear trend]. As with long shorts, :yellow[short movies] (say below 30 minutes) are :yellow[very scarse] and make the data
    noisy and highly questionable. For :yellow[very high runtimes], the data becomes very noisy as well, again, due to the fact that there are :yellow[fewer titles]
    with such a long runtime. In between, we clearly see that Movies :yellow[between 80-90] are generally :yellow[rated the lowest]. As seen in "Basics",
    :yellow[most movies produced] are :yellow[around that range]. Longer movies tend to be rated quite a lot higher from 5.5 to 6.5.
    '''
)
