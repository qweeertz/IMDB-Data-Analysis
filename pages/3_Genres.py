import streamlit as st
import pandas as pd
import numpy as np
from itertools import product
import plotly.express as px
from utils.helper_functions import country_name


# =================================================================================================

st.title('Analysis of genres')

df = st.session_state.df

dir = 'processed_data/3_Genres'

if 'df_genres' not in st.session_state:
    st.session_state.df_genres = pd.read_parquet(dir + '/df_genres.parquet')

df_genres = st.session_state.df_genres

st.write('In this analysis, considering both the series and the episodes is "counting double", so we :yellow[exclude episodes].')

# =================================================================================================

st.subheader('What genres are listed in our dataset?')

genres_count = df_genres['Genres'].value_counts()
genres_by_type = pd.read_parquet(dir + '/df_genres_by_type.parquet')
genres_by_type['Genres'] = pd.Categorical(genres_by_type['Genres'], categories=genres_count.index, ordered=True)

fig = px.bar(genres_by_type.sort_values(['Genres', 'Type']), x='Genres', y='Count', color='Type', color_discrete_map=st.session_state.type_colors,
             category_orders={'Genres': list(genres_count.index)})
st.plotly_chart(fig)

st.write(
    '''
    We clearly see some patterns for specific genres.
    - Obviously, the genre "Short" almost perfectly matches the type "Shorts" with a contribution from series due to the fact that we condensed "tvSeries" and
    "tvMiniSeries" into "Series".
    - There are :yellow[genres specific for series]: "Reality-TV", "Game-Show" and "Talk-Show" are almost exclusively. We do not care about these genres and
    :yellow[from now on exclude them]. The gerne "News" is a bit different with most entries being series, but non-neglible contributions come from movies and shorts as well.
    Also, we remove "Adult" titles.
    - Most (other) genres are dominated by movies. However, "Animation" is quite evenly distributed between movies, shorts and series.
    '''
)

blocked_genres = ['Game-Show', 'Reality-TV', 'Talk-Show', 'Adult', 'Short']
df_genres_filtered = df_genres[~df_genres['Genres'].isin(blocked_genres)]
genres_count = df_genres_filtered['Genres'].value_counts()

# =================================================================================================

st.divider()

st.subheader('Do some countries preferably produce some specific genre(s)?')

st.markdown(
    '''
    To answer this question, for each country $C$ with $N_C$ listed titles, we count the number of titles $N_G$ of some specific genre $G$ and calculate the proportion $N_G/N_C$.
    So, :yellow[for each country], we get a :yellow[distribution of genres] listed in our dataset.  
    In the plot below, :yellow[for each genre], we :yellow[depict this ratio together with] the :yellow[mean] of that genre :yellow[over all countries].
    Thus, :yellow[if a country is significantly above or below that line], if these distributions are heavily skewed,
    :yellow[the given country releases unusually many/few titles of that specific genre]. \n
    _Disclaimer_: We want to emphasize that the proportions depicted below :yellow[do not mean] that :yellow[e.g. 30% of the titles of a specific genre
    x is produced in country y], but that this number means that :yellow[30% of country y's produced titles are of genre x]!
    '''
)

df_countries = pd.read_parquet(dir + '/df_countries.parquet')
countries_count = pd.read_parquet(dir + '/countries_count.parquet')
genres_mean = pd.read_parquet(dir + '/genres_mean.parquet')

facet_wrap = 3
fig = px.bar(df_countries, x='OriginCountry', y='Count', facet_col='Genres', facet_col_wrap=facet_wrap,
            category_orders={'Genres': list(genres_count.index), 'OriginCountry': list(countries_count.index)}, color='OriginCountry',
            hover_data={
                'OriginCountry': False,
                'OriginCountryNames': True,
                'Count': ':.3f'
            },
            height=np.ceil(genres_mean.shape[0] / facet_wrap) * 200)
for (i, j) in product(fig._get_subplot_rows_columns()[0], fig._get_subplot_rows_columns()[1]):
    try:
        y_vals = []
        for item in fig.select_traces(row=i, col=j):
            y_vals.append(item.y[0])
        mean = sum(y_vals) / len(y_vals)
        fig.add_hline(y=mean, row=i, col=j, line_color='red', line_width=2, annotation_text='Mean')
    except:
        continue
fig.update_yaxes(matches=None, rangemode='tozero', showticklabels=True)
fig.for_each_annotation(lambda a: a.update(text=a.text.split('=')[-1]))
fig.update_xaxes(tickvals=list(countries_count.index), ticktext=[country_name(c) for c in countries_count.index])
name_map = {code: country_name(code) for code in df_countries['OriginCountry'].unique()}
fig.for_each_trace(lambda tr: tr.update(name=name_map[tr.name], legendgroup=name_map[tr.name]))
xaxes = [ax for ax in fig.layout if ax.startswith('xaxis')]
gap = 0.05
for ax_name in xaxes:
    ax = fig.layout[ax_name]
    if hasattr(ax, 'domain'):
        d0, d1 = ax.domain
        width = d1 - d0
        new_width = width * (1 - gap)
        ax.domain = [d0, d0 + new_width]
st.plotly_chart(fig)

st.markdown(
    '''
    A few notes:
    - The :yellow[detailed analysis heavily depends] on :yellow[which countries] and :yellow[what types] we consider.
    Excluding games and shorts does not change much as there are quite few entries.
    However, :yellow[excluding series makes a difference], for example for Japan which has a very large Anime industry.
    - Here are :yellow[some interesting insights] when all 25 countries and all Types (except episodes) considered
        - :yellow[Drama] and :yellow[comedy] are :yellow[quite evenly distributed].
        - :yellow[Action dominates Hongkong] (with approximately 25%), which :yellow[correlates with crime], :yellow[followed by India], :yellow[Japan] and :yellow[China].
        - :yellow[Romance dominates South Korea] (with approximately 15%), :yellow[quite closely followed by Turkey], :yellow[China] and then :yellow[India].
        - :yellow[Documentary] is :yellow[very ingomogeneous]. The UK produces almost 10% documentaries while most of the :yellow[asian countries produce very few documentaries].
        - :yellow[Animation clearly dominates Japan] (approximately 19%) :yellow[followed by the Soviet Union (SU)] at 9%.
        This large difference is lower when excluding series due to Japans large Anime industry.
        - :yellow[Family is quite inhomogeneous]. Surprisingly, :yellow[Denmark has the highest share], :yellow[followed by the Soviet Union (SU)], :yellow[Sweden]
        and the :yellow[Netherlands]. :yellow[Hongkong produces almost no family titles].
        - The :yellow[Soviet Union] released :yellow[unusually many war titles followed by Poland], :yellow[Iran] and :yellow[Russia].
        The :yellow[US releases surprisingly few war movies].
        - The :yellow[Soviet Union], appearantly :yellow[released unusually many musicals], even :yellow[before India].
        Note, however, that not that many musicals are released overall, so perhaps, the numbers are a bit misleading. The classification may be at fault here, too.
        - :yellow[Italy produces unusually many western movies] :yellow[followed by the United States]. On the other hand, :yellow[South Korea and Iran never released a western].
        The classification may be at fault here.  
        :yellow[Japan] produced :yellow[almost no western] movies even though many :yellow[Samurai movies], like "Seven Samurai", :yellow[heavily influenced western movies] -
        but of course, the Samurai movies themselves are not classified as western movies.
        - :yellow[Only the United States], the :yellow[UK], :yellow[Denmark] and :yellow[Italy] appearantly released :yellow[Film-Noir titles]. Appearantly, the IMDB simply
        does not label any movie as Film-Noir anymore, even though they can be seen as representatives of the genre (e.g. "Sin City", "Nightcrawler", "Blade Runner 2049").
        - :yellow[News is very inhomogeneous] as well with :yellow[Australia] being :yellow[at the top].
    - In general, note, that the later genres are the ones with the least titles released.
    So these numbers may be misleading and perhaps, the classification is a bit questionable.
    '''
)
st.markdown(
    r'''
    To :yellow[analyze this over all genres], we calculate the :yellow[standard deviation] $\sigma_C=\sqrt{\frac{\sum_G (p_{C,G}-\mu_G)^2}{N_G}}$ for each country $C$, where
    - $p_{C,G}$ is the proportion for country $C$ and genre $G$
    - $\mu_G$ is the mean over all countries for some genre $G$
    - $N_G$ is the number of genres

    This number is a :yellow[measure] to assess :yellow[how much a country deviates] from the mean over all genres. We see that :yellow[Japan] and :yellow[Hongkong] score
    the highest here, meaning that :yellow[their industries are the most specialized] in this sense. The main reasons for this are Japans large Anime industry
    and Hongkongs action movies.
    '''
)

colors = [bar.marker.color for bar in fig.data[:25]]

df_countries['SqError'] = (df_countries['Count'] - df_countries['Genres'].map(genres_mean.set_index('Genres')['Count']).astype('Float64'))**2
countries_standard_deviation = np.sqrt(df_countries.groupby('OriginCountryNames', observed=True).sum('SqError') / countries_count.shape[0]).reset_index()
countries_standard_deviation = df_countries.groupby('OriginCountryNames', observed=True)['SqError'].mean().pow(0.5).reset_index(name='StdDev')

fig = px.bar(countries_standard_deviation, x='OriginCountryNames', y='StdDev')
fig.update_traces(marker_color=colors)
st.plotly_chart(fig)

# =================================================================================================

st.divider()

st.subheader('Are there differences in the IMDB rating between different genres?')

df_ratings = df_genres[['Genres', 'IMDBRating']]
ratings_mean = pd.read_parquet(dir + '/ratings_mean.parquet')

fig = px.box(df_ratings, x='Genres', y='IMDBRating')
fig.update_xaxes(categoryorder='array', categoryarray=ratings_mean.index)
st.plotly_chart(fig)

st.write(
    f'''
    A few notes:
    - As we can see, :yellow[genres are rated very differently] with :yellow[medians ranging] from :yellow[{ratings_mean.iloc[1, 0]:.2f}] to :yellow[{ratings_mean.iloc[-1, 0]:.2f}].
    - :yellow[{ratings_mean.index[0]}] is the :yellow[highest rated genre] with a median of :yellow[{ratings_mean.iloc[0, 0]:.2f} followed by {ratings_mean.index[1]}]
    with a median of :yellow[{ratings_mean.iloc[1, 0]}].
    - :yellow[{ratings_mean.index[-1]}] is the :yellow[lowest rated genre] with a median of :yellow[{ratings_mean.iloc[-1, 0]:.2f}] with a :yellow[rather large gap]
    to :yellow[{ratings_mean.index[-2]}] with a median of :yellow[{ratings_mean.iloc[-2, 0]:.2f}]. One could argue that many horror titles are also classified as
    thriller, Sci-Fi and mystery so that these genres are influenced by the same titles.
    - :yellow[Film-Noir] has a :yellow[very narrow distribution] as there are :yellow[very few Film-Noir titles].
    - Interestingly, News has the broadest distribution, despite having the fewest titles.
    '''
)

# =================================================================================================

st.divider()

st.subheader('How has the release of genres changed over time?')

st.write(
    '''
    In this, we :yellow[focus on the most interesting genres]. Since depicting all at once is quite busy, below you can check genres to be shown.
    In the plot below, for each year, we depict the number of titles of a specific genre divided by the total number of movies released.
    So, we :yellow[show how much] (in %) a :yellow[specific genre] has :yellow[contributed to the overall (movie) industry] in any :yellow[given year].
    '''
)

options = ['Action', 'Family', 'Film-Noir', 'Horror', 'Musical', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
cols = st.columns(len(options))
genres_to_display = []
for i, (col, opt) in enumerate(zip(cols, options)):
    if col.checkbox(opt, value=True if i == 0 else False, key='genres' + str(opt)):
        genres_to_display.append(opt)

df_years = pd.read_parquet(dir + '/df_years.parquet')
df_years = df_years[df_years['Genres'].isin(genres_to_display)]

fig = px.line(df_years, x='StartYear', y='Count', color='Genres')
fig.update_xaxes(range=[1870, 2030], tickmode='array', tickvals=list(range(1870, 2030, 10)))
st.plotly_chart(fig)

st.write(
    '''
    A few notes:
    - Again, the :yellow[earlier years] of filmmaking (say up to 1920) are :yellow[very volatile]. These should be taken with a grain of salt.
    On the other hand, the data for the :yellow[most recent year(s)] is :yellow[not complete], so 2026 at least should not be taken at face value either.
    - From this plot, we can see some historic changes in movie production:
        - The :yellow[action genre rose in the 1960s] from approximately 3% to 9%. :yellow[Over the 1980s to 2000s], the action genre has been :yellow[most relevant] hitting
        up to 10% in 1994.
        - The :yellow[family genre hit a plateau] around 8% from :yellow[1930 to mid-1950] (probably in the wake of Disneys golden age) and has been
        :yellow[declining ever since] to approximately 2%.
        - :yellow[Film-Noir], according to the classification of the IMDB, were :yellow[only produced] between :yellow[1927 and 1958] (perhaps, this correlates to
        the introduction of color TV). As such, the history of Film-Noir is quite volatile with a peak around 5% in 1947.
        - The :yellow[horror genre] has a :yellow[volatile history] too.
        In the :yellow[early years] [pre-1930](https://en.wikipedia.org/wiki/History_of_horror_films#Early_film), we see a :yellow[plateau] around 3%. In this experimental era,
        many different countries released their own versions of nowadays well-known horror characters, for example adaptions of Dorian Grey in Denmark (1910), Russia (1915),
        Germany (1918), Hungary (1918) or the famous german "Nosferatu" (1922) adaption of Dracula. In that wake, many German expressionist movies are also classified as
        horror movies.  
        Interestingly, the :yellow[golden age of classic (Universal) monster movies] (approximately from 1930 to mid-1950) is accompanied by a :yellow[damp of releases].  
        In the :yellow[1960s], the genre :yellow[rose in popularity to a plateau] of around 5% from 1970-1990. Starting with the Hitchcock classic Psycho in 1960,
        the 1970s and 1980s were :yellow[dominated by] the :yellow[zombie] genre (Romero), :yellow[body horror] (Carpenter, Cronenberg) and :yellow[slasher movies]
        (Texas Chainsaw Massacre, Halloween) and :yellow[major contributions from other countries] like Argento from Italy. Afterwards, between :yellow[1990 to mid-2000],
        the :yellow[genre declined] while its popularity :yellow[rose again] to/slightly beyond 5% :yellow[after mid-2000] with titles like "Blair Witch Project", "Scream",
        "Paranormal Activity", etc.
        - The :yellow[musical genre] was :yellow[basically introduced in 1930] with a peak of around 8% and has been :yellow[slowly declining ever since] down to 0.3%
        in the recent years.
        - :yellow[Romances] were :yellow[most popular] between :yellow[1920 and mid-1940] with up to 17% and has been :yellow[quite constant since] then. Around the 2000,
        they slightly rose in popularity again.
        - Disregarding the early years of filmmaking pre-1930, the :yellow[Sci-Fi genre peaked in 1950]. One can :yellow[maybe identify a plateau from 1980 to 2000]
        where classics such as "Star Wars", "Blade Runner" and "Alien" strongly influenced the genre. :yellow[Recent years] (even without 2026)
        :yellow[seem to indicate hard times] for Sci-Fi titles.
        - The :yellow[thriller genre] has overall :yellow[gained in popularity]. While being around 1% over many decades from 1910 to mid-1950, the genre increased to
        roughly 8% in recent years perhaps with an earlier plateau in the 1990s around 5%.
        - :yellow[War movies could be seen an interesting mirror to historical events]. Again, since the early years are very volatile, it is difficult to extract something
        relevant (for WW1) here. :yellow[During WW2], however, from 1939-1945, we clearly see a :yellow[large spike in the genre]. This spike is probably constituted by
        many :yellow[propaganda movies]. Over the course of a decade, we see an increase of war movies with a peak around 4% in 1960, probably :yellow[processing the events
        of WW2]. :yellow[Afterwards], the genre :yellow[lost popularity] and suprisingly, :yellow[historic events can hardly be resolved]. Maybe one can attribute
        the :yellow[slight increase] between :yellow[mid-1970 to 1990] to movies :yellow[processing the Vietnam war] and the :yellow[tiny spike in 2004] to
        :yellow[processing 9/11] and the :yellow[following Iraq war].
        - The [history of western movies](https://en.wikipedia.org/wiki/Western_film#History) and its periods of popularity can be clearly seen.
        In the "silent-film era" from :yellow[1910 to (mid-)1920], :yellow[westerns] were :yellow[pretty popular] and entered the :yellow[golden age of classic (mostly American)
        westerns] from :yellow[1940 to 1960]. Afterwards, from :yellow[mid-1960 to mid-1970], the genre :yellow[experienced a revival] mostly due to
        :yellow[Italys spaghetti westerns], but has been :yellow[almost non-existent ever since] down to 0.2% over the last several decades :yellow[even below musicals].
    '''
)
