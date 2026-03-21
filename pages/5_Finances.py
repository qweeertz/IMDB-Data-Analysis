import streamlit as st
import pandas as pd
import plotly.express as px


# =================================================================================================

@st.cache_data
def prepare_exploded(df):
    roles = ['Directors', 'Actors', 'Actresses']
    exploded = {}
    for role in roles:
        exploded[role] = df[['PrimaryTitle', 'Budget', 'Revenue', 'ROI', role]].dropna(subset=[role]).explode(role)
    return exploded

@st.cache_data
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

dir = 'processed_data/5_Finances'

# =================================================================================================

st.title('Analysis of some financial aspects')

df = st.session_state.df

df_filtered = df[(df['Type'] == 'Movies') & (df['IMDBVotes'] >= 10000)]
budget_filter, revenue_filter = (df_filtered['Budget'] >= 1000), (df_filtered['Revenue'] >= 1000)
df_budget = df_filtered.loc[budget_filter, ['tconst', 'PrimaryTitle', 'Budget']].sort_values('Budget', ascending=False)
df_revenue = df_filtered.loc[revenue_filter, ['tconst', 'PrimaryTitle', 'Revenue']].sort_values('Revenue', ascending=False)
df_roi = pd.read_parquet(dir + '/df_roi.parquet')

st.write(
    '''
    _Disclaimer_: The :yellow[numbers] presented in this section are :yellow[from the TMDB]. These numbers, revenue and (more so) budget, :yellow[have a few problems] which we
    explain right below and should be taken to be 100% precise, however, they serve both as an exercise and as a :yellow[reference point]. To make the data more valid,
    we :yellow[only consider movies] (as the TMDB is most complete there), consider only titles with :yellow[at least 10.000 IMDB votes] and, in addition, consider only titles
    with a :yellow[budget and revenue of at least 1.000 US\\$]. Titles that have a budget or revenue of 5 US\\$ are obviously incorrect. With these filters, the IMDB and TMDB
    data mostly agrees. Still, :yellow[some problems remain], among which
    - The :yellow[budget numbers highly depend on what is being counted]. Whether :yellow[marketing], :yellow[reshoots] or :yellow[other expenses] is included is not at all
    transparent. Often, the IMDB (and the TMDB) seem to list the production budget which excludes marketing costs which can be rather high (even more or less as high as the
    production budget itself). [Officially](https://help.imdb.com/article/imdbpro/industry-research/box-office-by-imdbpro-glossary/GN8HA87MT4597FSW#productionbudget),
    the IMDB states that the :yellow[budget numbers are production budgets only], but this is :yellow[somewhat questionable] (see example Star Wars below).  
    The :yellow[revenue numbers] that the IMDB (and usually TMDB) lists :yellow[only refers to the theatrical box office earnings]. Home entertainment sales, rentals,
    television rights, etc are [officially](https://help.imdb.com/article/imdbpro/industry-research/box-office-by-imdbpro-glossary/GN8HA87MT4597FSW#boxofficetracking) excluded.
    - Another limitation is that :yellow[movies (especially nowadays) may not have a classical release], but are released to a streaming platform directly.
    As such, :yellow[they do not have classical box office numbers] (i.e. a revenue). Yet, sometimes, both the IMDB and the TMDB list some small number (e.g. a couple thousand
    US\\$) for such titles distorting the profit/ROI numbers. For example, the movie "Hellraiser" (2022) is listed with a budget of 14 Million US\\$ and a revenue of roughly
    10.000 US\\$. Its profit is thus -13.990.000 US\\$ and its ROI is roughly 0.0007.  
    We want to emphasize that this is not really the fault of the TMDB (or IMDB) since studios are somewhat secretive about the exact numbers of budget, in particular marketing
    expenditures, etc.
    - Furthermore, the numbers are :yellow[not inflation-adjusted]. Thus, both the budget and the revenue for older titles are difficult to compare.
    - Right below, we see that we are :yellow[left with approximately 8.000 titles] that we have a budget for, 9.000 titles that we have a revenue for and an overlap between
    the two for close to 8.000 titles for which we can calculate the profit and the ROI (see below). Of course, this is a :yellow[very small part of the original (movie) data],
    :yellow[but the "major players" are included] and we do get into the vicinity of more complete estimations (see below).
    '''
)

# =================================================================================================

st.divider()

col1, col2, col3 = st.columns(3)
col1.metric('Number of titles with budget available', len(df_budget))
col2.metric('Number of titles with revenue available', len(df_revenue))
col3.metric('Number of titles with both budget and revenue available', len(df_roi))

st.divider()

st.markdown('#### A few definitions')

st.markdown(
    r'''
    - The :yellow[**Profit**] is simply the difference between the revenue and the budget, i.e. $\text{Profit}=\text{Revenue}-\text{Budget}$.
    - The :yellow[**Return of Investment (ROI)**] is given by the ratio of the revenue and the budget of a movie, i.e. $\text{ROI}=\frac{\text{Revenue}}{\text{Budget}}$.
    Intuitively speaking, for each (unit of currency) you invest, you get ROI (units of currencies) back. Thus, if the ROI is 1, you neither gain, nor lose money,
    if ROI < 1, you lose money and if ROI > 1, you gain money.
    '''
)

# =================================================================================================

st.divider()

st.header('What is the general distribution?')

st.write(
    '''
    The table below shows an :yellow[excerpt of 50 titles] (sorted by highest revenue) for which we have both the budget and the revenue so we can calculate the profit
    and the ROI for it.
    '''
)

df_top_profit = df_roi[['PrimaryTitle', 'Budget', 'Revenue', 'Profit', 'ROI']].sort_values('Revenue', ascending=False).head(50)
df_top_profit_display = (df_top_profit.style.format(precision=0, thousands='.', decimal=',', subset=['Budget', 'Revenue', 'Profit'])
                         .format(precision=3, thousands='.', decimal=',', subset=['ROI']))
st.dataframe(df_top_profit_display, hide_index=True)

st.write(
    '''
    A few notes:
    - Both concerning revenue and profit, :yellow[Avatar] and :yellow[Avengers Endgame] are the :yellow[highest grossing movies ever released].
    The third place depends on the metric: Due to its relatively low budget, Ne Zha 2 takes the third spot concerning profit, while
    Avatar: The Way of Water has the third highest Revenue.
    - As mentioned in the beginning, the :yellow[numbers for very low revenues are essentially wrong]. These usually are movies directly released to streaming services
    that do not have a classical revenue. This distorts both the revenue and the ROI in this table.
    - In the IMDB/TMDB data, :yellow[Star Wars Episode IX: The Rise of Skywalker] is the movie with the :yellow[highest budget] of :yellow[around 490 Million US\\$].
    Often, :yellow[Star Wars Episode VII: The Force Awakens] is :yellow[listed as the most expensive movie ever made]. :yellow[In our dataset], it is listed with a budget
    of :yellow[245 Million US\\$]. Probably, the :yellow[main reason] is :yellow[how budget is defined here]: For Episode IX, marketing costs, reshoots and others
    seem to be counted while this does not seem to be the case for Episode VII.
    - :yellow[Paranormal Activity] has an incredible :yellow[ROI] of around :yellow[12.890] with the :yellow[Blair Witch Project] at :yellow[second place]
    having an ROI around :yellow[4144] (not seen in this excerpt of the table). These numbers are legit. These :yellow[small horror movies] with very small budgets :yellow[exploded at the box office]
    generating revenues around 200 Million US\\$.
    '''
)

st.write(
    '''
    Below, we see the the :yellow[distribution of budget] (light blue) and :yellow[revenue] (darker blue). As we can see in the standard scale, it is :yellow[heavily skewed].
    Zooming in the region of below 100 million US\\$, we see that there :yellow[many more movies in the lower budget classes] (e.g. around 500 movies with a budget
    below 10 Million US\\$) which is a good sign. Since both distributions account for the same (number of) movies, this means that :yellow[at higher values],
    :yellow[revenue dominates] and the industry makes a profit at all.  
    Switching on the :yellow[logarithmic y-axis], this can be seen very clearly at larger ranges.
    '''
)

log = st.checkbox('Logarithmic y-axis', value=False)
fig = px.histogram(df_roi, x=['Budget', 'Revenue'], barmode='overlay', log_y=log)
fig.update_layout(bargap=0.05, xaxis_title='(US$)', yaxis_title='Count', legend_title_text='')
st.plotly_chart(fig)

# =================================================================================================

st.divider()

st.header('How much money does the industry make?')

col1, col2, col3 = st.columns(3)
col1.metric('Total budget', f'{df_roi['Budget'].sum():,.0f} US$'.replace(',', '.'))
col2.metric('Total revenue', f'{df_roi['Revenue'].sum():,.0f} US$'.replace(',', '.'))
col3.metric('Total profit', f'{df_roi['Profit'].sum():,.0f} US$'.replace(',', '.'))

st.write(
    '''
    Above we see the :yellow[total numbers summed over all years]. Remember, that this :yellow[only] includes the :yellow[approximately 8.000 titles]
    that we have the clean data for and it is :yellow[not inflation-adjusted].  
    :yellow[According to AI agents] (ChatGPT, Gemini and Copilot), the :yellow[total revenue of the movie industry over all years] is in the range of
    :yellow[1-1.3 Trillion US\\$]. Adjusting for inflation, ChatGPT estimates 2.2-2.4 Trillion US\\$, Gemini 3.8-4.5 Trillion US\\$ and Copilot 2.5-3.5 Trillion US\\$.  
    Comparing our number to the non-inflation-adjusted value of 1-1.3 Trillion US\\$, we find that the approximately 8.000 titles we considered accumulated to about
    50-70 % of the total revenue.  
    Again, our numbers do no consistently account for reshoots, marketing, distribution costs, etc. while the revenue also does not include
    home entertainment releases, merchandise, etc.
    '''
)

# =================================================================================================

st.divider()

st.header('How did the (total) budget, revenue and profit evolve over the years?')

df_years = pd.read_parquet(dir + '/df_years.parquet')
fig = px.line(df_years, x='StartYear', y=['Budget', 'Revenue', 'Profit'])
fig.update_layout(bargap=0.05, xaxis_title='Release Year', yaxis_title='(US$)', legend_title_text='')
st.plotly_chart(fig)

st.write(
    '''
    A few notes:
    - The :yellow[general growing trend] is both :yellow[due to inflation and a growing industry].
    - Again, the data for the last years, 2026, is incomplete and should not be considered.
    - In this figure, the :yellow[devastating effects of Covid] become very clear. While the :yellow[budget took a somewhat small hit],
    the :yellow[revenue heavily collapsed] (note that the profit is only at 1.3 Billion US\\$, compared to 22 Billion US\\$ in 2019).
    :yellow[Ever since] (excluding 2026), the industry has been on the :yellow[path of recovery], but :yellow[still is well below pre-Covid].
    - :yellow[Wikipedia lists 2019] with a global revenue of around :yellow[42 Billion US\\$]. The large difference is :yellow[simply due to the movies missing] in our dataset.
    It :yellow[misses] a lot of global releases and with them, :yellow[big parts of local markets] (mainly China and India).
    - Again, note that the :yellow[industry has changed significantly], moving towards :yellow[streaming services]. These are :yellow[not at all considered here].
    '''
)

# =================================================================================================

st.divider()

st.header('Is there a connection between IMDB Rating and Budget/Revenue/ROI?')

col1, col2 = st.columns([0.2, 0.8])
with col1:
    metric = st.selectbox('Metric to depict', ['Budget', 'Revenue', 'ROI'], index=1, key='Rating')

fig = px.scatter(df_roi, x='IMDBRating', y=metric)
if metric == 'ROI':
    fig.update_yaxes(range=[0, 145])
st.plotly_chart(fig)

st.write(
    '''
    The story for :yellow[all three metrics] is :yellow[very similar], though ROI is a bit more messy and the distribution for revenue is a bit more pronounced than
    that of budget. Thus, we :yellow[focus on the revenue]. The main conclusion is that :yellow[poorly rated movies] make :yellow[little revenue] at the box office.
    Those movies with a :yellow[high revenue] tend to be :yellow[rated quite highly]. Of course, there is a :yellow[correlation] here since :yellow[viewers decide what to watch]
    at the theaters. And they tend to watch movies that they anticipate, like or that were recommended to them in some way. Nonetheless, the highly rated movies
    with an IMDB rating of, say 8.0, extend from 9.000 US\\$ up to 2.26 Billion US\\$. In fact, The Shawshank Redemption (1994), the highest rated movie at 9.3,
    made a revenue of around 28 Million US\\$ (not inflation-adjusted - which should not be too much in 1994 though).
    '''
)

# =================================================================================================

st.divider()

st.header('What genres are most expensive to make? What genres generate the highest revenue and offer the highest ROI?')

df_genres = pd.read_parquet(dir + '/df_genres.parquet')

col1, col2 = st.columns([0.2, 0.8])
with col1:
    sort_metric = st.selectbox('Sort genres by', ['Budget', 'Revenue', 'ROI'], index=1, key='sort_metric')
df_genres_sorted = df_genres.sort_values(sort_metric, ascending=False)

col1, col2, col3 = st.columns(3)
with col1:
    fig = px.bar(df_genres_sorted, x='Budget', y='Genres', title='Mean Budget', orientation='h', text='Budget', height=800)
    fig.update_traces(textposition='inside', texttemplate='%{x:,d}', textfont_size=100, textfont=dict(color='black'), textangle=0)
    fig.update_layout(xaxis_title=None, yaxis_title=None)
    fig.update_yaxes(autorange='reversed')
    st.plotly_chart(fig)
with col2:
    fig = px.bar(df_genres_sorted, x='Revenue', y='Genres', title='Mean Revenue', orientation='h', text='Revenue', height=800)
    fig.update_traces(textposition='inside', texttemplate='%{x:,d}', textfont_size=100, textfont=dict(color='black'), textangle=0)
    fig.update_layout(xaxis_title=None, yaxis_title=None)
    fig.update_yaxes(autorange='reversed')
    st.plotly_chart(fig)
with col3:
    fig = px.bar(df_genres_sorted, x='ROI', y='Genres', title='Mean ROI', orientation='h', text='ROI', height=800)
    fig.update_traces(textposition='inside', texttemplate='%{x:.2f}', textfont_size=100, textfont=dict(color='black'), textangle=0)
    fig.update_layout(xaxis_title=None, yaxis_title=None)
    fig.update_yaxes(autorange='reversed')
    st.plotly_chart(fig)

st.write(
    '''
    A few notes:
    - We see that :yellow[budget and revenue] are :yellow[quite closely related]. Up to thriller, i.e. the Top 8, their ranking agrees.
    - Animation movies (Disney, Pixar, etc.) are very prominent in the lists of highest budget and revenue. :yellow[CGI effects] turn out to be :yellow[rather expensive]
    and they sell many tickets.  
    Apart from that, the top lists are :yellow[dominated by huge Blockbuster movies] like Avatar, the MCU, Star Wars etc. All of these are big productions
    (with large CGI budgets) that have :yellow[significant overlap in genres]: Most of them are listed as adventure, action, sci-fi or action movies.
    Thus, :yellow[these genres rank highest] concerning both budget and revenue.
    - The :yellow[ROI metric] is :yellow[very different] and shows a :yellow[clear distinction between 3 genres and the rest]. Here, :yellow[horror] and :yellow[mystery] movies
    (probably with large overlap) :yellow[dominate].
    The most extreme examples are "Paranormal Activity" and "Blair Witch Project" with ROI's of around 12.890 and 4.144.
    :yellow[Horror] movies are :yellow[generally very cheap to make] (Bottom 4 in mean budgets), but :yellow[can explode at the box office].  
    Documentaries also score a unusual high mean ROI of close to 20. Generally speaking, :yellow[documentaries are even cheaper to make] (Bottom 3 in mean budgets)
    than horror movies while this :yellow[effect of exploding] at the box office is :yellow[not as significant]. Still, they achieve good results at the box office,
    relatively speaking.
    '''
)

# =================================================================================================

st.divider()

st.header('Which directors/actors/actresses achieve the best box office results?')

st.write('_Disclaimer_: This takes a couple of seconds.')

st.write(
    '''
    In the ranking below, we filter for :yellow[directors with at least 2 titles] and :yellow[actors]/:yellow[actresses with at least 5 titles] and show the
    :yellow[mean budget]/:yellow[revenue]/:yellow[ROI]. The :yellow[ROI] is :yellow[heavily skewed] (mostly due to Paranormal Activity).
    Here, you should :yellow[hover over names] to get the exact value.
    '''
)

col1, col2 = st.columns([0.2, 0.8])
with col1:
    metric = st.selectbox('Sort genres by', ['Budget', 'Revenue', 'ROI'], index=1, key='people')

role_stats = {
    role: pd.read_parquet(dir + '/df_' + role.lower() + '.parquet')
    for role in ['Directors', 'Actors', 'Actresses']
}

cols = st.columns(3)
for col, role in zip(cols, ['Directors', 'Actors', 'Actresses']):
    with col:
        min_count = 2 if role == 'Directors' else 5
        df_role = role_stats[role].copy()
        df_role = df_role[df_role['Count'] >= min_count]
        df_role = role_stats[role].sort_values('Mean' + metric, ascending=False).head(20)

        df_role['hovertext'] = df_role['Movies'].map(lambda movies: '<br>'.join(movies))

        fig = px.bar(df_role, x='Mean' + metric, y=role, title=role, orientation='h', text='Mean' + metric, height=800,
                     hover_name=role, hover_data={'hovertext': False, 'Mean' + metric: False, role: False})
        fig.update_traces(textposition='inside', texttemplate='%{x:,d}', textfont_size=100, textfont=dict(color='black'), textangle=0,
            customdata=df_role[[role, 'Mean' + metric, 'hovertext']], hovertemplate=(
                '<b>%{customdata[0]}</b><br>'
                'Mean ' + metric + ': %{customdata[1]:,.0f}<br><br>'
                '<i>Movies</i>:<br>%{customdata[2]}'))
        fig.update_layout(xaxis_title=None, yaxis_title=None)
        fig.update_yaxes(autorange='reversed')
        st.plotly_chart(fig, key=role)

st.write(
    '''
    Of course, people rank highest that :yellow[were involved in very high budget/revenue/roi productions], but :yellow[were not involved in too many productions]
    (e.g. Yu Yang with "Ne Zha" & "Ne Zha 2", Chadwick Boseman with "Avengers: Infinity War", "Avengers: Endgame" and "Black Panther" or Idina Menzel with "Frozen"
    and "Frozen 2").
    '''
)
