import streamlit as st
import plotly.express as px
import pandas as pd
from utils.helper_functions import country_name


# =================================================================================================

dir = 'processed_data/4_People'

# =================================================================================================

st.title('Analysis of people involved in the production')

st.write(
    '''
    _Disclaimer_: Many people are involved in the production of movies, series, games, etc. Since the production of games (and shorts) is probably (quite) disjoint from the rest,
    we :yellow[solely focus] on :yellow[movies] and :yellow[series] (episodes will be excluded as well since everything should be captured by series already).  
    In addition, below we set the :yellow[minimum number of IMDB votes] to :yellow[25.000] to somewhat :yellow[filter for relevance] in agreement with IMDBs requirement to be listed in
    their Top 250 (Movies) list.  
    You can :yellow[choose a tab] to display directors, writers, actors, actresses, cinematographers and composers and below the top lists, view statistics for individual people.  
    The last tab, :yellow["Graph"], shows a network graph
    of people involved in the industry. This should be considered as a playful tool (and a good exercise for me) and not a very useful metric.
    '''
)

col1, col2 = st.columns([1, 3])
with col1:
    N_people = st.number_input('Number of people considered', min_value=10, max_value=200, step=5, value=20)

roles = ['Directors', 'Writers', 'Actors', 'Actresses', 'Cinematographers', 'Composers', 'Graph']

tabs = st.tabs(roles)

for r, role in enumerate(roles):

    with tabs[r]:

        if not role == 'Graph':

            # =====================================================================================

            st.subheader(f'Who are the most active {role.lower()} and who has the highest average IMDB rating (based on titles they worked on)?')

            st.write(
                f'''
                We consider the :yellow[{N_people} {role.lower()}] with :yellow[at least 4 titles] to their name to avoid skewed statistics.
                You can :yellow[hover] over each name to get the :yellow[10 highest rated/voted titles] from each person.
                The central plot, showing average IMDB votes, serves as an :yellow[indicator for popularity] (inside the IMDB community).
                '''
            )

            df = pd.read_parquet(dir + '/df_' + role.lower() + '.parquet')
            df_titles = pd.read_parquet(dir + '/df_titles_' + role.lower() + '.parquet')

            active, popular, rating = st.columns(3)

            with active:
                fig = px.bar(df_titles.sort_values('Count', ascending=False).head(N_people), x='Count', y=role, orientation='h', text='Count', height=40 * N_people,
                             hover_name=role, hover_data={'hovertext_rating': True, 'MeanRating': False, role: False})
                fig.update_traces(textposition='inside', textfont_size=100, textfont=dict(color='black'), textangle=0, customdata=df_titles.sort_values('Count', ascending=False).head(N_people)[[role, 'MeanRating', 'hovertext_rating']],
                                  hovertemplate='<b>%{customdata[0]}</b><br>' +
                                  'Average Rating: %{customdata[1]:.2f}<br><br>' +
                                  '<i>Top 10 Movies</i>:<br>%{customdata[2]}')
                fig.update_yaxes(autorange='reversed')
                fig.update_layout(xaxis_title='Movie Count', yaxis_title='')

                st.plotly_chart(fig)

            with popular:
                fig = px.bar(df_titles.sort_values('MeanVotes', ascending=False).head(N_people), x='MeanVotes', y=role, orientation='h', text='MeanVotes',
                             height=40 * N_people, hover_name=role, hover_data={'hovertext_votes': True, 'MeanVotes': False, role: False})
                fig.update_traces(textposition='inside', textfont_size=100, textfont=dict(color='black'), textangle=0, texttemplate='%{x:.0f}',
                                  customdata=df_titles.sort_values('MeanVotes', ascending=False).head(N_people)[[role, 'MeanVotes', 'hovertext_votes']],
                                  hovertemplate='<b>%{customdata[0]}</b><br>' +
                                  'Average votes: %{customdata[1]:.0f}<br><br>' +
                                  '<i>Top 10 Movies</i>:<br>%{customdata[2]}')
                fig.update_yaxes(autorange='reversed')
                fig.update_layout(xaxis_title='Average IMDB votes', yaxis_title='')
                st.plotly_chart(fig)

            with rating:
                fig = px.bar(df_titles.sort_values('MeanRating', ascending=False).head(N_people), x='MeanRating', y=role, orientation='h', text='MeanRating',
                             height=40 * N_people, hover_name=role, hover_data={'Count': True, 'hovertext_rating': True, 'MeanRating': False, role: False})
                fig.update_traces(textposition='inside', textfont_size=100, textfont=dict(color='black'), textangle=0, texttemplate='%{x:.2f}',
                                  customdata=df_titles.sort_values('MeanRating', ascending=False).head(N_people)[[role, 'Count', 'hovertext_rating']],
                                  hovertemplate='<b>%{customdata[0]}</b><br>' +
                                  'Count: %{customdata[1]}<br><br>' +
                                  '<i>Top 10 Movies</i>:<br>%{customdata[2]}')
                fig.update_yaxes(autorange='reversed')
                fig.update_layout(xaxis_title='Average IMDB rating', yaxis_title='')
                st.plotly_chart(fig)

            # =====================================================================================

            st.divider()

            st.subheader('What are statistics about individual ' + role.lower() + '?')

            col1, col2 = st.columns([1, 3])
            with col1:
                names = df_titles.sort_values('Count', ascending=False)[role].unique()
                name = st.selectbox('Choose a ' + role[:-1] + ' (can also type):', names, index=0, key=role[:-1] + '_name')

            df_name = df[df[role] == name]

            genres = df_name['Genres'].explode('Genres').value_counts().reset_index()
            countries = df_name['OriginCountry'].explode('OriginCountry').value_counts().reset_index()
            countries['OriginCountry'] = countries['OriginCountry'].map(country_name)

            col1, col2, col3, col4, col5, col6 = st.columns([1, 1, 1, 1, 1, 1])
            col1.metric('Number of movies', df_name.shape[0])
            col2.metric('Average IMDB rating', round(df_name['IMDBRating'].mean(), 2))
            col3.metric('Number of genres', genres.shape[0])
            col4.metric('Favorite genre', genres.iloc[0, 0])
            col5.metric('Number of countries', countries.shape[0])
            col6.metric('Favorite country', countries.iloc[0, 0])

            fig = px.scatter(df_name, x='StartYear', y='IMDBRating', hover_data={'PrimaryTitle': True}, title='Timeline')
            fig.update_traces(marker=dict(size=12), customdata=df_name['PrimaryTitle'], hovertemplate='<b>%{customdata}</b><br>Year: %{x}<br>IMDB Rating: %{y:.1f}')
            fig.update_xaxes(range=[1920, 2030], tickmode='array', tickvals=list(range(1920, 2030, 10)))
            st.plotly_chart(fig)

            col1, col2, col3 = st.columns(3)
            with col1:
                fig = px.pie(genres, names='Genres', values='count', title='Genres')
                st.plotly_chart(fig)
            with col2:
                fig = px.pie(countries, names='OriginCountry', values='count', title='Production countries')
                st.plotly_chart(fig)
            with col3:
                fig = px.box(df_name, y='IMDBRating', title='IMDB Rating')
                fig.update_yaxes(range=[2, 10])
                st.plotly_chart(fig)

        # =========================================================================================

        elif role == 'Graph':

            st.subheader('Network')

            st.write(
                '''
                Below, we show a :yellow[network graph] of the :yellow[Top 150 Directors], :yellow[Top 200 Actors & Actresses] of movies (ranked by number of movies) with
                :yellow[at least 25000 IMDB votes]. Due to the sheer mass of people involved in movie productions and their high interconnectivity, we had to heavily filter
                the existing data. Still, the graph has **a lot** of edges which makes it a bit hard to read.
                '''
            )

            st.write(
                '''
                _Disclaimer_: There are multiple things about this graph that one can improve on which can be rather tedious and advanced to do. Nonetheless, as it was a fun
                and insightful task, I include the result here as it is.
                '''
            )

            with open(dir + '/Network.html', 'r', encoding='utf-8') as f:
                html = f.read()

            wrapped_html = f'''
                <div style='background-color:#222222; margin:0; padding:0;'>
                    {html}
                </div>
            '''
            st.components.v1.html(wrapped_html, height=760, scrolling=False)

            st.write(
                '''
                A few notes and tips:
                - :yellow[People form the nodes] (:yellow[yellow: directors, blue: actors, red: actresses]). If :yellow[two people] worked on the same project together, they are
                :yellow[connected by an edge] whose :yellow[color is the mix of the two colors] correpsonding to the two people. E.g. if two actors worked together, their node
                is red, but if an actor worked with a director, their node is a mix of blue and yellow, i.e. green. The other pairs are (actress, director) = orange and
                (actor, actress) = purple.
                - You can :yellow[zoom in] and :yellow[click on a person] to highlight their edges. In principle, hovering an edge reveals the title(s) the two people worked on
                together, but due to the mass of edges and overlap of these, this is basically impossible and should not be trusted at all.
                - The graph uses the :yellow["Fruchterman-Reingold" force-directed algorithm] to produce the :yellow[graph's layout].
                This layout is inspired by physics where :yellow[nodes] (directors, etc.) :yellow[repel each other] and :yellow[edges act like springs], :yellow[pulling them
                together]. Depending on the strengths of the repelling nodes and the springs, the algorithm finds a balanced state which is shown above.
                - This explains the basic structure of the graph:
                    - We can see that yellow dots, i.e. :yellow[directors form an outer circle]. :yellow[Directors], compared to actors and actresses,
                    :yellow[usually produce much less movies]. As a result, they are :yellow[less connected to the network] and have less edges (springs) that pull them into the
                    network.  
                    :yellow[Some directors are somewhat close to the center], like Steven Spielberg, which means that they directed a lot movies where a lot of
                    actors & actresses (that are in the network!) played a role in. Other such directors are Ron Howard, Martin Scorsese or Steven Soderbergh.
                    - The :yellow[inner circle] is :yellow[quite nicely mixed] of both :yellow[actors and actresses]. That means that men and women are :yellow[both quite well
                    connected], but of course, movies tend to have both male and female roles. Still, there may be a :yellow[slight bias] between the
                    :yellow[upper and lower half], where actors and actresses might have a slight edge.
                    - There are :yellow[a few completely disconnected nodes] like Akira Kurosawa. As collaborations between two or more directors on one single movies are
                    kind of rare, directors can essentially only be connected to the other nodes, i.e. actors and actresses. Since Kurosawas movies are a) quite old and b)
                    with a completely japanese cast, he is isolated, because those japanese actors and actresses that played characters in his movies, do not appear in this
                    network, because they do not appear in enough movies to be in the Top 200 actors/actresses ranked based on the number of movies that they played in.  
                    Another example is the japanese actress Megumi Hayashibara who is a voice actress and thus disconnected.
                    - One :yellow[would expect somewhat isolated regions of specific countries], e.g. an "island" of people working in the indian movie industry. However,
                    since the movie industry still is dominated by US-american productions, there :yellow[simply are not enough people in this network] from other countries
                    that could :yellow[form these "islands"]. Also, :yellow[filtering] for movies with at least 25000 IMDB votes :yellow[cuts out a substantial titles from
                    other countries]. Remember, that we had to do something like that to handle the vast amount of data.
                '''
            )
