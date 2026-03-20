import streamlit as st
import pycountry
import langcodes


# =================================================================================================

'''File containing several functions often called in the streamlit app.'''

def votes_slider(key):
    '''Creation of a slider to set minimum IMDB Votes.'''
    return st.slider('Minimum number of IMDB Votes', 100, 20000, value=100, step=100, key=key)


def type_select(key, default=[True, True, True, True, True]):
    '''Creation of a multiselect to select Types. Also returns a list of selected Types.'''
    options = ['Movies', 'Series', 'Episodes', 'Games', 'Shorts']
    cols = st.columns(len(options))
    selected = []
    for i, (col, opt) in enumerate(zip(cols, options)):
        if col.checkbox(opt, value=default[i], key=key + '_' + opt):
            selected.append(opt)

    return selected


def lang_name(code):
    '''Given an ISO 639-1 language code, e.g. 'en' for english, return more readable name of the language, e.g. 'English'.'''
    try:
        return langcodes.Language.get(code).display_name()
    except langcodes.LanguageTagError:
        return code


def country_name(code):
    '''Given an ISO 3166-1 alpha-2 country code, e.g. 'US' for the United States, return more readable name of the language, e.g. 'United States'.'''
    country = pycountry.countries.get(alpha_2=code)
    return country.name if country else code


def make_hovertext(movie_list):
    lines = [f'{title} ({metric:.1f})' for title, metric in movie_list[:10]]
    return '<br>'.join(lines)
