# IMDB Data Analysis Readme

This project consists of two parts: A data analysis part which downloads and prepares media data from the IMDB and the TMDB to create an interactive EDA dashboard. The second part introduces two machine learning algorithm to try to predict some movies IMDB rating and the number of its IMDB votes. The latter part only consists of the 'machine_learning.ipynb' notebook.

## Movie Analytics Dashboard

Interactive Streamlit app for movie, series data analysis including analysis of IMDB ratings, genres, countries of origin, people involved in production and budget/revenue using IMDB and TMDB data.

---

### Features

- Automated pipeline to
  - download public IMDB datasets
  - build a duckdb database
  - add further data from TMDB via their API
  - clean and prepare the data for use in the streamlit app
- Interactive dashboard-style streamlit app for visual exploration

### Live Demo

A fully functional version of the dashboard is deployed on **Streamlit Cloud** via [open the app](https://imdb-data-analysis-dnsa6sktqtkiq3uvss6n2c.streamlit.app/)

The deployed version uses a smaller subset of data (titles with at least 10.000 IMDB votes) for performance reasons of the cloud environment.
As such, some features and the written text to not match, work or make sense, but it gives an impression of the functionality etc.
If you want the analysis for the full dataset (titles with at least 100 IMDB votes - to somewhat filter for relevance), you need to execute the data pipeline (which takes a few hours due to the amount of TMDB API calls).

---

### Installation & Setup

```bash
cd 'YOUR_PROJECT_PATH'
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# python -m src.prepare_all_data  # Execute only if you want to download all data!
streamlit run streamlit_app/Home.py
```

---

### API Keys

Executing the pipeline to download the full dataset requires a TMDB API key which needs to be saved in a '.env' file via

```
TMDB_API_KEY=YOUR_API_KEY
```

---

### Acknowledgments

- IMDB for providing [public datasets](https://developer.imdb.com/non-commercial-datasets/)
- TMDB for their [API](https://developer.themoviedb.org/docs/getting-started)


## Machine Learning Jupyter Notebook

The 'machine_learning.ipynb' notebook uses a small subset of movies (only movies!) with at least 10.000 IMDB votes created and stored in 'processed_data/' during the data pipeline. It then trains both a random forest regressor and an XGBoost regressor on this dataset of approximately 12.000 movie titles.
