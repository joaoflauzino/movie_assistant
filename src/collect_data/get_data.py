import json
import pdb

import pandas as pd
import requests

f = open('src/collect_data/credentials.json')
credentials = json.load(f)
api_key = credentials['apikey']


def read_raw_data() -> pd.DataFrame:
    """
    Function that returns movies raw data path
    return: DataFrame
    """
    return pd.read_csv("data/raw/movies.csv", sep=",")


def get_title_movies(df: pd.DataFrame, n: int) -> list:
    """
    Function that gets title movies from dataframe

    Arguments:
        df: Movie dataframe
        n: Number of films to return
    Returns:
        A list of movie titles
    """
    return df["Tittle"].tolist()[:n]


def request_movie_imdb_id(title: list) -> dict:
    """
    Function that gets imbd id movies from omdbapi API

    Arguments:
        titles: Movie title
    Returns:
        A dict with movies description
    """
    id_movie = {}
    for text in title:
        response = requests.get(
            f"https://www.omdbapi.com/?s={text}&type=movie&apikey={api_key}")

        movie_details = json.loads(response.text)
        if text == movie_details['Search'][0]['Title']:
            id_movie[movie_details['Search'][0]['imdbID']] = text
        else:
            id_movie[movie_details['Search'][0]['imdbID']] = None
    return id_movie


def request_movie_description(details: dict) -> dict:
    """
    Function that gets movie description from omdbapi API

    Arguments:
        details: A dict with movies name and ids
    Returns:
        a dict with movies id, name and description
    """
    movie_information = {}
    for id in details:
        response = requests.get(
            f"https://www.omdbapi.com/?i={id}&apikey={api_key}")
        movie_details = json.loads(response.text)
        movie_information[id] = movie_details
    return movie_information


def main() -> None:
    df = read_raw_data()
    titles = get_title_movies(df, 10)
    imdb_ids = request_movie_imdb_id(titles)
    complete_information = request_movie_description(imdb_ids)
    dataframe = pd.DataFrame.from_dict(
        complete_information, orient='index').reset_index()
    dataframe.to_csv('data/processed/processed.csv', sep=",")


if __name__ == "__main__":
    main()
