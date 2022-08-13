# -*- coding: utf-8 -*-

import glob
import json
import logging
import sys

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

f = open('application/collect_data/credentials.json')
credentials = json.load(f)
api_key = credentials['apikey']


def get_raw_data() -> pd.DataFrame:
    """
    Function that returns movies raw data path

    Returns: 
        DataFrame
    """
    return pd.read_csv("data/raw/movies.csv", sep=",")


def get_title_movies(df: pd.DataFrame, n: int) -> list:
    """
    Function that gets title movies from dataframe

    Arguments:
        df (dataframe): Movie dataframe
        n (int): Number of films to return
    Returns:
        A list of movie titles
    """
    return df["Tittle"].tolist()[:n]


def get_movie_imdb_id(title: list) -> dict:
    """
    Function that gets imbd id movies from omdbapi API

    Arguments:
        titles (list): Movie title
    Returns:
        A dict with movies description
    """
    id_movie = {}
    try:
        logging.info('Requesting movies id...')
        for text in title:
            response = requests.get(
                f"https://www.omdbapi.com/?s={text}&type=movie&apikey={api_key}")

            movie_details = json.loads(response.text)
            if text == movie_details['Search'][0]['Title']:
                id_movie[movie_details['Search'][0]['imdbID']] = text
            else:
                id_movie[movie_details['Search'][0]['imdbID']] = None

        return id_movie

    except Exception as error:
        logging.error(f"Request error: {error}")
        raise ValueError('Request error: {error}')


def get_movie_description(details: dict) -> dict:
    """
    Function that gets movie description from omdbapi API

    Arguments:
        details (dict): A dict with movies name and ids
    Returns:
        dict: dict with movies id, name and description
    """
    movie_information = {}
    logging.info('Requesting movies description...')
    try:

        for id in details:
            response = requests.get(
                f"https://www.omdbapi.com/?i={id}&apikey={api_key}")
            movie_details = json.loads(response.text)
            movie_information[id] = movie_details
        return movie_information

    except Exception as error:
        logging.error(f"Request error: {error}")
        raise ValueError('Request error: {error}')


def transtorm_dict_dataframe(complete_information: dict, path: str) -> None:
    """
    Function that transforms dict information in pandas dataframe

    Arguments:
        complete_information(dict): A dict with movies name and ids.
        path (str): A path where the script will write the file.
    Returns:
        dataframe: Processed dataframe with details about movies
    """

    dataframe = pd.DataFrame.from_dict(
        complete_information, orient='index').reset_index()

    files_present = glob.glob(path)

    write = False

    if not files_present:
        logging.info('Saving processed file.')
        dataframe.to_csv(path, sep=",")
        write = True

    return write


def main(path) -> None:

    df = get_raw_data()
    titles = get_title_movies(df, 10)
    imdb_ids = get_movie_imdb_id(titles)
    complete_information = get_movie_description(imdb_ids)
    write = transtorm_dict_dataframe(complete_information, path)
    logging.info(f'The file was saved processed folder: {write}')


if __name__ == "__main__":
    path = 'data/processed/processed.csv'
    main(path)
