# -*- coding: utf-8 -*-

import os

import pandas as pd

from ..application.collect_data.get_data import (get_movie_description,
                                                 get_movie_imdb_id,
                                                 get_raw_data,
                                                 get_title_movies,
                                                 transtorm_dict_dataframe,
                                                 main)
from .responses import (get_response_movie_description_return,
                        get_response_movie_imdb_id_return)

requisitions_number = 2
raw_data = pd.read_csv("data/raw/movies.csv", sep=",")
movie_list = ['The Shawshank Redemption', 'The Dark Knight']
test_path = 'data/processed/test.csv'
original_path = 'data/processed/processed.csv'


get_raw_data_return = raw_data.columns
get_title_movies_return = raw_data['Tittle'].tolist()[:requisitions_number]
get_movie_imdb_id_return = get_response_movie_imdb_id_return()
get_movie_description_return = get_response_movie_description_return()


def test_get_raw_data():
    df = get_raw_data()
    columns = df.columns
    assert len(columns) == len(get_raw_data_return)


def test_get_title_movies():
    movies_titles = get_title_movies(raw_data, requisitions_number)
    assert len(movies_titles) == len(get_title_movies_return)


def test_get_movie_imdb_id():
    imbd_ids = get_movie_imdb_id(movie_list)
    assert imbd_ids == get_movie_imdb_id_return


def test_get_movie_description():
    movie_information = get_movie_description(get_movie_imdb_id_return)
    assert movie_information == get_movie_description_return


def test_transform_dict_dataframe():
    transtorm_dict_dataframe(
        get_movie_description_return, original_path)
    assert False


def test_transform_dict_dataframe():
    try:
        os.remove(test_path)
    except:
        pass
    transtorm_dict_dataframe(
        get_movie_description_return, test_path)
    assert True


def test_main():
    main(test_path)
    assert True


def test_main():
    main(original_path)
    assert False
