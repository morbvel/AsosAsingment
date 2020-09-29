package org.asos.interview.utils

trait Constants {
  val RATINGS_FILE_PATH: String = "/ratings.dat"
  val MOVIES_FILE_PATH: String = "/movies.dat"

  val ELEMENT_TO_SPLIT: String = "::"

  val MOVIES_RATINGS_USERS_FILENAME: String = "/movies_ratings_users.csv"
  val GENDERS_MOVIES_FILENAME: String = "/genders_movies.csv"
  val RANKING_MOVIES_RATINGS_FILENAME: String = "/ranking_movies_ratings_filename.parquet"
}
