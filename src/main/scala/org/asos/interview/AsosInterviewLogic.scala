package org.asos.interview

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.asos.interview.utils.AsosInterviewUtils

object AsosInterviewLogic {

  def groupedMoviesUsersAverageRating(ratingsRdd: RDD[(String, String, String)]): RDD[(String, Int, Double)] = {

    val ratingPerMovie: RDD[(String, Double)] = getRatingsPerMovie(ratingsRdd)

    val usersPerMovie: RDD[(String, Int)] = getUsersPerMovie(ratingsRdd)

    usersPerMovie
      .join(ratingPerMovie)
      .map(fullInfo => (fullInfo._1, fullInfo._2._1, fullInfo._2._2))
  }

  def getRatingsPerMovie(ratingsRdd: RDD[(String, String, String)]): RDD[(String, Double)] = {

    ratingsRdd
      .map(
        movies => (movies._1, movies._3.toDouble)
    ).groupByKey().map(
      userMoviesPair => (
        userMoviesPair._1,
        AsosInterviewUtils.getAverage(userMoviesPair._2.toList)
      )
    )
  }

  def getUsersPerMovie(ratingsRdd: RDD[(String, String, String)]): RDD[(String, Int)] = {

    ratingsRdd
      .map(
        movies => (movies._1, movies._2)
      ).distinct().map(
        movies => (movies._1, 1)
      ).groupByKey().map(
        userPair => (userPair._1, userPair._2.sum)
      )
  }

  def groupedMoviesGenres(movies: RDD[(String, String)], spark: SparkSession): RDD[(String, Int)] = {

    val genresPerMovies: RDD[(String, String)] = aggGenresPerMovie(movies, spark)

    genresPerMovies
      .map(row => (row._2, 1))
      .groupByKey()
      .map(row => (row._1, row._2.sum))

  }

  def aggGenresPerMovie(inputRdd: RDD[(String, String)], spark: SparkSession): RDD[(String, String)] = {

    def loop(
      movieId: String,
      listOfGenres: List[String],
      accList: List[(String, String)] = List(("",""))
    ): List[(String, String)] = {
      listOfGenres match{
        case Nil => accList
        case element :: tail => loop(movieId, tail, accList ::: List((movieId, element)) )
      }
    }

    inputRdd
      .map(row => (row._1, row._2.split("\\|")) )
      .flatMap(element => loop(element._1, element._2.toList))
      .filter(_._2 != "")

  }

  def getChartOfMoviesByRating(
    ratings: RDD[(String, String, String)],
    movies: RDD[(String, String)]
  ): RDD[(String, String, Double, Long)] = {

    val averageRatingByMovie: RDD[(String, Double)] = getRatingsPerMovie(ratings)

    val moviesWithRatings: RDD[(String, (String, Double))] = movies.join(averageRatingByMovie)

    moviesWithRatings
      .sortBy(_._2._2, false)
      .zipWithIndex()
      .map(row => (row._1._1, row._1._2._1, row._1._2._2, row._2 + 1))
  }

}
