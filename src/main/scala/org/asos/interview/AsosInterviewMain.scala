package org.asos.interview

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.asos.interview.utils.{AsosInterviewUtils, Constants}

object AsosInterviewMain extends App with Constants{

  val finalDirectory = if(args.length != 1) throw new Exception("Needed one parameter: \n\tDirectory to create CSV") else args(0)

  val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
  import spark.implicits._

  val ratingsRdd: RDD[String] = AsosInterviewUtils.getSourceData(RATINGS_FILE_PATH, spark)
  val moviesRdd: RDD[String] = AsosInterviewUtils.getSourceData(MOVIES_FILE_PATH, spark)

  val ratingSplit: RDD[(String, String, String)] = AsosInterviewUtils.splitRatingsRdd(ratingsRdd)
  val moviesSplit: RDD[(String, String, String)] = AsosInterviewUtils.splitMoviesRdd(moviesRdd)

  // Exercise 1
  val moviesUsersRatings: RDD[(String, Int, Double)] = AsosInterviewLogic.groupedMoviesUsersAverageRating(ratingSplit)

  val moviesRatingsString: RDD[String] = moviesUsersRatings.map(row => row._1 + "," + row._2 + "," + row._3)

  moviesRatingsString.coalesce(1).saveAsTextFile(finalDirectory+MOVIES_RATINGS_USERS_FILENAME)

  // Exercise 2
  val genresInMovies: RDD[(String, Int)] = AsosInterviewLogic.groupedMoviesGenres(
    moviesSplit.map(movie => (movie._1, movie._3)),
    spark
  )

  val genresInMoviesString: RDD[String] = genresInMovies.map(row => row._1 + "," + row._2)

  genresInMoviesString.coalesce(1).saveAsTextFile(finalDirectory+GENDERS_MOVIES_FILENAME)

  // Exercise 3
  case class RankingMoviesRating(movieId: String, movieTitle: String, averageRating: Double, ranking: Long)

  val rankingOfMoviesWithRating: List[(String, String, Double, Long)] =
    AsosInterviewLogic.getChartOfMoviesByRating(
      ratingSplit,
      moviesSplit.map(movie => (movie._1, movie._2))
    ).take(100).toList

    rankingOfMoviesWithRating
      .map(movie => RankingMoviesRating(movie._1, movie._2, movie._3, movie._4) ).toDF()
      .write.parquet(finalDirectory+RANKING_MOVIES_RATINGS_FILENAME)

}
