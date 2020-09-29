package org.asos.interview

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.asos.interview.utils.AsosInterviewUtils
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AsosInterviewLogicTest extends FlatSpec with TestsUtils{

  val spark: SparkSession = SparkSession.builder().config("spark.master", "local").enableHiveSupport().getOrCreate()

  "getRatingsPerMovie" should "get the average rating for each movie" in {
    val inputStream = getClass.getResourceAsStream("/ratings.dat")
    val ratingsRrd = spark.sparkContext.parallelize(scala.io.Source.fromInputStream(inputStream).getLines().toSeq)
    val ratingsSplited = AsosInterviewUtils.splitRatingsRdd(ratingsRrd)

    val result: RDD[(String, Double)] = AsosInterviewLogic.getRatingsPerMovie(ratingsSplited)
    
    assert(4 == result.count())
    assert(4.17 == getValueFromKey(result, "1190"))
    assert(7.0 == getValueFromKey(result, "1191"))
    assert(2.67 == getValueFromKey(result, "1192"))
    assert(6.67 == getValueFromKey(result, "1193"))
  }

  "getUsersPerMovie" should "get the number of different users for each film" in {
    val inputStream = getClass.getResourceAsStream("/ratings.dat")
    val ratingsRrd = spark.sparkContext.parallelize(scala.io.Source.fromInputStream(inputStream).getLines().toSeq)
    val ratingsSplited = AsosInterviewUtils.splitRatingsRdd(ratingsRrd)

    val result: RDD[(String, Int)] = AsosInterviewLogic.getUsersPerMovie(ratingsSplited)

    assert(4 == result.count())
    assert(4 == getValueFromKey(result, "1190"))
    assert(3 == getValueFromKey(result, "1191"))
    assert(3 == getValueFromKey(result, "1192"))
    assert(3 == getValueFromKey(result, "1193"))
  }

  "groupedMoviesUsersAverageRating" should "return how many users are there for each movie and their average rating" in {
    val inputStream = getClass.getResourceAsStream("/ratings.dat")
    val ratingsRrd = spark.sparkContext.parallelize(scala.io.Source.fromInputStream(inputStream).getLines().toSeq)
    val ratingsSplit = AsosInterviewUtils.splitRatingsRdd(ratingsRrd)

    val result: RDD[(String, (Int, Double))] = AsosInterviewLogic.groupedMoviesUsersAverageRating(ratingsSplit)
        .map(row => (row._1, (row._2, row._3)))

    assert(4 == result.count())
    assert((4, 4.17) == getValueFromKey(result, "1190"))
    assert((3, 7.0) == getValueFromKey(result, "1191"))
    assert((3, 2.67) == getValueFromKey(result, "1192"))
    assert((3, 6.67) == getValueFromKey(result, "1193"))
  }

  "aggGenresPerMovie" should "split and create as many rows as genres for each movie" in {
    val inputStream = getClass.getResourceAsStream("/movies.dat")

    val moviesRdd = spark.sparkContext.parallelize(scala.io.Source.fromInputStream(inputStream).getLines().toSeq)
    val splitedRdd = moviesRdd.map(row => {
      val rowSplit = row.split("::")
      (rowSplit(0), rowSplit(2))
    })

    val result = AsosInterviewLogic.aggGenresPerMovie(splitedRdd, spark)

    assert(3 == filterByKey(result, "1190").count)
    assert(3 == filterByKey(result, "1191").count)
    assert(4 == filterByKey(result, "1192").count)
    assert(5 == filterByKey(result, "1193").count)
  }

  "groupedMoviesGenres" should "return the ammount of movies that each genere appears on" in{
    val inputStream = getClass.getResourceAsStream("/movies.dat")

    val moviesRdd = spark.sparkContext.parallelize(scala.io.Source.fromInputStream(inputStream).getLines().toSeq)
    val moviesSplit = AsosInterviewUtils.splitMoviesRdd(moviesRdd)

    val results = AsosInterviewLogic.groupedMoviesGenres(
      moviesSplit.map(movie => (movie._1, movie._3)),
      spark
    )

    assert(1 == getValueFromKey(results, "Historic"))
    assert(1 == getValueFromKey(results, "Horror"))
    assert(1 == getValueFromKey(results, "Award Winning"))
    assert(2 == getValueFromKey(results, "Animation"))
    assert(1 == getValueFromKey(results, "Sci-Fi"))
    assert(2 == getValueFromKey(results, "Comedy"))
    assert(1 == getValueFromKey(results, "Thriller"))
    assert(1 == getValueFromKey(results, "Drama"))
    assert(1 == getValueFromKey(results, "Romance"))
    assert(1 == getValueFromKey(results, "Children's"))
    assert(3 == getValueFromKey(results, "Action"))
  }

  "getChartOfMoviesByRating" should "generate a chart with top rated films" in {
    val movies = getClass.getResourceAsStream("/movies.dat")
    val moviesRdd = spark.sparkContext.parallelize(scala.io.Source.fromInputStream(movies).getLines().toSeq)
    val moviesSplit = AsosInterviewUtils.splitMoviesRdd(moviesRdd)

    val ratings = getClass.getResourceAsStream("/ratings.dat")
    val ratingsRdd = spark.sparkContext.parallelize(scala.io.Source.fromInputStream(ratings).getLines().toSeq)
    val ratingsSplit = AsosInterviewUtils.splitRatingsRdd(ratingsRdd)

    val results: RDD[(String, (String, Double, Long))] = AsosInterviewLogic
      .getChartOfMoviesByRating(
        ratingsSplit,
        moviesSplit.map(movie => (movie._1, movie._2))
      )
      .map(test => (test._1, (test._2, test._3, test._4)))

    assert(("Jumanji (1995)",7.0,1) == getValueFromKey(results, "1191"))
    assert(("Waiting to Exhale (1995)",6.67,2) == getValueFromKey(results, "1193"))
    assert(("Toy Story (1995)",4.17,3) == getValueFromKey(results, "1190"))
    assert(("Grumpier Old Men (1995)",2.67,4) == getValueFromKey(results, "1192"))
  }

}
