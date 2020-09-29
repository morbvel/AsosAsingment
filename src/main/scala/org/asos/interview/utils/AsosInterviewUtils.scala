package org.asos.interview.utils

import java.io.InputStream

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object AsosInterviewUtils extends Constants {

  def getSourceData(filePath: String, spark: SparkSession): RDD[String] = {

    val localFileStream: Option[InputStream] = Some(getClass.getResourceAsStream(filePath))

    localFileStream match{
      case None => throw new Exception(s"${filePath} not found")
      case Some(x) => spark.sparkContext.parallelize(scala.io.Source.fromInputStream(x, "ISO-8859-1").getLines().toSeq)
    }

  }

  def getAverage(list: List[Double]): Double = {

    val size = list.size
    def loop(list: List[Double], acc: Double = 0): Double = {
      list match{
        case Nil => roundUp(acc / size)
        case element :: tail => loop(tail, acc + element )
      }
    }

    loop(list)
  }

  def roundUp(value: Double): Double = (value * 100).round / 100.toDouble

  def splitRatingsRdd(ratings: RDD[String]): RDD[(String, String, String)] = {
    ratings.map(rating => {
      val ratingSplit = rating.split(ELEMENT_TO_SPLIT)
      (ratingSplit(1), ratingSplit(0), ratingSplit(2))
    })
  }

  def splitMoviesRdd(movies: RDD[String]): RDD[(String, String, String)] = {
    movies.map(movie => {
      val movieSplit = movie.split(ELEMENT_TO_SPLIT)
      (movieSplit(0), movieSplit(1), movieSplit(2))
    })
  }

}
