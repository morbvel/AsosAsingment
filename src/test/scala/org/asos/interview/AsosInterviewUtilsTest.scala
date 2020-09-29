package org.asos.interview

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.asos.interview.utils.AsosInterviewUtils
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AsosInterviewUtilsTest extends FlatSpec with TestsUtils{

  val spark: SparkSession = SparkSession.builder().config("spark.master", "local[*]").enableHiveSupport().getOrCreate()

  "getSourceData" should "create an RDD from an existing path" in {

    val results = AsosInterviewUtils.getSourceData("/users.dat", spark)

    val groupedUsers = results.map(user => {
      (user.split("::")(0), 1)
    }).groupByKey().map(
      userPair => (userPair._1, userPair._2.sum)
    )

    val filteredUsers = groupedUsers.filter(groupedUser => groupedUser._2 > 2)

    assert(11 == groupedUsers.count())
    assert(0 == filteredUsers.count())
  }

  "getAverage" should "return the average from a list of values" in {
    val simpleListAverage = AsosInterviewUtils.getAverage(List(0,1,2,3,4,5,6,7,8,9,10))
    val negativeAndDecimalValuesAverage = AsosInterviewUtils.getAverage(List(-15, 45.2234, 20.5, -78.1000003))

    assert(5.0 == simpleListAverage)
    assert(-6.84 == negativeAndDecimalValuesAverage)
  }

  "roundUp" should "round values up to two decimals" in {
    assert(14.57 == AsosInterviewUtils.roundUp(14.56667688))
  }

  "splitRatingsRdd" should "create an RDD of (String, String, String) from source" in {
    val inputRdd: RDD[String] = spark.sparkContext.parallelize(List("field1::field2::field3::field4"))

    val result = AsosInterviewUtils
      .splitRatingsRdd(inputRdd)

    assert("field2" == result.map(test => test._1).first())
    assert("field1" == result.map(test => test._2).first())
    assert("field3" == result.map(test => test._3).first())
  }

  "splitMoviesRdd" should "create an RDD of (String, String, String) from source" in {
    val inputRdd: RDD[String] = spark.sparkContext.parallelize(List("field1::field2::field3"))

    val result = AsosInterviewUtils
      .splitMoviesRdd(inputRdd)

    assert("field1" == result.map(test => test._1).first())
    assert("field2" == result.map(test => test._2).first())
    assert("field3" == result.map(test => test._3).first())
  }

}
