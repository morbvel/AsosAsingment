package org.asos.interview

import org.apache.spark.rdd.RDD

trait TestsUtils {

  def getValueFromKey[K, V](rdd: RDD[(K, V)], key: K): V = {
    filterByKey(rdd, key).first()._2
  }

  def filterByKey[K, V](rdd: RDD[(K, V)], key: K): RDD[(K, V)] =
    rdd.filter(_._1 == key)
}
