package org.bytedance.omega

import org.apache.spark.sql.SparkSession

object TestMockData {
  def main(args: Array[String]): Unit = {

    //master("local[4]").
    val spark = SparkSession.builder().appName("TT")
      .getOrCreate()

    val sc = spark.sparkContext
    val rdd01 = sc.makeRDD(List(1,2,3,4,5,6))
    val r01 = rdd01.map { x => x * x }
    println(r01.collect().mkString(","))


    /* Array */
    val rdd02 = sc.makeRDD(Array(1,2,3,4,5,6))
    val r02 = rdd02.filter { x => x < 5}
    println(r02.collect().mkString(","))


    val rdd03 = sc.parallelize(List(1,2,3,4,5,6), 1)
    val r03 = rdd03.map { x => x + 1 }
    println(r03.collect().mkString(","))
    /* Array */
    val rdd04 = sc.parallelize(List(1,2,3,4,5,6), 1)
    val r04 = rdd04.filter { x => x > 3 }
    println(r04.collect().mkString(","))

  }


}
