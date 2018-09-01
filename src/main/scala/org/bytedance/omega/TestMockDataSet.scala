package org.bytedance.omega

import org.apache.spark.sql._
import org.apache.spark.sql.types._



object TestMockDataSet {
  case class Incidents(incidentnum:String)

  def main(args: Array[String]): Unit = {

    //.master("local[4]")
    val spark = SparkSession.builder().appName("tt")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.sqlContext.implicits._


    val rdd03 = sc.parallelize(List(Incidents("1"),Incidents("2")), 1)
    val sfpdDF = rdd03.toDF()
    //sfpdDF.show()


    //////**//////
    val rdd04 = sc.parallelize(List(Row(1,2,"中国"),Row(1,3,"你好")))
    val testSchema = StructType(Array(StructField("IncNum", IntegerType, true), StructField("Date", IntegerType, true), StructField("District", StringType, true)))
    val testDF04 = spark.sqlContext.createDataFrame(rdd04, testSchema)
    testDF04.show()

    val rdd1 = testDF04.rdd
    rdd1.repartition(2)

    rdd1.foreachPartition(partion=>{
      println("tttttttttttttttttttttttttt")
      partion.foreach(ff=>{println(ff.toString())})
    })
    sc.stop()

  }
}
