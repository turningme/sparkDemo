package org.bytedance.omega

import org.apache.spark.sql.SparkSession

object TestReadHive {
  def main(args: Array[String]): Unit = {
    try{
      println("------------------ " + System.getProperty("file.encoding"))
      System.setProperty("file.encoding","UTF-8")
      System.setProperty("user.language","zh")
      System.setProperty("user.region","UTF-8")
      System.setProperty("user.region","CN")
      println("------------------ " + System.getProperty("file.encoding"))
      val warehouseLocation = "/user/tiger/warehouse"
      val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
      //val spark = SparkSession.builder().master("local[4]").appName("OmegaReadHive").getOrCreate()

      val sc = spark.sparkContext
      val sqlContext = spark.sqlContext
      //sqlContext.udf.register()

      val df = sqlContext.sql("select * from caijing_dmp_stats.active_users_profiles_daily where date = '20180701' limit 10")
      df.show()

      df.registerTempTable("simpleTable")

      val df1 = sqlContext.sql("select * from simpleTable")

      df1.rdd.foreachPartition(partion=>{
        partion.foreach(ff=>{println(ff.toString())})
      })
      sc.stop()
    }catch {
      case ex:Exception => {
        println("Exception===>" + ex)
      }
    }

  }
}
