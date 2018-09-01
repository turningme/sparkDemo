package org.bytedance.omega

import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.bytedance.omega.util.{Constants, KafkaSink}

object TestHive2Kafka {
  var topic = "omega_feature_test"
  def main(args: Array[String]): Unit = {
    try{
      val warehouseLocation = "/user/tiger/warehouse"
      val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
      //val spark = SparkSession.builder().master("local[4]").appName("OmegaReadHive").getOrCreate()

      val sc = spark.sparkContext
      val sqlContext = spark.sqlContext
      //sqlContext.udf.register()

      val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
        val kafkaProducerConfig = {
          val p = new Properties()
          p.setProperty("bootstrap.servers", Constants.KAFKA_BOOTSTRAP_SERVERS)
          p.setProperty("key.serializer", classOf[StringSerializer].getName)
          p.setProperty("value.serializer", classOf[StringSerializer].getName)
          p
        }
        println("kafka producer init done!")
        sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
      }

      val df = sqlContext.sql("select * from caijing_dmp_stats.active_users_profiles_daily where date = '20180701' limit 100")
      df.show()


     df.foreachPartition(
       partition =>{
         partition.foreach(record=>{
           kafkaProducer.value.send(topic, 0,"", record.toString())
         })
       }
     )


      sc.stop()
    }catch {
      case ex : Exception =>{
        println(ex.toString)
      }
    }
  }
}
