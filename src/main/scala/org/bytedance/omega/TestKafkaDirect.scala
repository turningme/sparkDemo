package org.bytedance.omega

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object TestKafkaDirect {
  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder().master("local[4]").appName("OmegaReadKafkaDirect").getOrCreate()
    val sc = spark.sparkContext
    val ssc:StreamingContext = new StreamingContext(sc,Seconds(2))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.8.128.135:9192,10.8.128.136:9192,10.8.128.137:9192,10.8.128.138:9192,10.8.128.139:9192",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_omega_test1_05",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("omega_feature_test")
    val stream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,Subscribe[String, String](topics, kafkaParams))

    //println("------------------------------->" + stream.count())

    stream.foreachRDD(rdd=>{
      println("-----------------------"  + rdd.name  + ":" + rdd.count())
      rdd.foreach(record => {
        println(record.key() + "   :    " + record.value())
      })
    })


    ssc.start()
    ssc.awaitTermination()

  }
}
