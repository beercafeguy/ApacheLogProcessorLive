package com.beercafeguy.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogProcessor {

  def main(args: Array[String]): Unit = {
    val ssc=new StreamingContext("local[*]","LogProcessor",Seconds(10))
    val topics=List("apache_logs").toSet
    val kafkaParams=Map("metadata.broker.list" -> "no1010042056227.corp.adobe.com:9092")
    val lines=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics).map(_._2)
    lines.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
