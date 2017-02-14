package com.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by venu on 2/13/17.
  */
object SaleByPerson extends App {

  override def main(args: Array[String]): Unit = {
    //  val conf = new SparkConf().setAppName("Hello Spark").setMaster("local")
    //  val sc = new SparkContext(conf)
    //  val sqlContext = new SQLContext(sc)
    //  import sqlContext._

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      //.enableHiveSupport()
      .getOrCreate()


    val df = sparkSession.read.option("header","true").csv("src/main/resources/data/sales.csv")

  }

}
