package com.spark

import org.apache.log4j.LogManager
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

    // Importing the SparkSession gives access to all the SQL functions and implicit conversions.
    import sparkSession.implicits._
    val log = LogManager.getRootLogger

    //val df = sparkSession.read.option("header","true").csv("src/main/resources/data/sales.csv")

    val df = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/sales.csv")
      .cache

    sparkSession.stop()
  }

}
