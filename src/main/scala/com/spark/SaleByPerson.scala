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

    //Create spark session
    val spark = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .config("spark.some.config.option", "config-value")
      //.enableHiveSupport()
      .getOrCreate()

    // Importing the SparkSession gives access to all the SQL functions and implicit conversions.
    import spark.implicits._
    val log = LogManager.getRootLogger

    //val df = spark.read.option("header","true").csv("src/main/resources/data/sales.csv")

    val df = spark.read
      .option("header", "true")
      .format("csv")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/sales.csv")

    df.select("person").write.format("csv").save("src/main/resources/output/sales")

      //df.rdd.collect()

    spark.stop()
  }

}
