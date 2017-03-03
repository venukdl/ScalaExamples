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

    //show the results as output
    df.select("person", "sale_amount").show(10)

    //write output to csv
    //df.select("person").write.format("csv").save("src/main/resources/output/sales")

    // Register the DataFrame as a temporary view
    df.createOrReplaceTempView("sales")

    // SQL statements can be run by using the sql methods provided by Spark
    val largeSalesDF = spark.sql("SELECT person, sale_amount FROM sales WHERE sale_amount > 20")
    // The columns of a row in the result can be accessed by field index
    largeSalesDF.map(person => "Name: " + person(0)).show()

    //group by
    val salesGroupedDF = spark.sql("SELECT person, sum(sale_amount) FROM sales group by person")
    // The columns of a row in the result can be accessed by field index
    salesGroupedDF.map( person => ("Name: " + person(0) + "\t" + person(1)) ).show()

    spark.stop()
  }

}
