package com.sang.sparkudemy

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Compute the total amount spent per customer in some fake e-commerce data. */
object TotalSpentByCustomerSorted {

  /** Convert input data to (customerID, amountSpent) tuples */
  def extractCustomerPricePairs(line: String): (Int, Float) = {
    var fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalSpentByCustomerSorted")

    val input = sc.textFile("src/main/resources/customer-orders.csv")

    val mappedInput = input.map(extractCustomerPricePairs)

    val totalByCustomer = mappedInput.reduceByKey((x, y) => x + y)

    val totalByCustomerSorted = totalByCustomer.map(x => (x._2, x._1)).sortByKey()

    // Print the results.
    for (result <- totalByCustomerSorted) {
      val spend = result._1
      val cx = result._2
      println(s"$cx: $spend")
    }
  }
}