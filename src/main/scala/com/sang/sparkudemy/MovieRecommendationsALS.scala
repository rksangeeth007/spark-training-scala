package com.sang.sparkudemy

import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.ml.recommendation._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import scala.collection.mutable.WrappedArray

object MovieRecommendationsALS {
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("src/main/resources/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
  
  // Row format to feed into ALS
  case class Rating(userId: Int, movieId: Int, rating: Float)
    
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Make a session
    val spark = SparkSession
      .builder
      .appName("ALSExample")
      .master("local[*]")
      .getOrCreate()
      
    import spark.implicits._
    
    println("Loading movie names...")
    val nameDict = loadMovieNames()
 
    val data = spark.read.textFile("src/main/resources/u.data")
    
    val ratings = data.map( x => x.split('\t') ).map( x => Rating(x(0).toInt, x(1).toInt, x(2).toFloat) ).toDF()
    
    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")
    
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    
    val model = als.fit(ratings)
      
    // Get top-10 recommendations for the user we specified
    val userID:Int = args(0).toInt
    val users = Seq(userID).toDF("userId")
    val recommendations = model.recommendForUserSubset(users, 10)

    // Display them (oddly, this is the hardest part!)
    println("\nTop 10 recommendations for user ID " + userID + ":")
 
    for (userRecs <- recommendations) {
      val myRecs = userRecs(1) // First column is userID, second is the recs
      val temp = myRecs.asInstanceOf[WrappedArray[Row]] // Tell Scala what it is
      for (rec <- temp) {
        val movie = rec.getAs[Int](0)
        val rating = rec.getAs[Float](1)
        val movieName = nameDict(movie)
        println(movieName, rating)
      }
    }
    
    // Stop the session
    spark.stop()

  }
}