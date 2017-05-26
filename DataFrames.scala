package com.abolfadl.forexma

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object DataFrames {
  
  case class Price(date:Long, close:Double)
  
  def mapper(line:String): Price = {
    val fields = line.split(',')  
    val dotToRemove = ".".toSet
    val colonToRemove = ":".toSet
    
    val price:Price = Price((fields(0).filterNot(dotToRemove)+fields(1).filterNot(colonToRemove)).toLong, fields(5).toDouble)
    return price
  }
  
    def mapperTup(line:String) = {
    val fields = line.split(',')  
    val dotToRemove = ".".toSet
    val colonToRemove = ":".toSet
    
    (fields(1).split(":")(1).toInt,(fields(0).filterNot(dotToRemove)+fields(1).filterNot(colonToRemove)).toLong, fields(5).toDouble)
    
  }
  

  
   def getMATF(sp: SparkSession, cross: String, MAv: Int, pipVal:Double, sgnCross:Double, cr:String, timeFrame:Int): DataFrame = {
    
    import sp.implicits._
    val lines = sp.sparkContext.textFile(cross)
    val quotesRaw = lines.map(mapperTup).toDF("minu","date","close").cache()
    val quotes = quotesRaw.filter($"minu"%timeFrame<1).drop(quotesRaw.col("minu")).cache()
    val wSpec1 = Window.rowsBetween(-(MAv-1), 0)
    val quotesWithSMA = quotes.withColumn("SMA",avg(quotes("close")).over(wSpec1)  ).cache()
   if (sgnCross<0)
   {
  return quotesWithSMA.withColumn(cr,(-quotesWithSMA("close")+(quotesWithSMA("SMA")-(quotesWithSMA("close")/MAv.toDouble) +  (quotesWithSMA("SMA")/MAv.toDouble)) )/pipVal).drop(quotesWithSMA.col("close")).drop(quotesWithSMA.col("SMA"))

   }
   else
   {
    return quotesWithSMA.withColumn(cr,( quotesWithSMA("close")- (quotesWithSMA("SMA")+(quotesWithSMA("close")/MAv.toDouble) -  (quotesWithSMA("SMA")/MAv.toDouble)) )/pipVal).drop(quotesWithSMA.col("close")).drop(quotesWithSMA.col("SMA"))
   }
  } 
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/Temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
      // Inputs
      val MAv:Int = 10 // Moving average
      val timeFrame:Int =15 // Time frame in minutes (dont exceed 60)
      
  
    val eurStrength = getMATF(spark, "../EURUSDs.csv", MAv, 0.0001, 1,"EUR",timeFrame).cache()
    val cadStrength = getMATF(spark, "../USDCADs.csv", MAv, 0.0001, 1,"CAD",timeFrame).cache()
    val gbpStrength = getMATF(spark, "../GBPUSDs.csv", MAv, 0.0001, -1,"GBP",timeFrame).cache()
    val jpyStrength = getMATF(spark, "../USDJPYs.csv", MAv, 0.01, 1,"JPY",timeFrame).cache()
    val nzdStrength = getMATF(spark, "../NZDUSDs.csv", MAv, 0.0001, -1,"NZD",timeFrame).cache()
    val chfStrength = getMATF(spark, "../USDCHFs.csv", MAv, 0.0001, 1,"CHF",timeFrame).cache()
    val audStrength = getMATF(spark, "../AUDUSDs.csv", MAv, 0.0001, -1,"AUD",timeFrame).cache()
  
  
  val total = cadStrength.join(eurStrength,"date").join(gbpStrength,"date").join(jpyStrength,"date").join(nzdStrength,"date").join(chfStrength,"date").join(audStrength,"date").cache()
  val averageTotal = total.withColumn("AVG",(total("EUR")+total("CAD")+total("JPY")+total("CHF")+total("NZD")+total("AUD")+total("GBP"))/7.0).show()
  

    
    spark.stop()
  }
}