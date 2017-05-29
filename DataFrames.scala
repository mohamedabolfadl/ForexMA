package com.abolfadl.forexma

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
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
    
    //(fields(1).split(":")(1).toInt,(fields(0).filterNot(dotToRemove)+fields(1).filterNot(colonToRemove)).toLong, fields(5).toDouble)
    (fields(1).split(":")(1).toInt,fields(0).replace(".", "-")+" "+fields(1)+":00", fields(5).toDouble)
    
    
  }
  

  
   def getMATF_T(sp: SparkSession, cross: String, MAv: Int, pipVal:Double, sgnCross:Double, cr:String, timeFrame:Int, crossName:String): DataFrame = {
    
    import sp.implicits._
    val lines = sp.sparkContext.textFile(cross)
    val strName = crossName+"minu"
    val quotesRaw = lines.map(mapperTup).toDF(strName,"date","close").cache()
    //val quotes = quotesRaw.filter($"minu"%timeFrame<1).drop(quotesRaw.col("minu")).cache()
    val quotes = quotesRaw.filter(quotesRaw(strName)%timeFrame<1).cache()
    
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
    val MAv:Int = 33               // Moving average
    val timeFrame:Int = 1         // Time frame in minutes (dont exceed 60)
    val triggerVal:Double = 20.0   // Threshold of sending a trigger
    val thresholdAlarm:Int = 31    // Minimum time between two triggers 
    val wSpec1 = Window.orderBy("date")
    
        val eurStrength = getMATF_T(spark, "../EURUSDs.csv", MAv, 0.0001, 1,"EUR",timeFrame,"EURUSD").cache()
    val cadStrength = getMATF_T(spark, "../USDCADs.csv", MAv, 0.0001, 1,"CAD",timeFrame,"USDCAD").cache()
    val gbpStrength = getMATF_T(spark, "../GBPUSDs.csv", MAv, 0.0001, -1,"GBP",timeFrame,"GBPUSD").cache()
    val jpyStrength = getMATF_T(spark, "../USDJPYs.csv", MAv, 0.01, 1,"JPY",timeFrame,"USDJPY").cache()
    val nzdStrength = getMATF_T(spark, "../NZDUSDs.csv", MAv, 0.0001, -1,"NZD",timeFrame,"NZDUSD").cache()
    val chfStrength = getMATF_T(spark, "../USDCHFs.csv", MAv, 0.0001, 1,"CHF",timeFrame,"USDCHF").cache()
    val audStrength = getMATF_T(spark, "../AUDUSDs.csv", MAv, 0.0001, -1,"AUD",timeFrame,"AUDUSD").cache()
    
    //val eurStrength = getMATF(spark, "../EURUSDs.csv", MAv, 0.0001, 1,"EUR",timeFrame).cache()
    //val cadStrength = getMATF(spark, "../USDCADs.csv", MAv, 0.0001, 1,"CAD",timeFrame).cache()
    //val gbpStrength = getMATF(spark, "../GBPUSDs.csv", MAv, 0.0001, -1,"GBP",timeFrame).cache()
    //val jpyStrength = getMATF(spark, "../USDJPYs.csv", MAv, 0.01, 1,"JPY",timeFrame).cache()
    //val nzdStrength = getMATF(spark, "../NZDUSDs.csv", MAv, 0.0001, -1,"NZD",timeFrame).cache()
    //val chfStrength = getMATF(spark, "../USDCHFs.csv", MAv, 0.0001, 1,"CHF",timeFrame).cache()
    //val audStrength = getMATF(spark, "../AUDUSDs.csv", MAv, 0.0001, -1,"AUD",timeFrame).cache()
  
  // Joining strengths  
  val total = cadStrength.join(eurStrength,"date").join(gbpStrength,"date").join(jpyStrength,"date").join(nzdStrength,"date").join(chfStrength,"date").join(audStrength,"date").drop("EURUSDminu").drop("GBPUSDminu").drop("USDJPYminu").drop("NZDUSDminu").drop("USDCHFminu").drop("AUDUSDminu").cache()
  
  // Averaging strengths
  val averageTotal = total.withColumn("AVG",(total("EUR")+total("CAD")+total("JPY")+total("CHF")+total("NZD")+total("AUD")+total("GBP"))/7.0).cache()
 import spark.implicits._
  val averageTotalWithPreviousMinute = averageTotal.withColumn("PrevTime",lag(averageTotal("USDCADminu"),1).over(wSpec1)).withColumn("PrevDate",lag(averageTotal("date"),1).over(wSpec1)).cache()
  
  averageTotalWithPreviousMinute.filter((($"USDCADminu"-$"PrevTime")>1) && (($"USDCADminu"%10>5 && $"PrevTime"%10<5)|| ($"USDCADminu"%10>0 && $"USDCADminu"%10<5 && $"PrevTime"%10>5)) ).show()
  
  // Filtering according to thresholds
  val filteredAverageTotalS = averageTotal.filter(averageTotal("AVG")>triggerVal).cache()
  val filteredAverageTotalB = averageTotal.filter(averageTotal("AVG")<(-1.0*triggerVal)).cache()
  
  // Including date before triggering the threshold
 
  val withprevdateColumnS = filteredAverageTotalS.withColumn("PrevDate",lag(filteredAverageTotalS("date"),1).over(wSpec1)).cache()
  val withprevdateColumnB = filteredAverageTotalB.withColumn("PrevDate",lag(filteredAverageTotalB("date"),1).over(wSpec1)).cache()
  
  
  // Finding time difference between dates above threshold
  import spark.implicits._
  val differenceDateS = withprevdateColumnS.withColumn("DiffDate",(unix_timestamp(withprevdateColumnS("date"))-unix_timestamp(withprevdateColumnS("PrevDate")))/60D).cache()
  val differenceDateB = withprevdateColumnB.withColumn("DiffDate",(unix_timestamp(withprevdateColumnB("date"))-unix_timestamp(withprevdateColumnB("PrevDate")))/60D).cache()
 
  
  // Selecting triggers which occur after minimum threshold time
  val filteredByThresholdS = differenceDateS.filter(differenceDateS("DiffDate")>thresholdAlarm).drop(differenceDateS.col("PrevDate"))
  val filteredByThresholdB = differenceDateB.filter(differenceDateB("DiffDate")>thresholdAlarm).drop(differenceDateB.col("PrevDate"))
  filteredByThresholdS.show()
  filteredByThresholdB.show()
  
  // Savers
  //averageTotal.write.format("csv").save("C:/SparkScala/out")
  //averageTotal.rdd.saveAsTextFile("C:/SparkScala/out")
  //averageTotal.write.csv("C:/SparkScala/out")
  
    
    spark.stop()
  }
}
