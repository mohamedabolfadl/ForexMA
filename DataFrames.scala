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
    val timeFrame:Int = 30         // Time frame in minutes (dont exceed 60)
    val triggerVal:Double = 15.0   // Threshold of sending a trigger
    val thresholdAlarm:Int = 31    // Minimum time between two triggers 
    val wSpec1 = Window.orderBy("date")// Window for ordering Dataframes according to 'date' column
    

    // Obtain 1 minute accuracy data
    val eurStrength = getMATF_T(spark, "../EURUSDs.csv", MAv, 0.0001, 1,"EUR",1,"EURUSD").cache()
    val cadStrength = getMATF_T(spark, "../USDCADs.csv", MAv, 0.0001, 1,"CAD",1,"USDCAD").cache()
    val gbpStrength = getMATF_T(spark, "../GBPUSDs.csv", MAv, 0.0001, -1,"GBP",1,"GBPUSD").cache()
    val jpyStrength = getMATF_T(spark, "../USDJPYs.csv", MAv, 0.01, 1,"JPY",1,"USDJPY").cache()
    val nzdStrength = getMATF_T(spark, "../NZDUSDs.csv", MAv, 0.0001, -1,"NZD",1,"NZDUSD").cache()
    val chfStrength = getMATF_T(spark, "../USDCHFs.csv", MAv, 0.0001, 1,"CHF",1,"USDCHF").cache()
    val audStrength = getMATF_T(spark, "../AUDUSDs.csv", MAv, 0.0001, -1,"AUD",1,"AUDUSD").cache()
    

  //----------------------------------  STAGE A : CLEANING DATA   ---------------------------------------------------
    
  // Joining strengths  
  val total = cadStrength.join(eurStrength,"date").join(gbpStrength,"date").join(jpyStrength,"date").join(nzdStrength,"date").join(chfStrength,"date").join(audStrength,"date").drop("EURUSDminu").drop("GBPUSDminu").drop("USDJPYminu").drop("NZDUSDminu").drop("USDCHFminu").drop("AUDUSDminu").cache()
  
  // Averaging strengths
  val averageTotal = total.withColumn("AVG",(total("EUR")+total("CAD")+total("JPY")+total("CHF")+total("NZD")+total("AUD")+total("GBP"))/7.0).cache()
  //averageTotal.show(10)
  
  // Including previous date in the same row
  import spark.implicits._
  val averageTotalWithPreviousMinute = averageTotal.withColumn("PrevTime",lag(averageTotal("USDCADminu"),1).over(wSpec1)).withColumn("PrevDate",lag(averageTotal("date"),1).over(wSpec1)).cache()
  //averageTotalWithPreviousMinute.show(10)

  // User defined functions for processing the date strings
  val controlMin = (s: String) => { if(s.size<2) {"0"+s}else {s} }
  val updateD = (dt: String, minu: String) => { dt.substring(0, 14)+controlMin(minu)+":00"             }
  // Wrapping the previous UDF in order to apply it directly to columns
  import org.apache.spark.sql.functions.udf
  val updateDate = udf(updateD)

  // Filtering the locations where the 5 minute data is missing and including previous values of currs.
  val currPrevGaps =  averageTotalWithPreviousMinute.withColumn("PrevCAD",lag(averageTotalWithPreviousMinute("CAD"),1).over(wSpec1)).withColumn("PrevEUR",lag(averageTotalWithPreviousMinute("EUR"),1).over(wSpec1)).withColumn("PrevGBP",lag(averageTotalWithPreviousMinute("GBP"),1).over(wSpec1)).withColumn("PrevJPY",lag(averageTotalWithPreviousMinute("JPY"),1).over(wSpec1)).withColumn("PrevNZD",lag(averageTotalWithPreviousMinute("NZD"),1).over(wSpec1)).withColumn("PrevCHF",lag(averageTotalWithPreviousMinute("CHF"),1).over(wSpec1)).withColumn("PrevAUD",lag(averageTotalWithPreviousMinute("AUD"),1).over(wSpec1)).withColumn("PrevAVG",lag(averageTotalWithPreviousMinute("AVG"),1).over(wSpec1)).filter((($"USDCADminu"-$"PrevTime")>1) && (($"USDCADminu"%10>5 && $"PrevTime"%10<5)|| ($"USDCADminu"%10>0 && $"USDCADminu"%10<5 && $"PrevTime"%10>5)) ).cache()
  //currPrevGaps.show(10)
  // Interpolation between past and future values to obtain the value at 5 min
  val filledGaps = currPrevGaps.withColumn("EURn",(currPrevGaps("EUR")+currPrevGaps("PrevEUR"))/2).drop("EUR").drop("PrevEUR").withColumn("CADn",(currPrevGaps("CAD")+currPrevGaps("PrevCAD"))/2).drop("CAD").drop("PrevCAD").withColumn("GBPn",(currPrevGaps("GBP")+currPrevGaps("PrevGBP"))/2).drop("GBP").drop("PrevGBP").withColumn("JPYn",(currPrevGaps("JPY")+currPrevGaps("PrevJPY"))/2).drop("JPY").drop("PrevJPY").withColumn("NZDn",(currPrevGaps("NZD")+currPrevGaps("PrevNZD"))/2).drop("NZD").drop("PrevNZD").withColumn("CHFn",(currPrevGaps("CHF")+currPrevGaps("PrevCHF"))/2).drop("CHF").drop("PrevCHF").withColumn("AUDn",(currPrevGaps("AUD")+currPrevGaps("PrevAUD"))/2).drop("AUD").drop("PrevAUD").withColumn("AVGn",(currPrevGaps("AVG")+currPrevGaps("PrevAVG"))/2).drop("AVG").drop("PrevAVG").withColumn("NewTimeMin",(lit(5.0) * ceil(currPrevGaps("PrevTime")/5D)).cast("Int").cast("String")).cache()
  // Insert the new date at the 5 min intervals
  val filledGapswithDate = filledGaps.withColumn("NewDate",updateDate(filledGaps("date"),filledGaps("NewTimeMin"))).drop("date").drop("USDCADminu").drop("PrevTime").drop("PrevDate").drop("NewTimeMin").cache()
  // Renaming the column names to facilitate joining
  val newNames = Seq("EUR", "CAD", "GBP", "JPY","NZD","CHF","AUD","AVG","date")
  val dfRenamed = filledGapswithDate.toDF(newNames: _*).cache()
  // Reordering the columns
  val columns: Array[String] = dfRenamed.columns
  val reorderedColumnNames: Array[String] = Array("date","CAD","EUR","GBP","JPY","NZD","CHF","AUD","AVG") // do the reordering you want
  val averageTotal_Reordered: DataFrame = dfRenamed.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
 
  // Removing the temporary column of the minute field
  val averageTotal_filt = averageTotal.drop("USDCADminu").cache()
    
  // Merging data with interpolated data
  val completeData = averageTotal_filt.union(averageTotal_Reordered).orderBy("date")
  
    
  //----------------------------------  STAGE B : EXTRACT TIME FRAME   ---------------------------------------------------
  
  // Function to extract minute from the time stamp
  val getMin_D = (dt: String) => { dt.substring(14, 16).toInt             }
  val getMin = udf(getMin_D)
  
  // Filter according to the time frame
  val extracted_TF = completeData.filter(getMin($"date")%timeFrame===0).cache()
  
  extracted_TF.show()
  //----------------------------------  STAGE c : FILTER TRIGGER POINTS   ---------------------------------------------------
   
  
  // Filtering according to thresholds
  val filteredAverageTotalS = extracted_TF.filter(extracted_TF("AVG")>triggerVal).cache()
  val filteredAverageTotalB = extracted_TF.filter(extracted_TF("AVG")<(-1.0*triggerVal)).cache()
  filteredAverageTotalS.show()
  
  // Including date before triggering the threshold
  val withprevdateColumnS = filteredAverageTotalS.withColumn("PrevDate",lag(filteredAverageTotalS("date"),1).over(wSpec1)).cache()
  val withprevdateColumnB = filteredAverageTotalB.withColumn("PrevDate",lag(filteredAverageTotalB("date"),1).over(wSpec1)).cache()
  withprevdateColumnS.show()
  
  // Finding time difference between dates above threshold
  import spark.implicits._
  val differenceDateS = withprevdateColumnS.withColumn("DiffDate",(unix_timestamp(withprevdateColumnS("date"))-unix_timestamp(withprevdateColumnS("PrevDate")))/60D).cache()
  val differenceDateB = withprevdateColumnB.withColumn("DiffDate",(unix_timestamp(withprevdateColumnB("date"))-unix_timestamp(withprevdateColumnB("PrevDate")))/60D).cache()
 differenceDateS.show()
  
  // Selecting triggers which occur after minimum threshold time
  val filteredByThresholdS = differenceDateS.filter(differenceDateS("DiffDate")>thresholdAlarm).drop(differenceDateS.col("PrevDate"))
  val filteredByThresholdB = differenceDateB.filter(differenceDateB("DiffDate")>thresholdAlarm).drop(differenceDateB.col("PrevDate"))

  filteredByThresholdS.show()
//  filteredByThresholdB.show()
  
  // Savers
  //averageTotal.write.format("csv").save("C:/SparkScala/out")
  //averageTotal.rdd.saveAsTextFile("C:/SparkScala/out")
  //averageTotal.write.csv("C:/SparkScala/out")
  
    
    spark.stop()
  }
}
