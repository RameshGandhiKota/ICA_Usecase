
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object LoadHDFSFromWeb {

  val sparkConf = new SparkConf().setAppName("LoadingDataFromWeb2HDFS")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val spark = SparkSession.builder().appName("Loading_Web2HDFS_App").enableHiveSupport().getOrCreate()

  val fileURLBase1 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/"
  val fileURLBase2 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/"

  case class recordStructure11(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float)
  case class recordStructure12(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float, tmax: Float, tmin: Float)
  case class recordStructure13(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float, tmax: Float, tmin: Float, tmean: Float)
  //case class recordStructure14(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float, tmax: Float, tmin: Float, tmean: Float)
  //case class recordStructure15(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float, tmax: Float, tmin: Float, tmean: Float)

  case class recordStructure21(year: Int, month: Int, day: Int, b_mrng: Float, bt_mrng: Float, b_aftn: Float, bt_aftn: Float, b_evng: Float, bt_evng: Float)
  case class recordStructure22(year: Int, month: Int, day: Int, b_mrng: Float, t_mrng: Float, a_mrng: Float, b_aftn: Float, t_aftn: Float, a_aftn: Float, b_evng: Float, t_evng: Float, a_evng: Float)
  case class recordStructure23(year: Int, month: Int, day: Int, a_mrng: Float, a_aftn: Float, a_evng: Float)
  //case class recordStructure24(year: Int, month: Int, day: Int, a_mrng: Float, a_aftn: Float, a_evng: Float)
  //case class recordStructure25(year: Int, month: Int, day: Int, a_mrng: Float, a_aftn: Float, a_evng: Float)
  //case class recordStructure26(year: Int, month: Int, day: Int, a_mrng: Float, a_aftn: Float, a_evng: Float)
  //case class recordStructure27(year: Int, month: Int, day: Int, a_mrng: Float, a_aftn: Float, a_evng: Float)

  def loadFile11(): Unit = {

    // reading data from URL
    val fileName = "stockholm_daily_temp_obs_1756_1858_t1t2t3.txt"
    val fileURL = fileURLBase1 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt")

    // defining structure for Dataframe

    // converting RDD into Dataframe
    val dataDF = rawData.map(x => x.split(' ').filter(_ != "").toList).map { cols => recordStructure11(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toDF()

    // converting NaN varues as NULL varues and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_daily_temp_obs_1756_1858_t1t2t3")

  }

  def loadFile12(): Unit = {
    // reading data from URL
    val fileName = "stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt"
    val fileURL = fileURLBase1 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt")

    // converting RDD into Dataframe
    val dataDF = rawData.map(x => x.split(' ').filter(_ != "").toList).map { cols => recordStructure12(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat) }.toDF()

    // converting NaN varues as NULL varues and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_daily_temp_obs_1859_1960_t1t2t3txtn")

  }

  def loadFile13(): Unit = {
    // reading data from URL
    val fileName = "stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt"
    val fileURL = fileURLBase1 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt")

    // converting RDD into Dataframe
    val dataDF = rawData.map(x => x.split(' ').filter(_ != "").toList).map { cols => recordStructure13(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat) }.toDF()

    // converting NaN varues as NULL varues and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm")

  }

  def loadFile14(): Unit = {
    // reading data from URL
    val fileName = "stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt"
    val fileURL = fileURLBase1 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")

    // converting RDD into Dataframe
    val dataDF = rawData.map(x => x.split(' ').filter(_ != "").toList).map { cols => recordStructure13(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat) }.toDF()

    // converting NaN varues as NULL varues and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm")

  }

  def loadFile15(): Unit = {
    // reading data from URL
    val fileName = "stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt"
    val fileURL = fileURLBase1 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")

    // converting RDD into Dataframe
    val dataDF = rawData.map(x => x.split(' ').filter(_ != "").toList).map { cols => recordStructure13(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat) }.toDF()

    // converting NaN varues as NULL varues and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm")

  }

  def loadFile21(): Unit = {
    // reading data from URL
    val fileName = "stockholm_barometer_1756_1858.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_barometer_1756_1858.txt")

    // converting RDD into Dataframe
    val dataDF = rawData.map(x => x.split(' ').filter(_ != "").toList).map { cols => recordStructure21(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat) }.toDF()

    // converting NaN varues as NULL varues and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_barometer_1756_1858")

  }

  def loadFile22(): Unit = {
    // reading data from URL
    val fileName = "stockholm_barometer_1859_1861.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_barometer_1859_1861.txt")

    // converting RDD into Dataframe
    val dataDF = rawData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure22(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat, cols(9).toFloat, cols(10).toFloat, cols(11).toFloat) }.toDF()

    // converting NaN varues as NULL varues and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_barometer_1859_1861")

  }

  def loadFile23(): Unit = {
    // reading data from URL
    val fileName = "stockholm_barometer_1862_1937.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_barometer_1862_1937.txt")

    // converting RDD into Dataframe
    val dataDF = rawData.map(x => x.split(' ').filter(_ != "").toList).map { cols => recordStructure23(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toDF()

    // converting NaN varues as NULL varues and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_barometer_1862_1937")

  }

  def loadFile24(): Unit = {
    // reading data from URL
    val fileName = "stockholm_barometer_1938_1960.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_barometer_1938_1960.txt")

    // converting RDD into Dataframe
    val dataDF = rawData.map(x => x.split(' ').filter(_ != "").toList).map { cols => recordStructure23(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toDF()

    // converting NaN varues as NULL varues and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_barometer_1938_1960")

  }

  def loadFile25(): Unit = {
    // reading data from URL
    val fileName = "stockholm_barometer_1961_2012.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_barometer_1961_2012.txt")

    // converting RDD into Dataframe
    val dataDF = rawData.map(x => x.split(' ').filter(_ != "").toList).map { cols => recordStructure23(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toDF()

    // converting NaN varues as NULL varues and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_barometer_1961_2012")

  }

  def loadFile26(): Unit = {
    // reading data from URL
    val fileName = "stockholm_barometer_2013_2017.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_barometer_2013_2017.txt")

    // converting RDD into Dataframe
    val dataDF = rawData.map(x => x.split(' ').filter(_ != "").toList).map { cols => recordStructure23(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toDF()

    // converting NaN varues as NULL varues and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_barometer_2013_2017")

  }

  def loadFile27(): Unit = {
    // reading data from URL
    val fileName = "stockholmA_barometer_2013_2017.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholmA_barometer_2013_2017.txt")

    // converting RDD into Dataframe
    val dataDF = rawData.map(x => x.split(' ').filter(_ != "").toList).map { cols => recordStructure23(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toDF()

    // converting NaN varues as NULL varues and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholmA_barometer_2013_2017")

  }

  def main(args: Array[String]): Unit = {

    //
    // processing file(s) from URL1
    //

    // 1) processing 1st file from URL1 : stockholm_daily_temp_obs_1756_1858_t1t2t3.txt
    loadFile11

    // 2) processing 2nd file from URL1 : stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt
    loadFile12

    // 3) processing 3rd file from URL1 : stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt.txt
    loadFile13

    // 4) processing 4th file from URL1 : stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt
    loadFile14

    // 5) processing 5th file from URL1 : stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt
    loadFile15

    //
    // processing file(s) from URL2
    //

    // 1) processing 1st file from URL2 : stockholm_barometer_1756_1858.txt
    loadFile21

    // 2) processing 2nd file from URL2 : stockholm_barometer_1859_1861.txt
    loadFile22

    // 3) processing 3rd file from URL2 : stockholm_barometer_1862_1937.txt
    loadFile23

    // 4) processing 4th file from URL2 : stockholm_barometer_1938_1960.txt
    loadFile24

    // 5) processing 5th file from URL2 : stockholm_barometer_1961_2012.txt
    loadFile25

    // 6) processing 6th file from URL2 : stockholm_barometer_2013_2017.txt
    loadFile26

    // 7) processing 7th file from URL2 : stockholmA_barometer_2013_2017.txt
    loadFile27

    // End of main block
  }

}