
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

object LoadHDFSFromWeb {

  def main(args: Array[String]): Unit = {

    //val sparkConf = new SparkConf().setAppName(ContactHistory.appName)
    //val sc = new SparkContext(sparkConf)
    val spark = SparkSession().builder().appName("Loading_Web2HDFS_App").enableHiveSupport().getOrCreate()

    val fileURLBase1 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/"
    val fileURLBase2 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/"

    //
    // processing file(s) from URL1
    //

    // 1) processing 1st file from URL1 : stockholm_daily_temp_obs_1756_1858_t1t2t3.txt

    // reading data from URL
    val fileName = "stockholm_daily_temp_obs_1756_1858_t1t2t3.txt"
    val fileURL = fileURLBase1 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt")
    val rddData = rawData.collect

    // defining structure for Dataframe
    case class recordStructure(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float)

    // converting RDD into Dataframe
    val dataDF = rddData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toSeq.toDF()

    // converting NaN values as NULL values and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_daily_temp_obs_1756_1858_t1t2t3")

    //
    //

    // 2) processing 2nd file from URL1 : stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt

    // reading data from URL
    val fileName = "stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt"
    val fileURL = fileURLBase1 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt")
    val rddData = rawData.collect

    // defining structure for Dataframe
    case class recordStructure(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float, tmax: Float, tmin: Float)

    // converting RDD into Dataframe
    val dataDF = rddData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat) }.toSeq.toDF()

    // converting NaN values as NULL values and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_daily_temp_obs_1859_1960_t1t2t3txtn")

    //
    //

    // 3) processing 3rd file from URL1 : stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt.txt

    // reading data from URL
    val fileName = "stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt"
    val fileURL = fileURLBase1 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt")
    val rddData = rawData.collect

    // defining structure for Dataframe
    case class recordStructure(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float, tmax: Float, tmin: Float, tmean: Float)

    // converting RDD into Dataframe
    val dataDF = rddData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat) }.toSeq.toDF()

    // converting NaN values as NULL values and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm")

    //
    //

    // 4) processing 4th file from URL1 : stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt

    // reading data from URL
    val fileName = "stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt"
    val fileURL = fileURLBase1 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
    val rddData = rawData.collect

    // defining structure for Dataframe
    case class recordStructure(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float, tmax: Float, tmin: Float, tmean: Float)

    // converting RDD into Dataframe
    val dataDF = rddData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat) }.toSeq.toDF()

    // converting NaN values as NULL values and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm")

    //
    //

    // 5) processing 5th file from URL1 : stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt

    // reading data from URL
    val fileName = "stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt"
    val fileURL = fileURLBase1 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
    val rddData = rawData.collect

    // defining structure for Dataframe
    case class recordStructure(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float, tmax: Float, tmin: Float, tmean: Float)

    // converting RDD into Dataframe
    val dataDF = rddData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat) }.toSeq.toDF()

    // converting NaN values as NULL values and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm")

    //
    //

    //
    // processing file(s) from URL2
    //

    // 1) processing 1st file from URL2 : stockholm_barometer_1756_1858.txt

    // reading data from URL
    val fileName = "stockholm_barometer_1756_1858.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_barometer_1756_1858.txt")
    val rddData = rawData.collect

    // defining structure for Dataframe
    case class recordStructure(year: Int, month: Int, day: Int, b_mrng: Float, bt_mrng: Float, b_aftn: Float, bt_aftn: Float, b_evng: Float, bt_evng: Float)

    // converting RDD into Dataframe
    val dataDF = rddData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat) }.toSeq.toDF()

    // converting NaN values as NULL values and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    // dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_barometer_1756_1858")

    //
    //

    // 2) processing 2nd file from URL2 : stockholm_barometer_1859_1861.txt

    // reading data from URL
    val fileName = "stockholm_barometer_1859_1861.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_barometer_1859_1861.txt")
    val rddData = rawData.collect

    // defining structure for Dataframe
    case class recordStructure(year: Int, month: Int, day: Int, b_mrng: Float, t_mrng: Float, a_mrng: Float, b_aftn: Float, t_aftn: Float, a_aftn: Float, b_evng: Float, t_evng: Float, a_evng: Float)

    // converting RDD into Dataframe
    val dataDF = rddData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat, cols(9).toFloat, cols(10).toFloat, cols(11).toFloat) }.toSeq.toDF()

    // converting NaN values as NULL values and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    // dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_barometer_1859_1861")

    //
    //

    // 3) processing 3rd file from URL2 : stockholm_barometer_1862_1937.txt

    // reading data from URL
    val fileName = "stockholm_barometer_1862_1937.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_barometer_1862_1937.txt")
    val rddData = rawData.collect

    // defining structure for Dataframe
    case class recordStructure(year: Int, month: Int, day: Int, a_mrng: Float, a_aftn: Float, a_evng: Float)

    // converting RDD into Dataframe
    val dataDF = rddData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toSeq.toDF()

    // converting NaN values as NULL values and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    // dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_barometer_1862_1937")

    //
    //

    // 4) processing 4th file from URL2 : stockholm_barometer_1938_1960.txt

    // reading data from URL
    val fileName = "stockholm_barometer_1938_1960.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_barometer_1938_1960.txt")
    val rddData = rawData.collect

    // defining structure for Dataframe
    case class recordStructure(year: Int, month: Int, day: Int, a_mrng: Float, a_aftn: Float, a_evng: Float)

    // converting RDD into Dataframe
    val dataDF = rddData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toSeq.toDF()

    // converting NaN values as NULL values and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    // dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_barometer_1938_1960")

    //
    //

    // 5) processing 5th file from URL2 : stockholm_barometer_1961_2012.txt

    // reading data from URL
    val fileName = "stockholm_barometer_1961_2012.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_barometer_1961_2012.txt")
    val rddData = rawData.collect

    // defining structure for Dataframe
    case class recordStructure(year: Int, month: Int, day: Int, a_mrng: Float, a_aftn: Float, a_evng: Float)

    // converting RDD into Dataframe
    val dataDF = rddData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toSeq.toDF()

    // converting NaN values as NULL values and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    // dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_barometer_1961_2012")

    //
    //

    // 6) processing 6th file from URL2 : stockholm_barometer_2013_2017.txt

    // reading data from URL
    val fileName = "stockholm_barometer_2013_2017.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholm_barometer_2013_2017.txt")
    val rddData = rawData.collect

    // defining structure for Dataframe
    case class recordStructure(year: Int, month: Int, day: Int, a_mrng: Float, a_aftn: Float, a_evng: Float)

    // converting RDD into Dataframe
    val dataDF = rddData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toSeq.toDF()

    // converting NaN values as NULL values and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    // dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholm_barometer_2013_2017")

    //
    //

    // 7) processing 7th file from URL2 : stockholmA_barometer_2013_2017.txt

    // reading data from URL
    val fileName = "stockholmA_barometer_2013_2017.txt"
    val fileURL = fileURLBase2 + fileName
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n")
    val rawData = spark.sparkContext.parallelize(loadData)
    //val rawData = spark.sparkContext.textFile("/tmp/temp/data/stockholmA_barometer_2013_2017.txt")
    val rddData = rawData.collect

    // defining structure for Dataframe
    case class recordStructure(year: Int, month: Int, day: Int, a_mrng: Float, a_aftn: Float, a_evng: Float)

    // converting RDD into Dataframe
    val dataDF = rddData.map(x => x.split(' ').filter(_ != "")).map { cols => recordStructure(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toSeq.toDF()

    // converting NaN values as NULL values and then writing the data into Hive tables
    val map = dataDF.columns.map((_, "null")).toMap
    // dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode("overwrite").partitionBy("year").saveAsTable("stockholmA_barometer_2013_2017")

    //
    //

  }

}