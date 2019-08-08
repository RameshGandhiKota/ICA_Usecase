package com.ica.gruppen.web2hdfs

import org.apache.spark.sql.{ SparkSession, DataFrame }

import com.ica.gruppen.web2hdfs.ParseData._

object LoadFileGenericFlow {

  val spark = SparkSession.builder().appName("Loading_Web2HDFS_V2_App").enableHiveSupport().getOrCreate()

  def loadFileGenericFunc(fileURL: String, outputTblNm: String, set: Int, loadType: String): Unit = {
    val rawData = spark.sparkContext.textFile(fileURL)
    val len_temp = rawData.take(1).map(x => x.split(' ').filter(_ != "")).map(_.size)

    val len = len_temp(0)

    val dataDF: DataFrame = len match {
      case 6 if set == 1  => parse1Data6(rawData)
      case 8 if set == 1  => parse1Data8(rawData)
      case 9 if set == 1  => parse1Data9(rawData)
      case 12 if set == 1 => parse2Data12(rawData)
      case 9 if set == 2  => parse2Data9(rawData)
      case 6 if set == 2  => parse2Data6(rawData)
    }

    val map = dataDF.columns.map((_, "null")).toMap
    dataDF.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF.na.fill(map).write.format("parquet").mode(loadType).partitionBy("year").saveAsTable(outputTblNm)

  }


  def parseData(line:List[String]):ParseData1 = {
    
    line.size match {
      case 6 => ParseData6(line)
      case 8 => ParseData8(line)
      case 9 => ParseData9(line)
      //case 12 if set == 1 => parse2Data12(rawData)
      //case 9 if set == 2  => parse2Data9(rawData)
      //case 6 if set == 2  => parse2Data6(rawData)
    }
    
  }
  
  def loadFileGenericFuncV2(fileURL: String, outputTblNm: String, set: Int, loadType: String): Unit = {
    val rawData = spark.sparkContext.textFile(fileURL)
    val len_temp = rawData.take(1).map(x => x.split(' ').filter(_ != "")).map(_.size)

    val len = len_temp(0)

    val dataDF1 = rawData.map(x => x.split(' ').filter(_ != "").toList).map { parseData }.toDF()

    val map = dataDF1.columns.map((_, "null")).toMap
    dataDF1.na.fill(map).printSchema
    // dataDF.na.fill(map).show
    dataDF1.na.fill(map).write.format("parquet").mode(loadType).partitionBy("year").saveAsTable(outputTblNm)

  }  
  
}