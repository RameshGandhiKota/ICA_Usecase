package com.ica.gruppen.web2hdfs

import com.ica.gruppen.web2hdfs.CaseClassDefns._

import org.apache.spark.sql.{ DataFrame, SQLContext, SparkSession }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }

object ParseData {

  val sparkConf = new SparkConf().setAppName("LoadingDataFromWeb2HDFS")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  def parse1Data6(lines: RDD[String]): DataFrame = {

    lines.map(x => x.split(' ').filter(_ != "").toList).map { cols => Parse1Data6(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toDF()

  }

  def parse1Data8(lines: RDD[String]): DataFrame = {
    lines.map(x => x.split(' ').filter(_ != "").toList).map { cols => Parse1Data8(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat) }.toDF()

  }

  def parse1Data9(lines: RDD[String]): DataFrame = {

    lines.map(x => x.split(' ').filter(_ != "").toList).map { cols => Parse1Data9(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat) }.toDF()

  }

  def parse2Data9(lines: RDD[String]): DataFrame = {

    lines.map(x => x.split(' ').filter(_ != "").toList).map { cols => Parse2Data9(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat) }.toDF()

  }

  def parse2Data12(lines: RDD[String]): DataFrame = {

    lines.map(x => x.split(' ').filter(_ != "").toList).map { cols => Parse2Data12(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat, cols(6).toFloat, cols(7).toFloat, cols(8).toFloat, cols(9).toFloat, cols(10).toFloat, cols(11).toFloat) }.toDF()

  }

  def parse2Data6(lines: RDD[String]): DataFrame = {

    lines.map(x => x.split(' ').filter(_ != "").toList).map { cols => Parse2Data6(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toFloat, cols(4).toFloat, cols(5).toFloat) }.toDF()

  }

}