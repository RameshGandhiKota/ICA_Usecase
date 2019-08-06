package com.ica.gruppen.web2hdfs

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{ SparkSession, SQLContext, DataFrame }

import com.ica.gruppen.web2hdfs.LoadFileGenericFlow._

object LoadHDFSFromWeb2_v2 {

  def main(args: Array[String]): Unit = {

    val nargs = 4
    //val nargs = 6
    
    if (args.length < nargs) {
      /*  System.err.println(s"""
                |Usage: LoadHDFSFromWeb2_v2 <FileHTTPUrl> <ColumnNames seperate with single space> <ColumnDataTypes seperate with single space> <File data delimiter> <Target Hive table name> <Target Hive table load type: append/overwrite>
                """.stripMargin)*/
      System.err.println(s"""
                |Usage: LoadHDFSFromWeb2_v2 <FileHTTPUrl> <OutputHiveTableName> <FileSetNumber 1 or 2> <Target Hive table load type: append/overwrite> 
                """.stripMargin)
      System.exit(1)
    }

    val Array(filePath, tblName, fileSetNum, loadType) = args
    // val Array(fileName, colNames, colTypes, dlmtrChar, tblName, loadType) = args

    //val filePath = "/tmp/temp/data/stockholmA_barometer_2013_2017.txt"
    //val tblName = filePath.substring(filePath.lastIndexOf('/') + 1).replaceAll(".txt", "")
    loadFileGenericFunc(filePath, tblName, fileSetNum.toInt, loadType)

    // End of main block
  }

}