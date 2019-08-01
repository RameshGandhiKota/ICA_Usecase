
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Web2HDFS {
  //extends App{
  
  //println("Beginning of the program before MAIN block start executing!")
  
  def downloadFromWeb = {
    
  }
  
  def cleanseHDFSFile = {
    
  }
  
  def loading2HDFSArea = {
    
    
  }
  
  //val data = scala.io.Source.fromURL("")
  
  def main(args: Array[String]): Unit = {
    println("Main block started")
    val sc = new SparkContext()
    
    val fileURL =  """https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt""" 
    
    // downlaoding files from WEB
    /*val html = scala.io.Source.fromURL("https://spark.apache.org/").mkString
		val list = html.split("\n").filter(_ != "")
		val rdds = sc.parallelize(list)
		val count = rdds.filter(_.contains("Spark")).count()*/
    
    val loadData = scala.io.Source.fromURL(fileURL).mkString.split("\n") //.filter(_ =!= "")
    val rawData = sc.parallelize(loadData)
    
    downloadFromWeb
    
    
    // Placing the files in HDFS staging area, if any.
    //cleanseHDFSFile
    
    // Placing the cleansed files in target HDFS area
    //loading2HDFSArea    
    
    println("End of MAIN block")
  }
  
}