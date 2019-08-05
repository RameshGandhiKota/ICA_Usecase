
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

  case class recordStructure(year:Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float, tmax: Float, tmin: Float, tavg: Float)
  
  object recStructure{
    
    def apply(rec:Array[String]):recordStructrue = {
      //  val data = rec.map( x=> x.split(" ").filter( _ != ""))
      val yr = if( rec.length > 0 ) rec(0) else null
      val mn = if( rec.length > 1 ) rec(1) else null
      val dy = if( rec.length > 2 ) rec(2) else null
      val mg = if( rec.length > 3 ) rec(3) else null
      val at = if( rec.length > 4 ) rec(4) else null
      val eg = if( rec.length > 5 ) rec(5) else null
      val mi = if( rec.length > 6 ) rec(6) else null
      val mx = if( rec.length > 7 ) rec(7) else null
      val ag = if( rec.length > 8 ) rec(8) else null
      
      recordStructure(yr,mn,dy,mg,at,eg,mi,mx,ag)
    }
    
  }

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