package vch.ds.bigdata

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io._
import scala.collection.mutable.ArrayBuffer

object LabTest {

/**********helper functions **********/
/** function to detect if a given string is integer */
def isNumeric2(input: String): Boolean = input.forall(_.isDigit)

/** function to detect if a given sting could be convert to double or long int */
def throwsNumberFormatException(f: => Any): Boolean = {
  try { f; false } catch { case e: NumberFormatException => true }
}
def isNumeric(str: String): Boolean = {
  !throwsNumberFormatException(str.toLong) || !throwsNumberFormatException(str.toDouble)
}

def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
  val p = new java.io.PrintWriter(f)
  try { op(p) } finally { p.close() }
}
/** function to split result string by delimiter "\.br\". Using double \\ to declare it is not escape character. */
def splitResult(str: String): Array[String] = {
  val result = str.split("""\\.br\\""")
  if (result.length == 2) {
  return Array(result(0), result(1))
  }
  else if(result.length > 2) {
  return Array(result(0), result.tail.mkString("""\.br\"""))
  }
  else{
  return Array(result(0), null)
  }

}


/** group result by test type and reduce */
def groupByTestTypes(rdd : RDD[Array[Any]]): Seq[(String,Int)] = {
  rdd.map(row => (row(2).toString(),1)).reduceByKey(_+_).collect.toSeq.sortBy(_._2).reverse
}

/** get result summary of that group of records */
def getSummaryOnly(rdd : RDD[Array[Any]], resultGroup:String) : ArrayBuffer[(String,Long)] = {

  val DataToSave = ArrayBuffer[(String,Long)]()
  val pos_pattern="""\b(?i)pos""".r
  val neg_pattern="""\b(?i)neg""".r
  val range_pattern="""\>|\<|\=""".r
  val unit_pattern="""[0-9]\s?[a-zA-Z]+\/[a-zA-Z]+""".r

  val rdd_numeric = rdd.filter(row => isNumeric(row(0).toString()))
  var rdd_other = rdd.filter(row => !isNumeric(row(0).toString()))

  val rdd_posneg = rdd_other.filter(row => neg_pattern.findFirstIn(row(0).toString()).isDefined || pos_pattern.findFirstIn(row(0).toString()).isDefined)
  rdd_other = rdd_other.filter(row => !(pos_pattern.findFirstIn(row(0).toString()).isDefined) && !(neg_pattern.findFirstIn(row(0).toString()).isDefined))

  val rdd_range = rdd_other.filter(row => range_pattern.findFirstIn(row(0).toString()).isDefined)
  rdd_other = rdd_other.filter(row => !range_pattern.findFirstIn(row(0).toString()).isDefined)

  val rdd_unit = rdd_other.filter(row => unit_pattern.findFirstIn(row(0).toString()).isDefined)
  rdd_other = rdd_other.filter(row => !unit_pattern.findFirstIn(row(0).toString()).isDefined)

  DataToSave += ((resultGroup+"_numeric",rdd_numeric.count))
  DataToSave += ((resultGroup+"_posneg",rdd_posneg.count))
  DataToSave += ((resultGroup+"_range",rdd_range.count))
  DataToSave += ((resultGroup+"_unit",rdd_unit.count))
  DataToSave += ((resultGroup+"_other",rdd_other.count))
  return DataToSave
}


/*******get result summary of that group of records********/
def getSummary(rdd : RDD[Array[Any]], resultGroup:String) : (ArrayBuffer[(String,Long)],RDD[Array[Any]]) = {

  val DataToSave = ArrayBuffer[(String,Long)]()
  val pos_pattern="""\b(?i)pos""".r
  val neg_pattern="""\b(?i)neg""".r
  val range_pattern="""\>|\<|\=""".r
  val unit_pattern="""[0-9]\s?[a-zA-Z]+\/[a-zA-Z]+""".r

  val rdd_numeric = rdd.filter(row => isNumeric(row(0).toString()))
  var rdd_other = rdd.filter(row => !isNumeric(row(0).toString()))

  val rdd_posneg = rdd_other.filter(row => neg_pattern.findFirstIn(row(0).toString()).isDefined || pos_pattern.findFirstIn(row(0).toString()).isDefined)
  rdd_other = rdd_other.filter(row => !(pos_pattern.findFirstIn(row(0).toString()).isDefined) && !(neg_pattern.findFirstIn(row(0).toString()).isDefined))

  val rdd_range = rdd_other.filter(row => range_pattern.findFirstIn(row(0).toString()).isDefined)
  rdd_other = rdd_other.filter(row => !range_pattern.findFirstIn(row(0).toString()).isDefined)

  val rdd_unit = rdd_other.filter(row => unit_pattern.findFirstIn(row(0).toString()).isDefined)
  rdd_other = rdd_other.filter(row => !unit_pattern.findFirstIn(row(0).toString()).isDefined)

  DataToSave += ((resultGroup+"_numeric",rdd_numeric.count))
  DataToSave += ((resultGroup+"_posneg",rdd_posneg.count))
  DataToSave += ((resultGroup+"_range",rdd_range.count))
  DataToSave += ((resultGroup+"_unit",rdd_unit.count))
  DataToSave += ((resultGroup+"_other",rdd_other.count))
  return (DataToSave, rdd_other)
}



def main(args: Array[String]) {

  if (args.size !=3 ){
    println("Input argument must be: TableName, UserName, Password")
    return
  }

  val size = args(0)
  val UserName = args(1)
  val Password = args(2)
  val url = "jdbc:sqlserver://STDBDECSUP01;databaseName=BigData;user="+UserName+";password="+Password
  val tableName = "dbo.LabTest"+size

  val conf = new SparkConf()
               .setAppName("LabTest")

  val sc = new SparkContext(conf) // An existing SparkContext.
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val startTime = System.currentTimeMillis

  /** load data from SQL SERVER into jdbcDF */
  val jdbcDF = sqlContext.load("jdbc", Map(
    "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "url" -> url,
    "dbtable" -> tableName))

  val dbLoadTime = System.currentTimeMillis
  val resultrdd = jdbcDF.select("Result","Test_Name").rdd
  val split_result = resultrdd.map(row => (splitResult(row(0).toString()):+row(1)))

  /*******************************************/
  /** data process for "result only" records */ 

  val resultOnly = split_result.filter(row => row(1)==null)
  val resultWithComment = split_result.filter(row => row(1)!=null)

  //var d1 = getSummaryOnly(resultOnly,"resultOnly")
  var (d1, resultOnly_other) = getSummary(resultOnly,"resultOnly")
  //var d2 = getSummaryOnly(resultWithComment,"resultWithComment")
  var (d2, resultWithComment_other)= getSummary(resultWithComment,"resultWithComment")

  resultOnly_other
  resultWithComment_other

  val endTime = System.currentTimeMillis
  println("Result for" + size + "records:")
  println("Total run time: " + ((endTime-startTime)).toString)
  println("Time to Load DB: " + ((dbLoadTime-startTime)).toString)

  

}
}
