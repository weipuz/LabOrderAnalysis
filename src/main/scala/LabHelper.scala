package vch.ds.bigdata


import java.io._
import scala.collection.mutable.ArrayBuffer
import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.apache.spark.rdd.RDD


object LabHelper {

def isNumeric2(input: String): Boolean = input.forall(_.isDigit)

//function to detect if a given sting could be convert to double or long int
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
//function to split result string by delimiter "\.br\". Using double \\ to declare it is not escape character. 
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

/********group result by test type and reduce ***************/
def groupByTestTypes(rdd : RDD[Array[Any]]): Seq[(String,Int)] = {
  rdd.map(row => (row(2).toString(),1)).reduceByKey(_+_).collect.toSeq.sortBy(_._2).reverse
}
/*******get result summary of that group of records********/
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



/***************June 17 **********************/
/** Compute sentiment(contains negative senti word patterns) for those result contains "detected" or "present" */

def sentimentOfResult(rdd : RDD[org.apache.spark.sql.Row], resultGroup:String) : ArrayBuffer[(String,Long)] = {

  val DataToSave = ArrayBuffer[(String,Long)]()
  val neg_senti_pattern="""\bno\b|\bnot\b|\bnon|\bundetect""".r
  val present_pattern = """present""".r
  val detect_pattern = """detect""".r
  val pos_pattern="""\b(?i)pos""".r
  val neg_pattern="""\b(?i)neg""".r
  

  val rdd_present = rdd.filter(row => present_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_present_no = rdd_present.filter(row => neg_senti_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)

  val rdd_detect = rdd.filter(row => detect_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_detect_no = rdd_detect.filter(row => neg_senti_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)

  val rdd_both = rdd.filter(row => detect_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined && present_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_both_no = rdd_both.filter(row => neg_senti_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)

  DataToSave += ((resultGroup+"_present",rdd_present.count))
  DataToSave += ((resultGroup+"_present_no",rdd_present_no.count))
  DataToSave += ((resultGroup+"_detect",rdd_detect.count))
  DataToSave += ((resultGroup+"_detect_no",rdd_detect_no.count))
  DataToSave += ((resultGroup+"_both",rdd_both.count))
  DataToSave += ((resultGroup+"_both_no",rdd_both_no.count))
  return DataToSave
}

def posnegOfResult(rdd : RDD[org.apache.spark.sql.Row], resultGroup:String) : ArrayBuffer[(String,Long)] = {

  val DataToSave = ArrayBuffer[(String,Long)]()
  val neg_senti_pattern="""\bno\b|\bnot\b|\bnon|\bundetect""".r
  val present_pattern = """present""".r
  val detect_pattern = """detect""".r
  val pos_pattern="""\b(?i)pos""".r
  val neg_pattern="""\b(?i)neg""".r
  

  val rdd_present = rdd.filter(row => present_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_present_pos = rdd_present.filter(row => pos_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_present_neg = rdd_present.filter(row => neg_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)

  val rdd_detect = rdd.filter(row => detect_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_detect_pos = rdd_detect.filter(row => pos_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_detect_neg = rdd_detect.filter(row => neg_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  DataToSave += ((resultGroup+"_present",rdd_present.count))
  DataToSave += ((resultGroup+"_present_pos",rdd_present_pos.count))
  DataToSave += ((resultGroup+"_present_neg",rdd_present_neg.count))
  DataToSave += ((resultGroup+"_detect",rdd_detect.count))
  DataToSave += ((resultGroup+"_detect_pos",rdd_detect_pos.count))
  DataToSave += ((resultGroup+"_detect_neg",rdd_detect_neg.count))
  DataToSave += ((resultGroup+"_total",rdd.count))

  return DataToSave
}

def sentimentOfResultForTestType(rddAll : RDD[org.apache.spark.sql.Row], testName:String) : ArrayBuffer[(String,Long)] = {

  val DataToSave = ArrayBuffer[(String,Long)]()
  val neg_senti_pattern="""\bno\b|\bnot\b|\bnon|\bundetect""".r
  val present_pattern = """present""".r
  val detect_pattern = """detect""".r
  val pos_pattern="""\b(?i)pos""".r
  val neg_pattern="""\b(?i)neg""".r
  val rdd = rddAll.filter(row => row(1).toString().equals(testName))
  val resultGroup = testName.replaceAll(" ","")

  val rdd_present = rdd.filter(row => present_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_present_no = rdd_present.filter(row => neg_senti_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)

  val rdd_detect = rdd.filter(row => detect_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_detect_no = rdd_detect.filter(row => neg_senti_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)

  val rdd_both = rdd.filter(row => detect_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined && present_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_both_no = rdd_both.filter(row => neg_senti_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)

  DataToSave += ((resultGroup+"_present",rdd_present.count))
  DataToSave += ((resultGroup+"_present_no",rdd_present_no.count))
  DataToSave += ((resultGroup+"_detect",rdd_detect.count))
  DataToSave += ((resultGroup+"_detect_no",rdd_detect_no.count))
  DataToSave += ((resultGroup+"_both",rdd_both.count))
  DataToSave += ((resultGroup+"_both_no",rdd_both_no.count))
  return DataToSave
}

def posnegOfResultForTestType(rddAll : RDD[org.apache.spark.sql.Row], testName:String) : ArrayBuffer[(String,Long)] = {

  val DataToSave = ArrayBuffer[(String,Long)]()
  val neg_senti_pattern="""\bno\b|\bnot\b|\bnon|\bundetect""".r
  val present_pattern = """present""".r
  val detect_pattern = """detect""".r
  val pos_pattern="""\b(?i)pos""".r
  val neg_pattern="""\b(?i)neg""".r
  val rdd = rddAll.filter(row => row(1).toString().equals(testName))
  val resultGroup = testName.replaceAll(" ","")

  val rdd_present = rdd.filter(row => present_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_present_pos = rdd_present.filter(row => pos_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_present_neg = rdd_present.filter(row => neg_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)

  val rdd_detect = rdd.filter(row => detect_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_detect_pos = rdd_detect.filter(row => pos_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  val rdd_detect_neg = rdd_detect.filter(row => neg_pattern.findFirstIn(row(0).toString().toLowerCase).isDefined)
  DataToSave += ((resultGroup+"_present",rdd_present.count))
  DataToSave += ((resultGroup+"_present_pos",rdd_present_pos.count))
  DataToSave += ((resultGroup+"_present_neg",rdd_present_neg.count))
  DataToSave += ((resultGroup+"_detect",rdd_detect.count))
  DataToSave += ((resultGroup+"_detect_pos",rdd_detect_pos.count))
  DataToSave += ((resultGroup+"_detect_neg",rdd_detect_neg.count))
  DataToSave += ((resultGroup+"_total",rdd.count))

  return DataToSave

}

}