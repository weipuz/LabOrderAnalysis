package vch.ds.bigdata

import java.io._

import scala.collection.mutable.ArrayBuffer

import com.microsoft.sqlserver.jdbc.SQLServerDriver

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel,AssociationRules}
import org.apache.spark.mllib.fpm.AssociationRulesWithLift
import org.apache.spark.mllib.fpm.AssociationRulesWithMeasures

import vch.ds.bigdata.LabHelper._

object ARulesTest {

def convertToSet(order :List[(String,String)]) : Array[String]= {
  val batteryList = List("CBC & Differential",
    "Complete Blood Count",
    "Arterial Gas Plus O2HB",
    "Susceptibility",
    "BLD CULT C&S Routine",
    "Urine C&S",
    "Calcium,Ionized Syringe",
    "Creatinine,Blood",
    "MRSA CULT Screen"
    );
  val batteryExcludeList = List("Electrolytes"
    );
   //order.map{case (battery,test) => if(batteryList.contains(battery)) battery else test}.distinct.toArray
   order.map{case (battery,test) => if(battery==null || battery.isEmpty || batteryExcludeList.contains(battery)) test else battery}.distinct.toArray
}

def main(args: Array[String]) {
	if (args.size !=3 ){
    println("Input argument must be: TableName, UserName, Password")
    return
  }

  val dataset = args(0)
  val UserName = args(1)
  val Password = args(2)
  val url = "jdbc:sqlserver://STDBDECSUP01;databaseName=BigData;user="+UserName+";password="+Password
  val tableName = "dbo.LabTest"+dataset

  val conf = new SparkConf()
               .setAppName("LabTest")

  val sc = new SparkContext(conf) // An existing SparkContext.
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val jdbcDF = sqlContext.load("jdbc", Map(
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "url" -> url,
  "dbtable" -> tableName))

	val resultrdd = jdbcDF.select("Result","Test_Name","Resulting_Lab_Location", "Resulting_Lab_Code","Battery_Code_Ordered","Test_Code_Ordered","Ordered_Datetime","Order_Number","Patient_MRN","Battery_Name").rdd

	val orderRdd = resultrdd.map(row => (row(8).toString()+"::"+row(6).toString(),(row(9).toString,row(1).toString)))
	val orderListRdd = orderRdd.map{ case (orderId,(battery,test)) => (orderId,List((battery,test)))}.reduceByKey((a, b) => a ++ b)
	//printToFile(new File("orderTestCount.txt")) { p =>
	//orderListRdd.map{ case (orderId,orderList) => (orderList.length,1)}.reduceByKey(_+_).map{ case (numOfTestContained, numOfCases) => s"$numOfTestContained\t$numOfCases"}.collect.foreach(p.println)
	//}


	val orderListElements: RDD[Array[String]] = orderListRdd.map{ case (orderId,orderList) => convertToSet(orderList)}
/*
	orderListElements.flatMap(orderList => orderList).map(word => (word, 1)).reduceByKey((a, b)=> a+b)
	.collect.toSeq.sortBy(_._2).reverse.map{ case (str, int) => s"$str\t$int"}.take(20).foreach(println(_))

	printToFile(new File("orderBatteryTestCountAllGroupedFY13.txt")) { p =>
	orderListElements.map{ orderList => (orderList.length,1)}.reduceByKey(_+_).map{ case (numOfTestContained, numOfCases) => s"$numOfTestContained\t$numOfCases"}.collect.foreach(p.println)
	}
*/


	val fpg = new FPGrowth()
	fpg.setMinSupport(0.05)
	fpg.setNumPartitions(8)
	orderListElements.cache()

	val startTime = System.currentTimeMillis
	val numberOfOrders = orderListElements.count()
	val model = fpg.run(orderListElements)

	printToFile(new File("freqItemsets_"+dataset+".txt")) { p => {
	model.freqItemsets.collect().foreach { itemset =>
	p.println(itemset.items.mkString("[", "::", "]") + "\t" + itemset.freq.toDouble /numberOfOrders)
	}}}

	val endTimeItemset = System.currentTimeMillis

	/*******************************************************/
	//val arules = new AssociationRules()
	val arules = new AssociationRulesWithMeasures()
	arules.setMinConfidence(0.8)
	arules.setNumTranscation(numberOfOrders)
	val rules = arules.run(model.freqItemsets)

	val endtimeARules = System.currentTimeMillis

	printToFile(new File("ARules_"+dataset+"_measures.txt")) { p => {
	rules.collect().foreach { rule =>
	p.println(rule.antecedent.mkString("[", "::", "]") + "\t==>\t" + rule.consequent.mkString("[", "::", "]")+ "\t" + rule.support + "\t" + rule.confidence + "\t" + rule.lift + "\t" + rule.conviction )
	}}}


	val endTime = System.currentTimeMillis
	println("Total run time: " + ((endTime-startTime)).toString)
	println("Run time to generate frequentItemsets: " + ((endTimeItemset-startTime)).toDouble / 1000 + " seconds")
	println("Run time to generate associationRules: " + ((endTime-endTimeItemset)).toDouble / 1000 + " seconds")
	/*******************************************************/







}


}