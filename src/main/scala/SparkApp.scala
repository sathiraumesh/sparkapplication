
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkApp {


  def main(args: Array[String])
  {
    // logs of the app is disabled
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("Second").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("/Users/sathiraumesh/Desktop/SparkApp/breast_cancer_clinical_data.csv")
    averageAgeOfInitialDiagnosis(rdd)
    println("")
    averageInEachAJCCStage(rdd)
    println("")
    peopleWithVitalStatus(rdd)


  }

  def averageAgeOfInitialDiagnosis(data:RDD[String]): Unit ={
    val header = data.first()
    val remove_header = data.filter(x => x!=header)
    val avg_age = remove_header.map(x => x.split(",")).map(x => x(2).toInt).reduce(_+_)/remove_header.count
    println("Average age for Initial Diagnosis "+avg_age)
  }

  def averageInEachAJCCStage(data:RDD[String]): Unit ={
    val header = data.first()
    val remove_header = data.filter(x => x!=header)
    println("average people In Each AJCSS stage")
    val stages = remove_header.map(x => x.split(",")).map(x => (x(11),x(15).toInt)).mapValues((_, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues{ case (sum, count) => (1.0 * sum)/count}.foreach(println)

  }


  def peopleWithVitalStatus(data :RDD[String]):Unit={
    val header = data.first()
    val remove_header = data.filter(x => x!=header)
    println("people with vital status")
    val status = remove_header.map(x => x.split(",")).map(x => (x(14),1)).reduceByKey(_+_).foreach(println)
  }

}
