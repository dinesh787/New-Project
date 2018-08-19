package First_Demo

object Map {


  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.SQLImplicits
  import org.apache.spark.sql.{SQLContext, SparkSession}
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions.explode


  import org.apache.spark.sql.functions.col



  val spark = SparkSession.builder().master("local[*]").appName("jkjk").getOrCreate()

  def main(args: Array[String]) {

      val rdd1 = spark.sparkContext.parallelize(List(1,2,3,4))
    val maprdd = rdd1.map(x => x + 5 )
   // rdd1.collect()
   maprdd.foreach(println)

 // val mapFile = rdd1.map(line => (line,line.length))
 //  mapFile.foreach(println)
  }



 // C:\Users\indian\Desktop\Rdd.file

  //val data = spark.read.textFile("C:\\Users\\indian\\Desktop\\Rdd.file\spark_test.txt").rdd
}
