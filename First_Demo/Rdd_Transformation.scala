package First_Demo

import First_Demo.Map.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SQLContext, SparkSession}
object Rdd_Transformation {

  def main(args: Array[String]) = {


    val spark = SparkSession.builder.appName("mapExample").master("local[*]").getOrCreate()

     val data = spark.sparkContext.textFile("C:\\Users\\indian\\Desktop\\new_training\\Rdd.file\\spark_test.txt")
    val mapfile = data.map(line =>(line, line.length))
      mapfile.collect.foreach(println)
  //  mapfile.foreach(println)



    //flatmap

    //The input and output RDDs will therefore typically be of different sizes for flatMap
    // rdd.flatMap(_.split(" "))
//  val c =sc.parallelize(List("my name ","dinesh"))
    // c.flatMap(x=> x).collect
//val flatmapFile = data.flatMap(lines => lines.split(" "))
//------------------filter----------------------
   // Array[String] = Array("my name ", dinesh)
    //c.filter(x => (x.contains("m"))).collect
    //b.filter(x => x.contains("e")).collect
    //val linesWithSpark = textFile.filter(line => line.contains("Spark"))
   // 2)
   // val xx = sc.parallelize(List(12,25,89,52))
   // xx.filter(x => ( x % 2 ==0)).collect
   // xx.filter(x => ( x % 2 ==1)).collect
   // xx.filter(x => ( x / 2 < 15)).collect
    //xx.filter(( _ / 2 > 15)).collect
   // http://data-flair.training/forums/topic/explain-the-filter-transformation
---------------mappartion-----------------
    Recollect map, flatMap, filter
    map : takes one record as input and returns one processed record as output
    flatMap : takes one record as input and returns one or more processed records as output
    filter : takes one record is input and either accepted or discarded based on the logic which return true or false
    mapPartitions : is API, in which all the data with in a RDD partition is treated as Scala collection
    There are only handful of APIs for transformations on RDD, but Scala provide lot more APIs for transformations on collections
    By using mapPartitions, with in each partition we can leverage Scala collection APIs to perform more complex transformations
      Also if we have to create regex parser to process the records, we do not need to create new parser object for each and every element in RDD. Using mapPartitions we can create one parser object and apply on all the elements in Iterator.
      Here is the example for mapPartitions
    //mappartion => work on partition not on single value
    scala> val parallel = sc.parallelize(1 to 9, 3)

    scala> parallel.mapPartitions( x => List(x.next).iterator).collect
      ANS:- Array[Int] = Array(1, 4, 7)

      // compare to the same, but with default parallelize
      scala> val parallel = sc.parallelize(1 to 9)
    scala> parallel.mapPartitions( x => List(x.next).iterator).collect
        ANS:- Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8)
    https://www.supergloo.com/fieldnotes/apache-spark-examples-of-transformations/#mapPartitionsWithIndex
    -----------------------mapPartitionIndex---------------------------
    val rdd1 =  sc.parallelize(
      |                List(
        |                   "yellow",   "red",
    |                   "blue",     "cyan",
    |                   "black"
    |                ),
    |                3
    |             )
    rdd1: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[10] at parallelize at :21

    scala>

    scala> val mapped =   rdd1.mapPartitionsWithIndex{
      |                   // 'index' represents the Partition No
      |                   // 'iterator' to iterate through all elements
      |                   //                         in the partition
      |                   (index, iterator) => {
        |                      println("Called in Partition -> " + index)
        |                      val myList = iterator.toList
        |                      // In a normal user case, we will do the
        |                      // the initialization(ex : initializing database)
        |                      // before iterating through each element
        |                      myList.map(x => x + " -> " + index).iterator
        |                   }
      |                }
    mapped: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[11] at mapPartitionsWithIndex at :23

    scala>
      | mapped.collect()
    Called in Partition -> 1
    Called in Partition -> 2
    Called in Partition -> 0
    res7: Array[String] = Array(yellow -> 0, red -> 1, blue -> 1, cyan -> 2, black -> 2)
  //------------------union-------------------  //
    With the union() function, we get the elements of both the RDD in new RDD.
    The key rule of this function is that the two RDDs should be of the same type.
    val z = sc.parallelize(List(1,2,3,4,32,34))
    val x = sc.parallelize(List("a","b","c","e","f","w"))
    val e = z.map(z => z.toString)
    x.union(e).collect
    // union => a.toDF.union(b.toDF).show

    //---------------------intersection---------------------
    With the intersection() function, we get only the common element of both the RDD in new RDD.
    The key rule of this function is that the two RDDs should be of the same type.
    val rdd1 = spark.sparkContext.parallelize(Seq((1,"jan",2016),(3,"nov",2014, (16,"feb",2014)))
    val rdd2 = spark.sparkContext.parallelize(Seq((5,"dec",2014),(1,"jan",2016)))
    val comman = rdd1.intersection(rdd2)
    comman.foreach(Println)
    output:---(1,jan,2016)-----showing common record
    -----------------------------Distinct---------------
    It returns a new dataset that contains the distinct elements of the source dataset.
    It is helpful to remove duplicate data.
    remove duplicte in singal record
    val q =sc.parallelize(List(1,2,3,34,4,3))
    q.distinct.collect
----------------------GroupByKey-------------------------
    When we use groupByKey() on a dataset of (K, V) pairs, the data is shuffled according to the key value K in another RDD.
      In this transformation, lots of unnecessary data get to transfer over the network.

    Spark provides the provision to save data to disk when there is more data shuffled onto a single executor machine than can fit in memory.
    val data = spark.sparkContext.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    val group = data.groupByKey().collect()
    group.foreach(println)

    //(s,CompactBuffer(3, 4))
(p,CompactBuffer(7, 5))
(t,CompactBuffer(8))
(k,CompactBuffer(5, 6))
------------------reduceByKey:---
//val a=sc.parallelize(List('a' , 'b', 'c', 'd', 'a', 'f', 'd', 'c', 'b', 'c'), 3)
// a.map(x=>(x,1)).reduceByKey((x,y)=>x+y).collect
  //  a.map(x=>(x,1)).reduceByKey((_+_).collect

   // ReduceByKey requird two  value ex:-(x,y)
    //for singal value Reduce by key is not working
    //val x =sc.parallelize(Array(1,2,3,4,5,6,5,8))
    //x.map(x=>(x,"a")).reduceByKey(_+_).collect

    2)
    val data = spark.sparkContext.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    data.reduceByKey(_+_).collect
    Ans:-  Array[(Char, Int)] = Array((s,7), (p,12), (t,8), (k,11))

   ---------------------------------- sortByKey()--------------------------------
    When we apply the sortByKey() function on a dataset of (K, V) pairs, the data is sorted according to the key K in another RDD.

    sortByKey() example:

    val data = spark.sparkContext.parallelize(Seq(("maths",52), ("english",75), ("science",82), ("computer",65), ("maths",85)))
    val sorted = data.sortByKey()
    sorted.foreach(println)
    output:-(computer,65)
    (english,75)
    (maths,52)
    (maths,85)
    (science,82)
    Note – In above code, sortByKey() transformation sort the data RDD into Ascending order of the Key(String).

      --------------------------------join-------------------------------------
    The Join is database term. It combines the fields from two table using common values. join() operation in Spark is defined on pair-wise RDD. Pair-wise RDDs are RDD in which each element is in the form of tuples. Where the first element is key and the second element is the value.

      The boon of using keyed data is that we can combine the data together. The join() operation combines two data sets on the basis of the key.

    Join() example:

    val data = spark.sparkContext.parallelize(Array(('A',1),('b',2),('c',3)))
    val data2 =spark.sparkContext.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
    val result = data.join(data2)
    println(result.collect().mkString(","))
    Note –  The join() transformation will join two different RDDs on the basis of Key.
    --------------------------------coalesce()-------------------------------------
    To avoid full shuffling of data we use coalesce() function. In coalesce() we use existing partition so that less data is shuffled. Using this we can cut the number of the partition. Suppose, we have four nodes and we want only two nodes. Then the data of extra nodes will be kept onto nodes which we kept.

    Coalesce() example:

    val rdd1 = spark.sparkContext.parallelize(Array("jan","feb","mar","april","may","jun"),3)
    val result = rdd1.coalesce(2)
    result.foreach(println)

    //--------------------see partition-----------------------
    //val a=sc.parallelize(List('a' , 'b', 'c', 'd', 'a', 'f', 'd', 'c', 'b', 'c'), 3)
    //-----------3-------is partitton---------
    //                    OR
    //a.partitions.length
    //                          OR
    //a.partitions.size
  }
//--------------------reduce---------------
  //val b = sc.parallelize(List("my name", "dinesh"))
  //b.map(x => (x,x.length)).collect
  //b.map(x => (x.length)).reduce((x,y) => x + y)
-----https://vishnuviswanath.com/spark-scala.html---
  reduce() operation in Spark is a bit different from how the Hadoop MapReduce used to be,
  reduce() in spark produces only single output instead of producing key-value pairs
    Here we ran reduce operation on an Array(firstWords) which is not on an RDD.
    reduceByKey() can only called on an RDD where as reduce() can be called even if the object is not an RDD.
}



------------------------------------Action----------------------------------------
