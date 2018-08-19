package First_Demo

object MyList {


  def main(args: Array[String]) {

    val list = List (2,1,1,1,-1)
   // var myList = Array(1.9,2.9, 3.4, 3.5)
    var add = list(0)
    //var max = myList(0);
    //var min = myList(0);
    for ( i <- 1 to (list.length - 1) )
    {
      println(i," -----",List(i))
      if ( list(i) == 0)

      {


        add = add + list(i)
      }


    }

    println("add is " + add);

  }

}

 // def main(args: Array[String]): Unit ={
 //   val list = List (2,1,1,1)
 //   var sum = 0
  //  var mul =1
  //  list.foreach(println)
   // list.foreach(sum += _)
   // list.foreach(mul *= _)
   // println(list)
   // println(sum)
   // println(mul)

  //}

//}
