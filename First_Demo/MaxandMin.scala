package First_Demo

import scala.math.{abs, max}

object MaxandMin {
  def main(args: Array[String]) {
    var myList = Array(1.9,2.9, 3.4, 3.5)

    var max = myList(0);
    var min = myList(0);
    for ( i <- 1 to (myList.length - 1) )
    {
      println(i," -----",myList(i))
    if (myList(i) > max)

      {


      max = myList(i)

    }

      if (myList(i) < min)
        {

          min = myList(i)
        }
  }

  println("Max is " + max);
    println("Max is " + min);
  }


}
