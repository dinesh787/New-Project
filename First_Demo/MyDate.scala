package First_Demo

import java.util.Date
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.LocalDate


object MyDate {
  def main(args: Array[String]) {
  //  var x = 7
    //while (x > 0) {
      //println(x)
     // x = x - 1
    //}
    val format = new SimpleDateFormat("dd/MM/yyyy")
   // var sdate = format.parse("22/05/2018")
   // var edate = format.parse("22/08/2018")

    var sdate = LocalDate.parse("01/01/2018", DateTimeFormatter.ofPattern("MM/dd/yyyy"))
    val edate = LocalDate.parse("01/10/2018", DateTimeFormatter.ofPattern("MM/dd/yyyy"))

     while (sdate.compareTo(edate) < 0  )
   // while(sdate.equals(edate))
      {
       // println(sdate)
        sdate = sdate.plusDays(1)
      }

     sdate = LocalDate.parse("01/01/2018", DateTimeFormatter.ofPattern("MM/dd/yyyy"))

    for( a <- 1 to edate.compareTo(sdate))
    {
      println(sdate)
      sdate = sdate.plusDays(1)
    }

  }




}