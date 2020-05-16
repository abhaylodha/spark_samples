package my.hackerearth

object PerfectNumber extends App {
  /**
   * Try catch to avoid NZEC (Non-Zero Exit Code) Exceptions
   */
  try {
    val start = System.currentTimeMillis()
    run
    val end = System.currentTimeMillis()
    //println("Duration : " + (end - start) / 1000)
  } catch {
    case e: Exception => {
      println("Exception occurred.")
      println(e.getMessage)
      println(e.getStackTraceString)
    }
  }

  /**
   * Entry point of the program
   */

  def run = {
    val t = scala.io.StdIn.readLine
    val count = t.toShort

    var counter = 1;
    val result = for (i <- 1 to count) yield {
      val number = scala.io.StdIn.readLine().toLong
      //println("Working on " + number)
      //println("counter " + counter)
      counter = counter + 1
      if (isPefectNumber(number)) {
        number
      } else {
        -1
      }
    }
    result.foreach(item => if (item != -1) println("YES") else println("NO"))
  }

  /**
   * All required methods
   */
  def getDivisors(n: Long) = {

    val step = if (n % 2 == 0)
      1
    else
      2

    val numbers = for (i <- 1L to ((n / 2), step) if (n % i == 0)) yield {
      i
    }
    numbers
  }

  def isPefectNumber(n: Long): Boolean = {
    if (n < 0)
      return false

    val numbers = getDivisors(n).toSeq
    if ((numbers.foldLeft(0L)((acc, n) => acc + n)) == n)
      true
    else
      false

  }

}
