package my.hackerearth

import scala.annotation.tailrec

object Factorial extends App {

  def factorial(n: Long): Long = {

    @tailrec
    def factorial(acc: Long, n: Long): Long = {
      if (n <= 1)
        acc
      else
        factorial(acc * n, n - 1)
    }
    factorial(1L, n)
  }

  val number = scala.io.StdIn.readLine().toLong
  println(factorial(number))

}