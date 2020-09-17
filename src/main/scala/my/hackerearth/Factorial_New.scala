package my.hackerearth

object Factorial_New extends App {
    def factorial(n : Long) : Long = {

        def factorial(acc : Long, n : Long) : Long = {
            if (n<=1)
                acc
            else
                factorial(acc * n, n - 1)
        }

        factorial(1,n)

    }

    val number = scala.io.StdIn.readLine().toLong
    
    println(factorial(number))
}
