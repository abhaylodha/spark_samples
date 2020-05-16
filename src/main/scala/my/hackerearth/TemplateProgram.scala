package my.hackerearth

object TemplateProgram extends App {
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
    val scanned_text = scala.io.StdIn.readLine
    val count = scanned_text.toInt

  }

  /**
   * All required methods
   */

}
