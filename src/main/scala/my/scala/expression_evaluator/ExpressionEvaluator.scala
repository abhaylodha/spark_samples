package my.scala.expression_evaluator

import my.spark.common_utils.Logger

class ExpressionEvaluator

class BooleanExpressionEvaluator(expressionString: String) extends ExpressionEvaluator {

  def evaluate: Boolean = {
    expressionString
    true

  }
  def and(that: BooleanExpressionEvaluator): Boolean = this.evaluate && that.evaluate
  def or(that: BooleanExpressionEvaluator): Boolean = this.evaluate || that.evaluate

  private def in(valuetoCheck: String, setOfStrings: Seq[String]): Boolean = {
    setOfStrings.contains(valuetoCheck)
  }

}

object ExpressionEvaluator extends Logger {
  def main(args: Array[String]): Unit = {
    val parenth_contents = raw"(?<=\()[^)]+(?=\))".r
    val parenth_contents(r) = """(
    (Expr1)
    _AND_ (Expr2 _OR_ Expr3)
    _AND_ (Expr4)
    _AND_ (Expr5)
    )
    """

    info(r)
  }
  def apply(): ExpressionEvaluator = new ExpressionEvaluator()

}
