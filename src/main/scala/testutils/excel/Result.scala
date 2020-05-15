package testutils.excel

case class Result(
  val outcome: String = "unknown",
  val details: String = "could not run") {
  def getOutcome = outcome
  def getDetails = details
}
