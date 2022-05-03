package za.co.absa.standardization.config

object DefaultErrorCodesConfig extends ErrorCodesConfig {
  val castError: String = "E00000"
  val nullError: String = "E00002"
  val typeError: String = "E00006"
  val schemaError: String = "E00007"
}
