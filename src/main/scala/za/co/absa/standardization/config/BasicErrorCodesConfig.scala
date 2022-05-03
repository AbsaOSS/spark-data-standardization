package za.co.absa.standardization.config

case class BasicErrorCodesConfig(castError: String,
                                nullError: String,
                                typeError: String,
                                schemaError: String) extends ErrorCodesConfig

object BasicErrorCodesConfig {
  def fromDefault(): ErrorCodesConfig = {
    BasicErrorCodesConfig(
      DefaultErrorCodesConfig.castError,
      DefaultErrorCodesConfig.nullError,
      DefaultErrorCodesConfig.typeError,
      DefaultErrorCodesConfig.schemaError
    )
  }
}
