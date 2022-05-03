package za.co.absa.standardization.config

case class BasicStandardizationConfig(failOnInputNotPerSchema: Boolean,
                                     errorCodes: ErrorCodesConfig,
                                     metadataColumns: MetadataColumnsConfig,
                                     errorColumn: String,
                                     timezone: String) extends StandardizationConfig

object BasicStandardizationConfig {
  def fromDefault(): BasicStandardizationConfig = {
    BasicStandardizationConfig(
      DefaultStandardizationConfig.failOnInputNotPerSchema,
      BasicErrorCodesConfig.fromDefault(),
      BasicMetadataColumnsConfig.fromDefault(),
      DefaultStandardizationConfig.errorColumn,
      DefaultStandardizationConfig.timezone
    )
  }
}
