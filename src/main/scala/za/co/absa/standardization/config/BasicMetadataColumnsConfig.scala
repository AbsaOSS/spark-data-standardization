package za.co.absa.standardization.config

import za.co.absa.standardization.RecordIdGeneration

case class BasicMetadataColumnsConfig(addColumns: Boolean,
                                     prefix: String,
                                     recordIdStrategy: RecordIdGeneration.IdType) extends MetadataColumnsConfig

object BasicMetadataColumnsConfig {
  def fromDefault(): BasicMetadataColumnsConfig = {
    BasicMetadataColumnsConfig(
      DefaultMetadataColumnsConfig.addColumns,
      DefaultMetadataColumnsConfig.prefix,
      DefaultMetadataColumnsConfig.recordIdStrategy
    )
  }
}
