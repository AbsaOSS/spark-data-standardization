package za.co.absa.standardization.config

import za.co.absa.standardization.RecordIdGeneration

object DefaultMetadataColumnsConfig extends MetadataColumnsConfig {
  val addColumns: Boolean = true
  val prefix: String = "standardization"
  val recordIdStrategy: RecordIdGeneration.IdType = RecordIdGeneration.IdType.TrueUuids
}
