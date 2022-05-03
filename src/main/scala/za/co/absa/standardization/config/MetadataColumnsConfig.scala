package za.co.absa.standardization.config

import com.typesafe.config.Config
import za.co.absa.standardization.RecordIdGeneration

trait MetadataColumnsConfig {
  val addColumns: Boolean
  val prefix: String
  val recordIdStrategy: RecordIdGeneration.IdType

  // TODO Final?
  val infoDateColumn = prefix + "_info_date"
  val infoDateColumnString = s"${infoDateColumn}_string"
  val reportDateFormat = "yyyy-MM-dd"
  val infoVersionColumn = prefix + "_info_version"
  val recordId = prefix + "_record_id"
}

//object MetadataColumnsConfig {
//  def fromConfig(config: Config): MetadataColumnsConfig = {
//    val recordIdStrategy = RecordIdGeneration.getRecordIdGenerationType(
//      config.getString("recordId.generationStrategy")
//    )
//
//    MetadataColumnsConfig(config.getString("prefix"), recordIdStrategy)
//  }
//}
