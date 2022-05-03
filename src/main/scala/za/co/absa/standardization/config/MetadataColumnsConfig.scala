/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
