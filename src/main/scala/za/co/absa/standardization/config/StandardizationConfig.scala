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

import com.typesafe.config.{Config, ConfigFactory}
import za.co.absa.standardization.RecordIdGeneration

trait StandardizationConfig {
  val failOnInputNotPerSchema: Boolean
  val errorCodes: ErrorCodesConfig
  val metadataColumns: MetadataColumnsConfig
  val errorColumn: String
  val timezone: String
}

//object StandardizationConfig {
//  def fromConfig(generalConfig: Config = ConfigFactory.load().getConfig("standardization")): StandardizationConfig = {
//    // TODO
//    val errorCodesConfig = ErrorCodesConfig.fromConfig(generalConfig.getConfig("errorCodes"))
//    val metadataColumnsConfig = MetadataColumnsConfig.fromConfig(generalConfig.getConfig("metadataColumns"))
//    val errorColumn = generalConfig.getString("errorColumn")
//
//
//    StandardizationConfig(
//      generalConfig.getBoolean("failOnInputNotPerSchema"),
//      errorCodesConfig,
//      metadataColumnsConfig,
//      errorColumn
//    )
//  }
//
//}
