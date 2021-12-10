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

package za.co.absa.standardization

import com.typesafe.config.{Config, ConfigFactory}
import za.co.absa.standardization.RecordIdGeneration.getRecordIdGenerationType

case class StandardizationConfig(recordIdGenerationStrategy: RecordIdGeneration.IdType,
                                 failOnInputNotPerSchema: Boolean) {

}

object StandardizationConfig {
  def fromConfig(generalConfig: Config = ConfigFactory.load()): StandardizationConfig = {
    StandardizationConfig(
      getRecordIdGenerationType(generalConfig.getString("standardization.recordId.generation.strategy")),
      generalConfig.getBoolean("standardization.failOnInputNotPerSchema")
    )
  }

}
