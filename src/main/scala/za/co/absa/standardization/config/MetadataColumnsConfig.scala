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

import za.co.absa.standardization.RecordIdGeneration

trait MetadataColumnsConfig {
  def addColumns: Boolean
  def prefix: String
  def recordIdStrategy: RecordIdGeneration.IdType

  def reportDateFormat = "yyyy-MM-dd"

  def infoDateColumn = prefix + "_info_date"
  def infoDateColumnString = s"${infoDateColumn}_string"
  def infoVersionColumn = prefix + "_info_version"
  def recordId = prefix + "_record_id"
}
