/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.standardization

object Constants {
  // ToDo Configurable
  final val InfoDateColumn = "standardization_info_date"
  final val InfoDateColumnString = s"${InfoDateColumn}_string"
  final val ReportDateFormat = "yyyy-MM-dd"
  final val InfoVersionColumn = "standardization_info_version"
  final val EnceladusRecordId = "standardization_record_id"
}
