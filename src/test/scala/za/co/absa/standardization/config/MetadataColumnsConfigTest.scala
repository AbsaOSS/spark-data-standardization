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

import org.scalatest.funsuite.AnyFunSuiteLike
import za.co.absa.standardization.RecordIdGeneration
import za.co.absa.standardization.types.CommonTypeDefaults

class MetadataColumnsConfigTest extends AnyFunSuiteLike {

  test("Test DefaultStandardizationConfig") {
    val conf = DefaultStandardizationConfig
    assert(conf.errorColumn == "errCol")
    assert(!conf.failOnInputNotPerSchema)
    assert(conf.timezone == "UTC")

    assert(conf.errorCodes.castError == "E00000")
    assert(conf.errorCodes.nullError == "E00002")
    assert(conf.errorCodes.typeError == "E00006")
    assert(conf.errorCodes.schemaError == "E00007")

    assert(conf.metadataColumns.addColumns)
    assert(conf.metadataColumns.prefix == "standardization")
    assert(conf.metadataColumns.recordIdStrategy == RecordIdGeneration.IdType.TrueUuids)
    assert(conf.metadataColumns.reportDateFormat == "yyyy-MM-dd")
    assert(conf.metadataColumns.infoDateColumn == "standardization_info_date")
    assert(conf.metadataColumns.infoDateColumnString == "standardization_info_date_string")
    assert(conf.metadataColumns.infoVersionColumn == "standardization_info_version")
    assert(conf.metadataColumns.recordId == "standardization_record_id")

    assert(conf.typeDefaults == CommonTypeDefaults)


  }

}
