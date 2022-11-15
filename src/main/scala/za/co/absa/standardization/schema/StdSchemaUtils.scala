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

package za.co.absa.standardization.schema

import org.apache.spark.sql.types._

object StdSchemaUtils {

  /**
    * Determine the name of a field
    * Will override to "sourcecolumn" in the Metadata if it exists
    *
    * @param field  field to work with
    * @return       Metadata "sourcecolumn" if it exists or field.name
    */
  def getFieldNameOverriddenByMetadata(field: StructField): String = {
    if (field.metadata.contains(MetadataKeys.SourceColumn)) {
      field.metadata.getString(MetadataKeys.SourceColumn)
    } else {
      field.name
    }
  }

  /**
    * Converts a fully qualified field name (including its path, e.g. containing fields) to a unique field name without
    * dot notation
    * @param path  the fully qualified field name
    * @return      unique top level field name
    */
  def unpath(path: String): String = {
    path.replace("_", "__")
        .replace('.', '_')
  }

  implicit class FieldWithSource(val structField: StructField) {
    def sourceName: String = {
      StdSchemaUtils.getFieldNameOverriddenByMetadata(structField.structField)
    }
  }



}
