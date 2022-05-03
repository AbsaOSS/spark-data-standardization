package za.co.absa.standardization.config

import com.typesafe.config.Config

trait ErrorCodesConfig extends Serializable {
  val castError: String
  val nullError: String
  val typeError: String
  val schemaError: String
}

//object ErrorCodesConfig {
//  def fromConfig(config: Config): ErrorCodesConfig = {
//    val castError    = config.getString("castError")
//    val nullError    = config.getString("nullError")
//    val typeError    = config.getString("typeError")
//    val schemaError  = config.getString("schemaError")
//
//    ErrorCodesConfig(castError, nullError, typeError, schemaError)
//  }
//}
