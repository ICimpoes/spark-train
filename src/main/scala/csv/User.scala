package csv

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


case class User(id: String, name: String, email: String, age: Int)

object User {
  val userSchema = StructType(Array(
    StructField("id", DataTypes.StringType),
    StructField("name", DataTypes.StringType),
    StructField("email", DataTypes.StringType),
    StructField("age", DataTypes.IntegerType)
  ))
}