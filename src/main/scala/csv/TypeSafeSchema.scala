package csv
import org.apache.spark.sql.{DataFrame, Dataset}


object TypeSafeSchema extends App with SparkCsvDemo {

  import sparkSession.implicits._

  val users: DataFrame = sparkSession.read
    .schema(User.userSchema)
    .option("header", "true")
    .option("delimiter", ",")
    .csv("src/main/resources/users.csv")

  val userDS: Dataset[User] = users.as[User]

  userDS.printSchema()

  userDS.map(u => u.name -> u.age).show()

}
