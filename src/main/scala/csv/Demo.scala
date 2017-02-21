package csv

import org.apache.spark.sql.functions._

object Demo extends App with SparkCsvDemo {

  val users = sparkSession.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("src/main/resources/users.csv")

  val locations = sparkSession.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("src/main/resources/locations.csv")

  val userLocation = sparkSession.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("src/main/resources/user_location.csv")


  val usersFromList =
    users
      .join(userLocation, users("id") === userLocation("userid"))
      .join(locations, locations("id") === userLocation("locationid"))
      .where(users("name").isin("James", "John"))
      .orderBy(desc("age"))

  usersFromList.show()

  users.createTempView("users")

  sparkSession.sql(
    "SELECT name, age, CASE WHEN age > 60 THEN 'old' ELSE 'young' END AS old_or_young FROM users"
  ).show()


}
