package csv

import org.apache.spark.sql.functions._

object MoreExamples extends App with SparkCsvDemo {

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


  users.createTempView("users")
  userLocation.createTempView("user_location")


  val usersWhoLiveTogether =
    userLocation
      .join(users, users("id") === userLocation("userid"))
      .join(locations, locations("id") === userLocation("locationid"))
      .groupBy("locationid", "address")
      .agg(col("address"), countDistinct("userid").as("number_of_users"), collect_list("name").as("live_together"))
      .select("live_together", "address")
      .where("number_of_users > 1")

  usersWhoLiveTogether.show()



  val usersWithNoLocation =
    sparkSession.sql(
      """
        |SELECT name FROM users u
        | LEFT JOIN user_location ul ON u.id = ul.userid
        | WHERE ul.locationid IS NULL
      """.stripMargin)

  usersWithNoLocation.show()


}
