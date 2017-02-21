package csv

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkCsvDemo {

  val sparkSession = SparkSession.builder
    .master("local")
    .appName("spark sql demo")
    .getOrCreate()

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

}
