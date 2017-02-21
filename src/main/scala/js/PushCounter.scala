package js

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object PushCounter extends App {

  val conf = new SparkConf()
    .setAppName("GitHub push counter")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)

  val x1: RDD[(Int, Int)] = sc.parallelize(List(2 -> 1))
  val z: RDD[(Int, Int)] = sc.parallelize(List(1 -> 2))



//  val y: RDD[String] = sc.parallelize(List("2"))
  x1.join(z)

  val sqlContext = new SQLContext(sc)
//
//  val fields: RDD[Array[String]] = sc.textFile("/home/icimpoes/dev/study/spark/src/main/resources/2015-01-01-15.json").map(_.split("\\|"))
//
//  val ids = fields map { line => (line(0), line(1), line(2), line(3)) }
//  val x = ids map { case (c, _, l, _) => c -> l } groupBy (_._2) map { case ((c, l)) => c -> l.size } sortBy(_._2, false)
//
//  val all = ids.groupBy { case (c, a, l, m) => (a, l, m) }.map { case ((z1, y)) => z1 -> y.toSet.map{ a: (String, String, String, String) => a._1}.size }
//  ids.groupBy(_._1).map { case ((c, l)) => c -> l.toSet.size }.sortBy(_._2, false)
//  ids.groupBy(_._1).map { case ((c, o)) => o.mkString(" :: ") -> c }
//
//  fields.distinct()

  val grouped = sqlContext.read.json("/home/icimpoes/dev/study/spark/src/main/resources/2015-01-01-15.json").filter("asda").groupBy("").count()
  val ordered = grouped.orderBy("count")

  val employees = Set("asdasd", "asdsaadsdsa")

  val bcEmployees = sc.broadcast(employees)

  import sqlContext.implicits._

  val isEmp = bcEmployees.value.contains _
  val sqlFunc = sqlContext.udf.register("SetContainsUdf", isEmp)
  val filtered = ordered.filter(sqlFunc($"login"))
  filtered.collect
}