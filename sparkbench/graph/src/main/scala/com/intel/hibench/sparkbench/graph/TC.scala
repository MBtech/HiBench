package com.intel.hibench.sparkbench.graph
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import scala.io.Source
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.SparkSession
import com.intel.hibench.sparkbench.common.IOCommon

// Triangle Counting
object TC {
  def main(args: Array[String]) {
    // Start Spark.
    println("\n### Starting Spark\n")
    val sparkConf = new SparkConf().setAppName("Triangle Counting").set("spark.cassandra.connection.host", IOCommon.getProperty("hibench.cassandra.host").fold("")(_.toString))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext

    // Suppress unnecessary logging.
    Logger.getRootLogger.setLevel(Level.ERROR)

    // Load a graph.
    val path = args(0)
    // Number of partitions
    val numPartitions = args(1).toInt

    println(s"${DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)} Loading edge list: ${path}\n")
    // Source.fromFile(path).getLines().foreach(println)

    val g: Graph[Int, Int] = GraphLoader.edgeListFile(
      sc,
      path,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK,
      numEdgePartitions = numPartitions
    )

    val gp = g.partitionBy(PartitionStrategy.fromString("RandomVertexCut"), numPartitions)
    val triCounts = gp.triangleCount().vertices


    val triCountsDF = triCounts.toDF("vid","value")
    triCountsDF.take(10).foreach(println)
    triCountsDF.write.format("org.apache.spark.sql.cassandra").options(Map("table"->"tc", "keyspace"->"test")).save()
    // Stop Spark.
    sc.stop()
  }
}
