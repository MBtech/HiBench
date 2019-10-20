/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.sparkbench.graph

import java.io._

import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.spark.HashPartitioner
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object GraphDataGenerator {

  val MAX_ID: Int = 2401080

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph-DataGeneration")
    val sc = new SparkContext(conf)

    var modelPath = ""
    var outputPath = ""
    var totalNumRecords: Int = 0
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val numPartitions = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((parallel / 2).toString).toInt

    if (args.length == 3) {
      modelPath = args(0)
      outputPath = args(1)
      totalNumRecords = args(2).toInt

      println(s"Model Path: $modelPath")
      println(s"Output Path: $outputPath")
      println(s"Total Records: $totalNumRecords")
    } else {
      System.err.println(
        s"Usage: $GraphDataGenerator <MODEL_PATH> <OUTPUT_PATH> <NUM_RECORDS>"
      )
      System.exit(1)
    }

    val g = GraphGenerators.logNormalGraph(sc, totalNumRecords)

    val edges = g.edges.map(edge => edge.dstId + " " + edge.srcId)
    edges.saveAsTextFile(outputPath)

    sc.stop()
  }

}
