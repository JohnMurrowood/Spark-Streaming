package streaming

import org.apache.spark._
import org.apache.spark.streaming._


object SparkStreaming {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }
    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(".")
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))

    // Task A
    val re = "[A-Za-z]+".r
    val letters = lines.map(_.replaceAll("[^A-Za-z ]", ""))
    val words = letters.flatMap(re.findAllIn(_))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    var c = 0
    wordCounts.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
    rdd.saveAsTextFile("hdfs:///TaskA00".concat(c.toString))
    c = c + 1
    }
    })

    // Task B
    var b_c = 0
    val re2 = "[A-Za-z]{5,}".r
    val wordFilter = lines.map(_.split(" ").map(_.replaceAll("[^A-Za-z ]", "")).flatMap(re2.findAllIn(_)).toList)
    val co_occurrence = wordFilter.flatMap(x =>
      for {
        i <- x
        j <- x
        if (i != j)
      } yield {
        ((i, j), 1)
      })

    val coCounts = co_occurrence.reduceByKey(_ + _)

    coCounts.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
    rdd.saveAsTextFile("hdfs:///TaskB00".concat(b_c.toString))
    b_c = b_c + 1
    }
    })


    // Task C
    var c_c = 0
    def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
      val newCount = runningCount.getOrElse(0) + newValues.sum
      Some(newCount)
    }

    val running = coCounts.updateStateByKey[Int](updateFunction _)

    running.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
    rdd.saveAsTextFile("hdfs:///TaskC00".concat(c_c.toString))
    c_c = c_c + 1
    }
    })

    running.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
