import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xm on 15/03/2017.
  */
object testCogroup {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("cogroup-example")
    val sc = new SparkContext(conf)

    val data1 = sc.parallelize(List((1, "www"), (2, "bbs")))

    val data2 = sc.parallelize(List((1, "iteblog"), (2, "very")))

    val result = data1.cogroup(data2).collect()

    for (i <- result.length) {
      print("ruic-log: " + result(i))
    }

    sc.stop()

  }

}
