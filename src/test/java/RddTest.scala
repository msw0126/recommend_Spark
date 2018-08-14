object RddTest {
  def main(args: Array[String]): Unit = {
    /* 使用makeRDD创建RDD */
    /* List */
    import org.apache.spark.SparkContext
    System.setProperty("hadoop.home.dir", "F:\\tools\\Spark\\hadoop-2.7.5")
    val sc = new SparkContext("local", "My App")
//    val rdd01 = sc.makeRDD(List((1,2), (3,4)))
//    val r01 = rdd01.map { x => x._1 * x._1 }
//    println(r01.collect().mkString(","))
    val rdd01 = sc.makeRDD(List((37857, "interesting animation style"), (37857,"dave mckean"), (4865, "Comics")))
     rdd01.reduceByKey(_+_).groupBy(k=>k._1).collect().foreach{println}  // .groupBy(k=>k._1._1)
  }

}
