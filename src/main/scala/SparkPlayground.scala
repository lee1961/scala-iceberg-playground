import org.apache.spark.sql.SparkSession

object SparkPlayground {
  def main(args: Array[String]): Unit = {
    print("my first scala program")

    var spark = SparkSession
      .builder()
      .appName("Spark hello world")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()


    println("Spark Session created")
    val sampleSeq = Seq((1, "Spark"), (2, "Bug Data"))

    val df =  spark.createDataFrame(sampleSeq).toDF("course id", "course name")

    df.write.csv("/tmp/spark_output5/data")

    df.show()



  }
}
