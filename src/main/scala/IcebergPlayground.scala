import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import java.io.File
object IcebergPlayground {

  var spark = SparkSession
    .builder()
    .appName("Spark hello world")
    .config("spark.master", "local")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "$PWD/warehouse")
    .config("spark.sql.defaultCatalog", "local")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    print("my first scala program")
    val p = new File("D:\\scala-proj\\$PWD\\warehouse\\nyc")
    if (p.exists()) {
      println("already exist, hence deleting")
      new File("D:\\scala-proj\\$PWD\\warehouse\\nyc").delete()
    }

    println("Spark Session created")
//    val sampleSeq = Seq((1, "Spark"), (2, "Bug Data"))
//
//    val df = spark.createDataFrame(sampleSeq).toDF("course id", "course name")
//
//    df.writeTo("prod.db.table").append()

    val schema = StructType(Array(
      StructField("vendor_id", LongType, true),
      StructField("trip_id", LongType, true),
      StructField("trip_distance", FloatType, true),
      StructField("fare_amount", DoubleType, true),
      StructField("store_and_fwd_flag", StringType, true)
    ))
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    df.writeTo("nyc.taxis").partitionedBy(df.col("vendor_id")).createOrReplace()


    val scheme = spark.table("nyc.taxis").schema
    val data = Seq(
      Row(1: Long, 1000371: Long, 1.8f: Float, 15.32: Double, "N": String),
      Row(2: Long, 1000372: Long, 2.5f: Float, 22.15: Double, "N": String),
      Row(2: Long, 1000373: Long, 0.9f: Float, 9.01: Double, "N": String),
      Row(1: Long, 1000374: Long, 8.4f: Float, 42.13: Double, "Y": String)
    )
    val datafra = spark.createDataFrame(spark.sparkContext.parallelize(data), scheme)
    datafra.writeTo("nyc.taxis").append()

    val x = 5
    //df.writeTo("prod.db.table").append()
//    spark.sql("CREATE TABLE local.db.table (id bigint, data string) USING iceberg")
//
//
//    spark.table("source").select("id", "data")
//      .writeTo("local.db.table").append()
//
//    val df = spark.table("local.db.table")
//
//    df.count()


    //df.write.csv("/tmp/spark_output5/data")

    //df.show()


  }
}
