import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
object IcebergInsert {

  var spark = SparkSession
    .builder()
    .appName("Spark hello world")
    .config("spark.master", "local")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "$PWD/warehouse")
    .config("spark.sql.defaultCatalog", "local")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    print("my first scala program")




    println("Spark Session created")

    val df = spark.table("local.demo.nyc.taxis")

    val newData = Seq(
      Row(1: Long, 1000371: Long, 1.8f: Float, 15.32: Double, "new data": String),
    )

//    df.write(data).
//    val sampleSeq = Seq((1, "Spark"), (2, "Bug Data"))
//
//    val df = spark.createDataFrame(newData).toDF("course id", "course name")
//
//
//
//    df.writeTo("local.demo.nyc.taxis").




    //val df = spark.sql("Select * FROM local.demo.nyc.taxis.history")

    //df.


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
