import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.File


trait Helper {
  val CSV_INPUT = "data/all.csv"
  val PARQUET_DATA = "data/raw.parquet"

  val WHLOCATION = "spark-warehouse"
  val LOCALDIR = "tmpdir"
}

object TRAPSpark extends Helper {

  def getRawData(spark: SparkSession): DataFrame ={
    val parquetFile = new File(PARQUET_DATA)

    if(!parquetFile.isDirectory){
      val csv = spark.read
        .option("inferSchema", true)
        .option("header", false)
        .option("mode","FAILFAST")
        .option("delimiter",";")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(CSV_INPUT)
        .toDF("plate", "gate", "lane", "timestamp", "nationality")

      csv.write.parquet(PARQUET_DATA)
    }

    spark.read.parquet(PARQUET_DATA)
  }

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("TRAP2017")
      .config("spark.sql.warehouse.dir", WHLOCATION)
      .config("spark.local.dir", LOCALDIR)
      .master("local[*]")
      .getOrCreate()

    val df = getRawData(spark)

    df.show(false)

    df.describe().show()

    df.groupBy("nationality").count().show()
  }

}