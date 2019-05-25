import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object DeloiCheck extends App {
  println("Inside DeloiCheck")
  
    case class Record(Country:String, value1: Int, value2: Int, value3: Int, value4:Int, value5: Int)
    case class CombinedRecord(Country:String, value: String)
  //
  def mapper(line:String): Record = {
    val fields = line.split(',')  
    
    val values = fields(1).split(';').map(_.toInt)
        
    val rec:Record = Record(fields(0), values(0), values(1), values(2), values(3), values(4))
    return rec
  }
 
      Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") 
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val file = spark.sparkContext.textFile("C:/Users/Prakash/Downloads/data.csv")
    
    // Get the dataframe with the give case class schema
    val file_header = file.first()    
    val file_dataframe =file.filter(row => row != file_header).map(mapper).toDF().cache()
    
    // Get the sum of value columns
    val newNames = Seq("Country", "val1_total","val2_total","val3_total","val4_total","val5_total")
    val file_group_by_country = file_dataframe.groupBy("Country").sum().toDF(newNames: _*)
    //file_group_by_country.show()    
    
    // combine individual value columns
    val file_group_by_country_combined = file_group_by_country.withColumn("values", concat($"val1_total",lit(";"),$"val2_total",lit(";"),$"val3_total",lit(";"),$"val4_total",lit(";"),$"val5_total"))
   // file_group_by_country_combined.show()
    
    //Select only country and combined column    
    val result = file_group_by_country_combined.select("Country", "values")    
    result.show()
    
    // Write it into a single Parquet file
    result.coalesce(1).write.format("parquet").save("C:/Users/Prakash/Downloads/data.parquet")
    println("ending check")
  
}