package pack

import org.apache. spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql. functions._

object Hello {
    def main(args: Array[String]) = {
        System.setProperty("hadoop.home.dir","D:\\hadoop")
        
        val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._
        
        val df = spark.read.format("json").option("multiline",true).load("file:///C:/data_import/Sparkdata/complexjson/actorsj.json")
    
        df.show()
        df.printSchema()
        
        val arrayflatten = df.withColumn("Actors",expr("explode(Actors)"))
    
        arrayflatten.show()
        arrayflatten.printSchema()
        
        val final_flatten = arrayflatten.select("Actors.Birthdate",
                                        "Actors.name",
                                        "Actors.age",
                                        "Actors.hasChildren",
                                        "Actors.hasGreyHair",
                                        "Actors.BornAt",
                                        "Actors.photo",
                                        "Actors.picture.*",
                                        "country",
                                        "version")
                                        
        final_flatten.printSchema()
        final_flatten.show()
        
        
        
    }
}