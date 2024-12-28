package sda.traitement
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._


object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter()= {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }

    def calculTTC () : DataFrame ={
      /*..........................coder ici...............................*/
      dataFrame.withColumn("TTC", round(col("HTT") + col("HTT") * col("TVA"), 2)).drop("TVA", "HTT")
    }
    def extractDateEndContratVille(): DataFrame = {
      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, false)
        .add("Date_End_contrat", StringType, false)
      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)
      /*..........................coder ici...............................*/
      dataFrame.withColumn("metaData_json", from_json(col("MetaData"), schema))
        .withColumn("Ville", col("metaData_json.MetaTransaction")(0)("Ville"))
        .withColumn("Date_End_contrat_raw", col("metaData_json.MetaTransaction")(0)("Date_End_contrat"))
        .withColumn("Date_End_contrat", regexp_extract(col("Date_End_contrat_raw"), "(\\d{4}-\\d{2}-\\d{2})", 1))
        .drop("MetaData", "metaData_json", "Date_End_contrat_raw")
    }

    def contratStatus(): DataFrame = {
      /*..........................coder ici...............................*/
      dataFrame.withColumn("Contrat_Status", when(to_date(col("Date_End_contrat"), "yyyy-MM-dd") < current_date(), "Expired").otherwise("Actif"))
    }


  }

}
