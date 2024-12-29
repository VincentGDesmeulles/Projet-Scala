package sda.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class XmlReader(path: String,
                     rootTag: Option[String] = None,
                     rowTag: Option[String]= None,
                     multiline: Option[Boolean] = None
                    )
  extends Reader {
  val format = "xml"

  def read()(implicit spark: SparkSession): DataFrame = {

    // Lire le fichier XML avec les options données
    val df = spark.read.format(format)
      .option("rootTag", rootTag.getOrElse("Clients"))
      .option("rowTag", rowTag.getOrElse("Client"))
      .option("multiline", multiline.getOrElse(true).toString)
      .load(path)

    // Créer une colonne MetaData avec la structure MetaTransaction sous forme JSON
    val dfWithMetaData = df.withColumn("MetaData",
      to_json(
        struct(
          lit("MetaTransaction").alias("MetaTransaction"),
          array(
            struct(
              col("Ville").alias("Ville"),
              col("Date_End_contrat").alias("Date_End_contrat")
            )
          ).alias("MetaTransaction")
        )
      )
    )

    // Aplatir la structure HTT_TVA en une seule colonne HTT_TVA
    val dfFlattened = dfWithMetaData.withColumn("HTT_TVA", concat_ws("|", col("HTT_TVA.HTT"), col("HTT_TVA.TVA")))

    dfFlattened
  }
}