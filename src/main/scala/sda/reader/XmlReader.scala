package sda.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
case class XmlReader(path: String,
                     rootTag: Option[String] = None,
                     rowTag: Option[String]= None,
                     multiline: Option[Boolean] = None

                    )
  extends Reader {
  val format = "xml"

  def read()(implicit  spark: SparkSession): DataFrame = {

    spark.read.format(format)
      .option("rootTag", "Clients")
      .option("rowTag", "Client")
      .option("multiline", "true")
      .load(path)
  }
}