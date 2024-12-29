package sda.traitement

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.Row

class ServiceVenteTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("ServiceVenteTest")
    .getOrCreate()

  import spark.implicits._
  import ServiceVente.DataFrameUtils

  test("formatter should split HTT_TVA column into HTT and TVA") {
    val inputData = Seq(
      ("100|0.2"),
      ("200|0.1")
    ).toDF("HTT_TVA")

    val expectedData = Seq(
      ("100|0.2", "100", "0.2"),
      ("200|0.1", "200", "0.1")
    ).toDF("HTT_TVA", "HTT", "TVA")

    val result = inputData.formatter()

    assert(result.collect() === expectedData.collect())
  }

  test("calculTTC should compute TTC based on HTT and TVA") {
    val inputData = Seq(
      ("100", "0.2"),
      ("200", "0.1")
    ).toDF("HTT", "TVA")
      .withColumn("HTT", col("HTT").cast("double"))
      .withColumn("TVA", col("TVA").cast("double"))

    val expectedData = Seq(
      (120.0),
      (220.0)
    ).toDF("TTC")

    val result = inputData.calculTTC()

    assert(result.collect() === expectedData.collect())
  }

  test("extractDateEndContratVille should extract Ville and Date_End_contrat") {
    val inputData = Seq(
      ("{""MetaTransaction"": [{""Ville"": ""Paris"", ""Date_End_contrat"": ""2024-12-31T00:00:00""}]}")
    ).toDF("MetaData")

    val expectedData = Seq(
      ("Paris", "2024-12-31")
    ).toDF("Ville", "Date_End_contrat")

    val result = inputData.extractDateEndContratVille()

    assert(result.select("Ville", "Date_End_contrat").collect() === expectedData.collect())
  }

  test("contratStatus should determine contract status") {
    val inputData = Seq(
      ("2023-12-31"),
      ("2025-01-01")
    ).toDF("Date_End_contrat")

    val expectedData = Seq(
      ("2023-12-31", "Expired"),
      ("2025-01-01", "Actif")
    ).toDF("Date_End_contrat", "Contrat_Status")

    val result = inputData.contratStatus()

    assert(result.collect() === expectedData.collect())
  }

}