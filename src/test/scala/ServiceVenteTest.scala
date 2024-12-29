import org.scalatest.funsuite.AnyFunSuite
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.holdenkarau.spark.testing.SparkSessionProvider.sparkSession

class ServiceVenteTest extends AnyFunSuite with DataFrameSuiteBase {

  import org.apache.spark.sql.functions._
  import ServiceVente.DataFrameUtils

  test("formatter") {
    val spark = sparkSession
    import spark.implicits._

    val inputData = Seq(
      ("1000|0.2"),
      ("2000|0.1")
    ).toDF("HTT_TVA")

    val expectedData = Seq(
      ("1000|0.2", "1000", "0.2"),
      ("2000|0.1", "2000", "0.1")
    ).toDF("HTT_TVA", "HTT", "TVA")

    val result = inputData.formatter()

    assertDataFrameEquals(expectedData, result)
  }

  test("calculTTC") {
    val spark = sparkSession
    import spark.implicits._

    val inputData = Seq(
      ("1000", "0.2"),
      ("2000", "0.1")
    ).toDF("HTT", "TVA")

    val expectedData = Seq(
      (1200.0),
      (2200.0)
    ).toDF("TTC")

    val formattedData = inputData
      .withColumn("HTT", regexp_replace(col("HTT"), ",", "."))
      .withColumn("TVA", regexp_replace(col("TVA"), ",", "."))

    val result = formattedData.calculTTC()

    assertDataFrameEquals(expectedData, result)
  }

  test("extractDateEndContratVille") {
    val spark = sparkSession
    import spark.implicits._

    val inputData = Seq(
      ("{\"MetaTransaction\": [{\"Ville\": \"Paris\", \"Date_End_contrat\": \"2024-12-31\"}]}"),
      ("{\"MetaTransaction\": [{\"Ville\": \"Lyon\", \"Date_End_contrat\": \"2023-12-31\"}]}")
    ).toDF("MetaData")

    val expectedData = Seq(
      ("Paris", "2024-12-31"),
      ("Lyon", "2023-12-31")
    ).toDF("Ville", "Date_End_contrat")

    val result = inputData.extractDateEndContratVille()

    assertDataFrameEquals(expectedData, result)
  }

  test("contratStatus") {
    val spark = sparkSession
    import spark.implicits._

    val inputData = Seq(
      ("2024-12-31"),
      ("2022-12-31")
    ).toDF("Date_End_contrat")

    val expectedData = Seq(
      ("2024-12-31", "Actif"),
      ("2022-12-31", "Expired")
    ).toDF("Date_End_contrat", "Contrat_Status")

    val result = inputData.contratStatus()

    assertDataFrameEquals(expectedData, result)
  }
}