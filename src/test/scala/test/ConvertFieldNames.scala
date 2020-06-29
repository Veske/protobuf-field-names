package test

import com.holdenkarau.spark.testing.DatasetSuiteBase
import fields.TestMessage
import org.apache.spark.sql.Dataset
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConvertFieldNames extends AnyFlatSpec with DatasetSuiteBase with Matchers with OptionValues {

  it should "be able to convert Scala structure to Proto" in {
    import spark.implicits._

    val testData = Seq(TestData(1L, "hey", "hello"))
    val result = spark
      .createDataset(testData)
      .transform(toProtobuf)
      .collect()
    result.head.longText shouldBe "hello"
  }

  private def toProtobuf[T](in: Dataset[T]) = {
    import scalapb.spark.Implicits._
    in.as[TestMessage]
  }

}
