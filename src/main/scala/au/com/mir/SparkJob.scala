package au.com.mir

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object SparkJob {

  def findGenderDiversity(df: DataFrame, gender: String): Double = {
    val totalPopulation = df.count()

    val totalSelectedGender = df
      .filter(col("Gender") === gender)
      .count

    math.rint(totalSelectedGender.toDouble / totalPopulation.toDouble * 100)
  }

  def findBirthPlace(df: DataFrame, location: String): Long = {
    df
      .filter(col("Birth Place").contains(location))
      .count

  }
}
