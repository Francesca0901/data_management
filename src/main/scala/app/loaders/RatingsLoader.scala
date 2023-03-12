package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    sc.textFile(getClass.getResource(path).getPath)
      .map(_.split("\\|"))
      .map(arr => (arr(0).toInt, arr(1).toInt,
        if (arr.length == 4) None else Some(arr(2).toDouble),
        arr(arr.length - 2).toDouble,
        arr(arr.length - 1).toInt))
  }
}