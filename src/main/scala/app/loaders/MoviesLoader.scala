package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val reg = """^(\d+)\|"(.+)"\|"(.+)"$""".r
    sc.textFile(getClass.getResource(path).getPath)
      .map { line =>
        val matchh = reg.findFirstMatchIn(line).get
        val id = matchh.group(1).toInt
        val title = matchh.group(2)
        val genres = matchh.group(3).split("\\|").toList
        (id, title, genres)
      }
  }
}

