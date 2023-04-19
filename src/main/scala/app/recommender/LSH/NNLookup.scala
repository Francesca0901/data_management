package app.recommender.LSH

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param queries The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    val queryWithSignature = lshIndex.hash(queries)
    val foundMovies = lshIndex.lookup(queryWithSignature).map{ case (signature, keywordList, matchedMovies) => (keywordList, matchedMovies)}
    foundMovies
  }
}
