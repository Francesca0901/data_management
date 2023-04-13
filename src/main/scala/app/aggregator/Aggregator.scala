package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition.numPartitions
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state = null
  private var partitioner: HashPartitioner = null
  private var ratings: RDD[(Int, Int, Option[Double], Double, Int)] = null
  private var title: RDD[(Int, String, List[String])] = null
//  private var averageRatingWithGenres: RDD[(String, Double, List[String])] = null
  var averageRating: RDD[(String, Double)] = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    partitioner = new HashPartitioner(numPartitions)
    this.ratings = ratings
    this.title = title

//    val titleWithId: RDD[(String, List[String])] = title.map { case (movieId, title, genres) => (title, genres) }
    averageRating = getResult().persist(MEMORY_AND_DISK)

  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titleids and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    val ratingsByTitleId: RDD[(Int, Iterable[(Int, Int, Double)])] = ratings.map { case (user_id, title_id, old_rating, rating, timestamp) => (title_id, (user_id, title_id, rating)) }
      .groupByKey(partitioner) // (title_id, (user_id, title_id, rating)) }
    val averageRatingByTitleId: RDD[(Int, Double)] = ratingsByTitleId.mapValues { ratings =>
      val sum = ratings.map(_._3).sum
      val count = ratings.size
      sum / count
    }
    val averageRatingWithTitle: RDD[(String, Double)] = title.map { case (movieId, title, genres) => (movieId, title) }
      .leftOuterJoin(averageRatingByTitleId).map {
      case (movieId, (title, Some(rating))) => (title, rating)
      case (movieId, (title, None)) => (title, 0.0)
    }
    averageRatingWithTitle
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    val titleWithId: RDD[(String, List[String])] = title.map { case (movieId, title, genres) => (title, genres) }
    val averageRatingWithGenres = averageRating.join(titleWithId).map { case (title, (rating, genres)) => (title, rating, genres) }

    val filteredRatings = averageRatingWithGenres.filter { case (_, _, genres) =>
      keywords.forall(keyword => genres.contains(keyword))
    }.map { case (titles, ratings, genres) => ratings }

    val keywordQueryResult = {
      if (filteredRatings.count() == 0) -1.0
      else filteredRatings.reduce(_ + _) / filteredRatings.count()
    }
    keywordQueryResult
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val deltaRating = sc.parallelize(delta_)

    val deltaWithUserTitleRating = delta_.map { case (user_id, title_id, _, _, _) => (user_id, title_id) }.toSet
    // delete the old records
    val filteredRatings = ratings.filter { case (user_id, title_id, _, _, _) => !deltaWithUserTitleRating.contains((user_id, title_id)) }

    ratings = filteredRatings.union(deltaRating)
    val newAverageRating = getResult()
    averageRating.unpersist()
    averageRating = newAverageRating.persist(MEMORY_AND_DISK)
  }
}
