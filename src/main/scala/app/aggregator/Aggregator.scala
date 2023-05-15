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

  private var state: RDD[(Int, (String, Int, Double, List[String]))] = null
  private var partitioner: HashPartitioner = null
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
    partitioner = new HashPartitioner(ratings.getNumPartitions)

    val ratingsByTitleId: RDD[(Int, Iterable[(Int, Int, Double)])] = ratings.map { case (user_id, title_id, old_rating, rating, timestamp) => (title_id, (user_id, title_id, rating)) }
      .groupByKey(partitioner) // (title_id, (user_id, title_id, rating)) }

    val averageRatingByTitleId: RDD[(Int, (Double, Int))] = ratingsByTitleId.mapValues { ratings =>
      val sum = ratings.map(_._3).sum
      val count = ratings.size
      (sum / count, count)
    }

    state = title.map { case (movieId, title, genres) => (movieId, (title, genres)) }
      .leftOuterJoin(averageRatingByTitleId).map {
      case (movieId, (title, Some((avgRating, count)))) => (movieId, (title._1, count, avgRating, title._2))
      case (movieId, (title, None)) => (movieId, (title._1, 0, 0.0, title._2))
    }.persist(MEMORY_AND_DISK)

//    val titleWithId: RDD[(String, List[String])] = title.map { case (movieId, title, genres) => (title, genres) }
//    averageRating = getResult().persist(MEMORY_AND_DISK)

  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titleids and ratings
   */
  def getResult(): RDD[(String, Double)] = state.map { case (_, (title, _, ratingAvg, _)) => (title, ratingAvg) }

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
//    val titleWithId: RDD[(String, List[String])] = title.map { case (movieId, title, genres) => (title, genres) }
//    val averageRatingWithGenres = averageRating.join(titleWithId).map { case (title, (rating, genres)) => (title, rating, genres) }
//
//    val filteredRatings = averageRatingWithGenres.filter { case (_, _, genres) =>
//      keywords.forall(keyword => genres.contains(keyword))
//    }.map { case (titles, ratings, genres) => ratings }
//
//    val keywordQueryResult = {
//      if (filteredRatings.count() == 0) -1.0
//      else filteredRatings.reduce(_ + _) / filteredRatings.count()
//    }
//    keywordQueryResult

    val filteredRatings = state.filter { case (_, (title, _, _, genres)) =>
      keywords.forall(keyword => genres.contains(keyword))
    }.map { case (_, (_, _, ratings, _)) => ratings }

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
    val deltaRDD = sc.parallelize(delta_)

    // !!calculate the difference between the new and old ratings, and calculate the count accordingly
    val deltaRating: RDD[(Int, (Double, Int))] = deltaRDD.map {
      case (userId, titleId, Some(oldRating), newRating, _) => (titleId, (newRating - oldRating, 0)) // get the difference, count doesn't change
      case (userId, titleId, None, newRating, _) => (titleId, (newRating, 1)) // new rating, count increases
    }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val updatedState = state.leftOuterJoin(deltaRating)
      .map {
        case (titleId, ((title, count, avgRating, genres), Some((deltaRating, deltaCount)))) =>
          val newCount = count + deltaCount
          val newAvgRating = if (newCount == 0) 0.0 else (avgRating * count + deltaRating) / newCount
          (titleId, (title, newCount, newAvgRating, genres))
        case (titleId, ((title, count, avgRating, genres), None)) =>
          (titleId, (title, count, avgRating, genres))
      }

    state.unpersist()
    state = updatedState.persist(MEMORY_AND_DISK)
  }
}
