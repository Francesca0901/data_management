package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state = null
  private var averRatingPerUser: RDD[(Int, Double)] = null
  private var normalizedRatings: RDD[(Int, (Int, Double))] = null
  private var globalAverageRatings: RDD[(Int, Double)] = null
  private var globalAverage: Double = 0.0

//  private var partitioner: HashPartitioner = null
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {

    //the average rating by user over all movies
    averRatingPerUser = ratingsRDD.map{
      case(user_id, _, _, rating, _) => (user_id, (rating, 1))
    }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2)                                  // (userId, averageRating)

    val totalRatingsAndCount = ratingsRDD.map{
      case(user_id, _, _, rating, _) => (rating, 1)
    }.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    globalAverage = totalRatingsAndCount._1 / totalRatingsAndCount._2

    //pre-process ratings to instead express how much each rating deviates from the user’s average rating
    //normalize deviation
    val ratingWithAverageRating = ratingsRDD.map {
      case (userId, movieId, _, rating, timestamp) => (userId, (movieId, rating, timestamp))
    }.join(averRatingPerUser) // (userId, ((movieId, rating, timestamp), averageRating))

    normalizedRatings = ratingWithAverageRating.map {
      case (userId, ((movieId, rating, _), averageRating)) =>
        val normalizedRating = if (rating > averageRating) {
          (rating - averageRating) / (5 - averageRating)
        } else if (rating < averageRating) {
          (rating - averageRating) / (averageRating - 1)
        } else {
          (rating - averageRating) / 1
        }
        (userId, (movieId, normalizedRating))
    }

    globalAverageRatings = normalizedRatings.map {
      case (userId, (movieId, normalizedRating)) => (movieId, (normalizedRating, 1))
    }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2) // (movieId, averageRating)
  }

  def predict(userId: Int, movieId: Int): Double = {         // need optimization
    val userAverageRating = {
      val rating = averRatingPerUser.lookup(userId)
      if (rating.isEmpty) 0.0 else rating.head
    }
//    val globalAverageRatingOnMovie = globalAverageRatings.lookup(movieId).head
    val globalAverageRatingOnMovie = {
      val globalRatings = globalAverageRatings.lookup(movieId)
      if (globalRatings.isEmpty) -2.0 else globalRatings.head
    }

    if (globalAverageRatingOnMovie == -2.0) {
      return userAverageRating
    }

    if (userAverageRating == 0.0) {
      return globalAverage
    }

    val scale = {
      if (globalAverageRatingOnMovie > 0) {
        5 - userAverageRating
      } else if (globalAverageRatingOnMovie < 0) {
        userAverageRating - 1
      } else 1
    }

    userAverageRating + globalAverageRatingOnMovie * scale
  }
}
