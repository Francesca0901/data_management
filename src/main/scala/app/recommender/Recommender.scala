package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  private val bcCollaborativePredictor = sc.broadcast(collaborativePredictor)
  private val bcBaselinePredictor = sc.broadcast(baselinePredictor)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    // already watched movie list
    val watchedMovie = ratings.map{ case (user_id, movie_id, _, _, _) => (user_id, movie_id) }.lookup(userId)

    val genreRDD = sc.parallelize(genre)
    val lookedUpMovie = nn_lookup.lookup(genreRDD.map(_.split(" ").toList)).flatMap {
      case (genres, movieRDD) => movieRDD
    }.map{
      case (movieId, movieName, genres) => movieId
    }

    // exclude seen movies
    val filteredMovie = lookedUpMovie.filter(movieId => !watchedMovie.contains(movieId))

    val predictMovieRating = filteredMovie.map{ case (movieId) =>
      val rating = bcBaselinePredictor.value.predict(userId, movieId)
      (movieId, rating)
    }.sortBy(-_._2).take(K).toList

    predictMovieRating
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    ???
  }
}
