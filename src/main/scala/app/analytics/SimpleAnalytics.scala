package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime

import scala.math.Ordered.orderingToOrdered


class SimpleAnalytics() extends Serializable {
  private var usedRatings: RDD[(Int, Int, Option[Double], Double, Int)] = null
  private var usedMovies: RDD[(Int, String, List[String])] = null

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null

  private var titlesGroupedById: RDD[(Int, String)] = null
  private var ratingsGroupedByYearByTitle: RDD[((Int, Int), Int)] = null
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {
    ratingsPartitioner = new HashPartitioner(ratings.getNumPartitions)
    moviesPartitioner = new HashPartitioner(movie.getNumPartitions)

    usedRatings = ratings
    usedMovies = movie

    val moviesByYear = ratings.map { rating =>
      val dateTime = new DateTime(rating._5 * 1000L)
      val year = dateTime.getYear
      ((year, rating._2), 1)
    }

    ratingsGroupedByYearByTitle = moviesByYear                                    // ((year, movieId), count)
      .reduceByKey(ratingsPartitioner, _ + _)
      .persist()

    titlesGroupedById = movie.map { case (movieId, name, _) => (movieId, name) }  // (movieId, movieName)
      .partitionBy(ratingsPartitioner)
      .persist()
  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    val moviesEachYear = usedRatings.map { rating =>
      val dateTime = new DateTime(rating._5 * 1000L)
      val year = dateTime.getYear
      (year, rating._2)
    }
    val numMoviesByYear = moviesEachYear.distinct.groupBy(_._1).mapValues(_.size)
    numMoviesByYear
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    val mostRatedMovieEachYear = ratingsGroupedByYearByTitle.map { case ((year, movieId), count) => (year, (movieId, count)) }
      .groupByKey()
      .mapValues { movies =>
        movies.toList.sortWith { case ((movieId1, count1), (movieId2, count2)) =>
          if (count1 == count2) movieId1 > movieId2
          else count1 > count2
        }.head
      }
      .map { case (year, (movieId, _)) => (movieId, year) }

    val result = mostRatedMovieEachYear.join(titlesGroupedById).map { case (movieId, (year, movieName)) => (year, movieName) }
    result
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    val mostRatedMovieEachYear = ratingsGroupedByYearByTitle.map { case ((year, movieId), count) => (year, (movieId, count)) }
      .groupByKey()
      .mapValues { movies =>
        movies.toList.sortWith { case ((movieId1, count1), (movieId2, count2)) =>
          if (count1 == count2) movieId1 > movieId2
          else count1 > count2
        }.head
      }
      .map { case (year, (movieId, _)) => (movieId, year) }

    // Join the mostRatedMovieByYear RDD with usedMovies RDD to get the movie name and genres
    val result = mostRatedMovieEachYear.join(usedMovies.map { case (movieId, name, genres) => (movieId, genres) }).map {
      case (movieId, (year, genres)) => (year, genres)
    }
    result
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
    val mostRatedGenreEachYear = getMostRatedGenreEachYear.flatMap(x => x._2)
      .map(x => (x, 1)).reduceByKey(_ + _)
    val (maxMovie, maxCount):(String, Int) = mostRatedGenreEachYear.sortBy(x => (-x._2, x._1)).first()
    val (minMovie, minCount):(String, Int) = mostRatedGenreEachYear.sortBy(x => (x._2, x._1)).first()
    ((minMovie, minCount), (maxMovie, maxCount))
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset, (movieId, movieName, movieGenres)
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    val requiredGenresList = requiredGenres.collect.toList
    val requiredGenresSet = movies.filter{ case (_, _, genres) =>
      genres.intersect(requiredGenresList).nonEmpty
    }.map(_._2)
    requiredGenresSet
  }

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    // broadcast the requiredGenres list
    val requiredGenresBroadcast = broadcastCallback(requiredGenres)
    val requiredGenresSet = movies.filter { case (_, _, genres) =>
      genres.intersect(requiredGenresBroadcast.value).nonEmpty
    }.map(_._2)

    requiredGenresSet
  }

}

