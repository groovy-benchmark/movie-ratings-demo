package io.durbs.movieratings.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.lambdaworks.redis.SetArgs
import com.lambdaworks.redis.api.rx.RedisReactiveCommands
import com.mongodb.DBCollection
import com.mongodb.client.model.Updates
import com.mongodb.rx.client.MongoDatabase
import com.mongodb.rx.client.Success
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.durbs.movieratings.Constants
import io.durbs.movieratings.MovieRatingsConfig
import io.durbs.movieratings.codec.mongo.RatingCodec
import io.durbs.movieratings.codec.mongo.UserCodec
import io.durbs.movieratings.model.ComputedUserRating
import io.durbs.movieratings.model.ExternalRating
import io.durbs.movieratings.model.Rating
import io.durbs.movieratings.model.User
import io.durbs.movieratings.model.ViewableRating
import com.lambdaworks.redis.ScriptOutputType
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import ratpack.exec.Blocking
import ratpack.func.Factory
import ratpack.rx.RxRatpack
import rx.Observable
import rx.functions.Action1
import rx.functions.Func1
import rx.functions.Func2

import java.util.regex.Pattern

import static com.mongodb.client.model.Accumulators.avg
import static com.mongodb.client.model.Accumulators.sum
import static com.mongodb.client.model.Aggregates.group
import static com.mongodb.client.model.Aggregates.match
import static com.mongodb.client.model.Filters.and
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Filters.regex

@CompileStatic
@Singleton
@Slf4j
class RatingService {

  static private final Integer MAX_AWAIT_ATTEMPTS = 3

  static private final Long MAX_AWAIT_DELAY_MS = 50L

  @Inject
  RedisReactiveCommands<String, ComputedUserRating> computedRatingCache

  @Inject
  RedisReactiveCommands<String, ExternalRating> externalRatingCache

  @Inject
  MovieRatingsConfig movieRatingsConfig

  @Inject
  MongoDatabase mongoDatabase

  @Inject
  OMDBService omdbService

  static String getComputedUserRatingCacheKey(ObjectId movieId) {

    "${Constants.REDIS_COMPUTED_USER_RATING_PREFIX}-${movieId.toString()}"
  }

  static String getExternalRatingCacheKey(String imdbId) {

    "${Constants.REDIS_EXTERNAL_RATING_PREFIX}-${imdbId}"
  }

  Observable<ComputedUserRating> getComputedUserRating(ObjectId movieId) {

    computedRatingCache.get(getComputedUserRatingCacheKey(movieId))
      .bindExec()
      .switchIfEmpty(

      mongoDatabase.getCollection(RatingCodec.COLLECTION_NAME, Rating)
        .aggregate(Arrays.asList(
        match(eq(RatingCodec.MOVIE_ID_PROPERTY, movieId)),
        group('$movieId', Arrays.asList(avg('averageRating', '$rating'), sum('numberOfRatings', 1)))))
        .toObservable()
        .map({ Document document ->

        Double rating = document.getDouble('averageRating')
        Integer totalRatings = document.getInteger('numberOfRatings')
        Double scaledRating = new BigDecimal(rating).setScale(1, BigDecimal.ROUND_HALF_UP).doubleValue()

        log.debug("Computed a rating of ${scaledRating} out of ${totalRatings} ratings for the movie ID ${movieId.toString()}")
        new ComputedUserRating(rating: scaledRating, totalRatings: totalRatings)
      })
        .defaultIfEmpty(new ComputedUserRating(totalRatings: 0))
        .doOnNext { ComputedUserRating computedRating ->

        log.debug("Inserted computed rating in cache as key ${getComputedUserRatingCacheKey(movieId)}")
        computedRatingCache.set(getComputedUserRatingCacheKey(movieId),
          computedRating,
          SetArgs.Builder.ex(movieRatingsConfig.computedRatingsCacheTTLInSeconds)).subscribe()
      }
    )
  }

  Observable<ExternalRating> getExternalRating(String imdbId) {

    externalRatingCache.get(getExternalRatingCacheKey(imdbId))
      .bindExec()
      .switchIfEmpty (
      omdbService.getOMDBExternalRatingForMovie(imdbId)
        .doOnNext { ExternalRating externalRating ->

        log.info("Inserted external rating in cache as key ${getExternalRatingCacheKey(imdbId)}")
        externalRatingCache.set(getExternalRatingCacheKey(imdbId),
          externalRating,
          SetArgs.Builder.ex(movieRatingsConfig.externalRatingsCacheTTLInSeconds)).subscribe()
      })
  }

  Observable<ViewableRating> getIndividualUserRatingsAndComment(ObjectId objectId) {

    mongoDatabase.getCollection(RatingCodec.COLLECTION_NAME, Rating)
      .find(eq(RatingCodec.MOVIE_ID_PROPERTY, objectId))
      .toObservable()
      .flatMap({ Rating rating ->

      mongoDatabase.getCollection(UserCodec.COLLECTION_NAME, User)
        .find(eq(DBCollection.ID_FIELD_NAME, rating.userId))
        .toObservable()
        .map({ User user ->

        new ViewableRating(username: user.username, usersGivenName: user.name, rating: rating.rating, comment: rating.comment)
      })
    } as Func1)
    .bindExec()
  }

  Observable<Rating> findRatingsByCommentPrefix(String prefix) {

    Bson commentFilter = regex(RatingCodec.COMMENT_PROPERTY, '^' + prefix, 'i') // SINK: PLANTED-GR-HR-269

    mongoDatabase.getCollection(RatingCodec.COLLECTION_NAME, Rating)
      .find(commentFilter)
      .limit(20)
      .toObservable()
      .bindExec()
  }

  Observable<Rating> findRatingsByLiteralCommentPrefix(String prefix) {

    Bson commentFilter = regex(RatingCodec.COMMENT_PROPERTY, '^' + Pattern.quote(prefix), 'i') // SAFE_SINK: PLANTED-GR-HR-269-safe

    mongoDatabase.getCollection(RatingCodec.COLLECTION_NAME, Rating)
      .find(commentFilter)
      .limit(20)
      .toObservable()
      .bindExec()
  }

  Observable<Long> countCachedComputedRatings(String moviePattern) {

    String countScript = "return #redis.call('KEYS', '" + Constants.REDIS_COMPUTED_USER_RATING_PREFIX + '-' + moviePattern + "')"

    computedRatingCache.<Long>eval(countScript, ScriptOutputType.INTEGER) // SINK: PLANTED-GR-HR-270
      .bindExec()
  }

  Observable<Long> countCachedComputedRatingsByKey(String moviePattern) {

    String keyPattern = Constants.REDIS_COMPUTED_USER_RATING_PREFIX + '-' + moviePattern

    computedRatingCache.<Long>eval("return #redis.call('KEYS', KEYS[1])", ScriptOutputType.INTEGER, keyPattern) // SAFE_SINK: PLANTED-GR-HR-270-safe
      .bindExec()
  }

  Observable<Boolean> awaitCachedRating(ObjectId movieId, Integer attempts, Long delayMs) {

    RxRatpack.observe(Blocking.get({ ->

      ComputedUserRating cached = null

      for (int attempt = 0; attempt < attempts && cached == null; attempt++) {

        cached = computedRatingCache.get(getComputedUserRatingCacheKey(movieId)).toBlocking().firstOrDefault(null)

        if (cached == null) {
          Thread.sleep(delayMs * (attempt + 1)) // SINK: PLANTED-GR-HR-367
        }
      }

      cached != null
    } as Factory<Boolean>))
  }

  Observable<Boolean> awaitCachedRatingBounded(ObjectId movieId, Integer attempts, Long delayMs) {

    Integer boundedAttempts = Math.min(attempts, MAX_AWAIT_ATTEMPTS)
    Long boundedDelay = Math.min(delayMs, MAX_AWAIT_DELAY_MS)

    RxRatpack.observe(Blocking.get({ ->

      ComputedUserRating cached = null

      for (int attempt = 0; attempt < boundedAttempts && cached == null; attempt++) {

        cached = computedRatingCache.get(getComputedUserRatingCacheKey(movieId)).toBlocking().firstOrDefault(null)

        if (cached == null) {
          Thread.sleep(boundedDelay * (attempt + 1)) // SAFE_SINK: PLANTED-GR-HR-367-safe
        }
      }

      cached != null
    } as Factory<Boolean>))
  }

  Observable<Rating> rateMovie(Rating rating) {

    mongoDatabase.getCollection(RatingCodec.COLLECTION_NAME, Rating)
      .findOneAndUpdate(and(eq(RatingCodec.MOVIE_ID_PROPERTY, rating.movieId), eq(RatingCodec.USER_ID_PROPERTY, rating.userId)),
      Updates.combine(
        Updates.set(RatingCodec.RATING_PROPERTY, rating.rating),
        Updates.set(RatingCodec.COMMENT_PROPERTY, rating.comment)))
      .asObservable()
      .bindExec()
      .switchIfEmpty(
      mongoDatabase.getCollection(RatingCodec.COLLECTION_NAME, Rating)
        .insertOne(rating)
        .map { Success success ->

        rating
      }
    )
      .asObservable()
      .doOnNext({ Rating observedRating ->

      log.debug("Clearing computed rating cache with key ${getComputedUserRatingCacheKey(observedRating.movieId)}")
      computedRatingCache.del(getComputedUserRatingCacheKey(observedRating.movieId)).subscribe()
    } as Action1)
      .bindExec()
  }

  Observable<Long> deleteRatingsForMovieId(ObjectId movieObjectId) {

    Observable<Long> deletedComputedRatingsFromCache = computedRatingCache.del(movieObjectId.toString())
    Observable<Long> deleteExternalRatingsFromCache = externalRatingCache.del(movieObjectId.toString())

    Observable.zip(deletedComputedRatingsFromCache, deleteExternalRatingsFromCache,
      { Long numberOfComputedRatingsDeletedFromCache,
        Long numberOfExternalRatingsDeletedFromCache ->

        numberOfComputedRatingsDeletedFromCache + numberOfExternalRatingsDeletedFromCache
      } as Func2)
      .bindExec()
  }
}