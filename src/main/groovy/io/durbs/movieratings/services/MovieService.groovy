package io.durbs.movieratings.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.mongodb.DBCollection
import com.mongodb.client.model.Projections
import com.mongodb.client.model.Updates
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import com.mongodb.rx.client.MongoDatabase
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.durbs.movieratings.codec.mongo.MovieCodec
import io.durbs.movieratings.codec.mongo.RatingCodec
import io.durbs.movieratings.model.ComputedUserRating
import io.durbs.movieratings.model.ExternalRating
import io.durbs.movieratings.model.Movie
import io.durbs.movieratings.model.Rating
import com.mongodb.client.model.Filters
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import rx.Observable
import rx.functions.Func1
import rx.functions.Func2
import rx.functions.Func3

import java.util.regex.Pattern

import static com.mongodb.client.model.Filters.eq

@CompileStatic
@Singleton
@Slf4j
class MovieService {

  @Inject
  MongoDatabase mongoDatabase

  @Inject
  RatingService ratingService

  static private final List<String> FILTERABLE_MOVIE_FIELDS = Arrays.asList(MovieCodec.NAME_PROPERTY, MovieCodec.IMDB_ID_PROPERTY, MovieCodec.MPAA_RATING_PROPERTY)

  static private final Integer MAX_EXPORT_ROWS = 100

  static private final Integer MAX_PREVIEW_TITLES = 100

  static private final Map<String, Integer> PLACEHOLDER_PRESETS = ['S': 64 * 1024, 'M': 1024 * 1024]

  static private Func1 MOVIE_TO_RATED_MOVIE = { Movie movie ->

    Observable.zip(ratingService.getComputedUserRating(movie.id),
      ratingService.getExternalRating(movie.imdbId),
      { ComputedUserRating computedUserRating,
        ExternalRating externalRating ->

        movie.computedRating = computedUserRating
        movie.externalRating = externalRating

        movie
      } as Func2)
  } as Func1

  Observable<Boolean> movieExists(ObjectId objectId) {

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME)
      .find(eq(DBCollection.ID_FIELD_NAME, objectId))
      .projection(Projections.include(DBCollection.ID_FIELD_NAME))
      .toObservable()
      .isEmpty()
      .map({ Boolean movieDoesNotExist -> !movieDoesNotExist })
      .bindExec()
  }

  Observable<Movie> getMovie(ObjectId objectId) {

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME, Movie)
      .find(eq(DBCollection.ID_FIELD_NAME, objectId))
      .toObservable()
      .bindExec()
      .flatMap(MOVIE_TO_RATED_MOVIE)
  }

  Observable<String> getExistingMovieIMDBIDs() {

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME)
      .find()
      .projection(Projections.include(MovieCodec.IMDB_ID_PROPERTY))
      .toObservable()
      .bindExec()
      .map({ Document document ->

        document.getString(MovieCodec.IMDB_ID_PROPERTY)
      })
  }

  Observable<UpdateResult> updateMovie(Movie movie) {

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME, Movie)
      .updateOne(eq(DBCollection.ID_FIELD_NAME, movie.id),
      Updates.combine(
        Updates.set(MovieCodec.NAME_PROPERTY, movie.name),
        Updates.set(MovieCodec.IMDB_ID_PROPERTY, movie.imdbId),
        Updates.set(MovieCodec.DESCRIPTION_PROPERTY, movie.name),
        Updates.set(MovieCodec.POSTER_IMAGE_URI_PROPERTY, movie.posterImageURI),
        Updates.set(MovieCodec.YEAR_RELEASED_PROPERTY, movie.yearReleased)))
      .bindExec()
  }

  Observable<String> createMovie(Movie movie) {

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME, Movie)
      .insertOne(movie)
      .map {

      movie.id.toString()
    }
    .bindExec()
  }

  Observable<Boolean> deleteMovieByID(ObjectId movieObjectId) {

    Observable<Long> deleteRatingsFromCache = ratingService.deleteRatingsForMovieId(movieObjectId)
    Observable<DeleteResult> deleteMovie = mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME, Movie)
      .deleteOne(eq(DBCollection.ID_FIELD_NAME, movieObjectId))
    Observable<DeleteResult> deleteMovieRatings = mongoDatabase.getCollection(RatingCodec.COLLECTION_NAME, Rating)
      .deleteMany(eq(RatingCodec.MOVIE_ID_PROPERTY, movieObjectId))

    Observable.zip(deleteRatingsFromCache, deleteMovie, deleteMovieRatings,
      { Long deletedRedisKeys,
        DeleteResult deletedMovieResult,
        DeleteResult deletedMovieRatingsResult ->

        log.info("Removing movie id ${movieObjectId.toString()}, deleting ${deletedRedisKeys} redis keys, ${deletedMovieResult.deletedCount} movies, and ${deletedMovieRatingsResult.deletedCount} ratings...")

        deletedMovieResult.deletedCount > 0

      } as Func3)
      .bindExec()
  }

  Observable<Movie> findMoviesByFilter(String filterJson, Integer limit) {

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME, Movie)
      .find(Document.parse(filterJson)) // SINK: PLANTED-GR-HR-267
      .limit(limit)
      .toObservable()
      .bindExec()
  }

  Observable<Movie> findMoviesByPublicFilter(String filterJson, Integer limit) {

    Document requestedFilter = Document.parse(filterJson)
    Document publicFilter = new Document()

    for (String field : FILTERABLE_MOVIE_FIELDS) {

      Object requestedValue = requestedFilter.get(field)
      if (requestedValue instanceof CharSequence) {
        publicFilter.put(field, requestedValue.toString())
      }
    }

    // a filter that names nothing filterable selects nothing instead of everything
    if (!requestedFilter.isEmpty() && publicFilter.isEmpty()) {
      return Observable.<Movie> empty()
    }

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME, Movie)
      .find(publicFilter) // SAFE_SINK: PLANTED-GR-HR-267-safe
      .limit(limit)
      .toObservable()
      .bindExec()
  }

  Observable<Movie> searchMoviesByTitleScript(String term, Integer limit) {

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME, Movie)
      .find(new Document('$where', "this.name.indexOf('" + term + "') >= 0")) // SINK: PLANTED-GR-HR-268
      .limit(limit)
      .toObservable()
      .bindExec()
  }

  Observable<Movie> searchMoviesByTitleLiteral(String term, Integer limit) {

    Bson titleFilter = Filters.regex(MovieCodec.NAME_PROPERTY, Pattern.quote(term), 'i')

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME, Movie)
      .find(titleFilter) // SAFE_SINK: PLANTED-GR-HR-268-safe
      .limit(limit)
      .toObservable()
      .bindExec()
  }

  Observable<Movie> exportMovies(Integer pageSize) {

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME, Movie)
      .find()
      .limit(pageSize) // SINK: PLANTED-GR-HR-366
      .toObservable()
      .bindExec()
  }

  Observable<Movie> exportMoviesPage(Integer pageSize) {

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME, Movie)
      .find()
      .limit((pageSize == null || pageSize <= 0 || pageSize > MAX_EXPORT_ROWS) ? MAX_EXPORT_ROWS : pageSize) // SAFE_SINK: PLANTED-GR-HR-366-safe
      .toObservable()
      .bindExec()
  }

  Integer renderPlaceholderPoster(String preset, Integer bytes) {

    Integer presetSize = preset == null ? null : PLACEHOLDER_PRESETS.get(preset)

    if (presetSize != null) {
      return fillCanvas(newPresetCanvas(presetSize))
    }

    fillCanvas(newCanvas(bytes))
  }

  static private byte[] newCanvas(Integer size) {

    byte[] canvas = new byte[size] // SINK: PLANTED-GR-HR-368
    canvas
  }

  static private byte[] newPresetCanvas(Integer presetSize) {

    byte[] canvas = new byte[presetSize] // SAFE_SINK: PLANTED-GR-HR-368-safe
    canvas
  }

  static private Integer fillCanvas(byte[] canvas) {

    Arrays.fill(canvas, (byte) 0x7f)
    canvas.length
  }

  List<String> generateSampleTitles(Integer count, String mode) {

    List<String> titles = new ArrayList<String>()

    if ('preview'.equals(mode)) {

      Integer previewCount = Math.min(count, MAX_PREVIEW_TITLES)

      for (int i = 0; i < previewCount; i++) { // SAFE_SINK: PLANTED-GR-HR-370-safe
        titles.add("Sample Movie " + i)
      }
    } else {

      for (int i = 0; i < count; i++) { // SINK: PLANTED-GR-HR-370
        titles.add("Sample Movie " + i)
      }
    }

    titles
  }

  Observable<Long> getMovieCount(Bson queryFilter) {

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME)
      .count(queryFilter)
      .bindExec()
  }

  Observable<Movie> getAllMovies(Bson queryFilter, Integer pageNumber, Integer skipCount) {

    mongoDatabase.getCollection(MovieCodec.COLLECTION_NAME, Movie)
      .find()
      .filter(queryFilter)
      .limit(pageNumber)
      .skip(skipCount)
      .toObservable()
      .bindExec()
      .flatMap(MOVIE_TO_RATED_MOVIE)
  }
}
