package io.durbs.movieratings.handling.chainaction

import com.google.inject.Inject
import com.google.inject.Singleton
import groovy.util.logging.Slf4j
import io.durbs.movieratings.Constants
import io.durbs.movieratings.handling.handler.pagination.GetAllMoviesHandler
import io.durbs.movieratings.handling.handler.JWTTokenHandler
import io.durbs.movieratings.handling.handler.MovieIDExtractionAndVerificationHandler
import io.durbs.movieratings.handling.handler.pagination.MovieSearchHandler
import io.durbs.movieratings.model.Movie
import io.durbs.movieratings.model.Rating
import io.durbs.movieratings.model.User
import io.durbs.movieratings.model.ViewableRating
import io.durbs.movieratings.services.MovieService
import io.durbs.movieratings.services.OMDBService
import io.durbs.movieratings.services.RatingService
import io.durbs.movieratings.services.ThrottleService
import org.bson.types.ObjectId
import ratpack.groovy.handling.GroovyChainAction
import ratpack.handling.Context
import ratpack.jackson.Jackson
import rx.Observable
import rx.functions.Func1

import javax.validation.ConstraintViolation
import javax.validation.Validator

import static ratpack.jackson.Jackson.fromJson

@Singleton
@Slf4j
class MovieRestEndpoint extends GroovyChainAction {

  @Inject
  MovieService movieService

  @Inject
  RatingService ratingService

  @Inject
  OMDBService omdbService

  @Inject
  ThrottleService throttleService

  @Inject
  Validator validator

  @Override
  void execute() throws Exception {

    onlyIf({
      Context context ->
        !context.getRequest().getMethod().isGet()
    }, JWTTokenHandler)

    get('movies/search', MovieSearchHandler)

    get('movies/filter') {

      String filterJson = context.request.queryParams.get('filter') ?: '{}'
      String scope = context.request.queryParams.get('scope')

      Observable<Movie> matches = 'public'.equals(scope) ?
        movieService.findMoviesByPublicFilter(filterJson, 20) :
        movieService.findMoviesByFilter(filterJson, 20)

      matches
        .toList()
        .subscribe { List<Movie> movies ->

        render Jackson.json(movies.collect { Movie movie -> ['name': movie.name, 'imdbId': movie.imdbId, 'yearReleased': movie.yearReleased] })
      }
    }

    get('movies/lookup') {

      String title = context.request.queryParams.get('title') ?: ''
      String mode = context.request.queryParams.get('mode')

      Observable<Movie> matches = 'literal'.equals(mode) ?
        movieService.searchMoviesByTitleLiteral(title, 20) :
        movieService.searchMoviesByTitleScript(title, 20)

      matches
        .toList()
        .subscribe { List<Movie> movies ->

        render Jackson.json(movies.collect { Movie movie -> ['name': movie.name, 'imdbId': movie.imdbId, 'yearReleased': movie.yearReleased] })
      }
    }

    get('ratings/search') {

      String prefix = context.request.queryParams.get('q') ?: ''
      String mode = context.request.queryParams.get('mode')

      Observable<Rating> matches = 'literal'.equals(mode) ?
        ratingService.findRatingsByLiteralCommentPrefix(prefix) :
        ratingService.findRatingsByCommentPrefix(prefix)

      matches
        .toList()
        .subscribe { List<Rating> ratings ->

        render Jackson.json(ratings)
      }
    }

    get('ratings/cached') {

      String moviePattern = context.request.queryParams.get('movie') ?: '*'
      String mode = context.request.queryParams.get('mode')

      Observable<Long> cachedCount = 'key'.equals(mode) ?
        ratingService.countCachedComputedRatingsByKey(moviePattern) :
        ratingService.countCachedComputedRatings(moviePattern)

      cachedCount
        .single()
        .subscribe { Long count ->

        render Jackson.json(['count': count])
      }
    }

    get('movies/export') {

      String pageSizeParam = context.request.queryParams.get(Constants.PAGE_SIZE_QUERY_PARAM_KEY) ?: '100'
      String mode = context.request.queryParams.get('mode')

      Integer pageSize = (pageSizeParam as Integer).abs()

      Observable<Movie> rows = 'paged'.equals(mode) ?
        movieService.exportMoviesPage(pageSize) :
        movieService.exportMovies(pageSize)

      rows
        .toList()
        .subscribe { List<Movie> movies ->

        render Jackson.json(movies.collect { Movie movie -> ['name': movie.name, 'imdbId': movie.imdbId] })
      }
    }

    get('movies/placeholder') {

      String preset = context.request.queryParams.get('preset')
      Integer bytes = ((context.request.queryParams.get('bytes') ?: '4096') as Integer).abs()

      render Jackson.json(['bytes': movieService.renderPlaceholderPoster(preset, bytes)])
    }

    get('movies/samples') {

      Integer count = ((context.request.queryParams.get('count') ?: '10') as Integer).abs()
      String mode = context.request.queryParams.get('mode')

      List<String> titles = movieService.generateSampleTitles(count, mode)

      render Jackson.json(['count': titles.size(), 'first': titles.take(3)])
    }

    get('ratings/await') {

      ObjectId movieId = new ObjectId(context.request.queryParams.get('movie'))
      Integer retries = (context.request.queryParams.get('retries') ?: '1') as Integer
      Long delay = (context.request.queryParams.get('delay') ?: '100') as Long
      String mode = context.request.queryParams.get('mode')

      Observable<Boolean> cached = 'bounded'.equals(mode) ?
        ratingService.awaitCachedRatingBounded(movieId, retries, delay) :
        ratingService.awaitCachedRating(movieId, retries, delay)

      cached
        .single()
        .subscribe { Boolean warm ->

        render Jackson.json(['warm': warm])
      }
    }

    get('diagnostics/latency') {

      String profile = context.request.queryParams.get('profile')
      Long millis = (context.request.queryParams.get('ms') ?: '0') as Long

      throttleService.simulateLatency(profile, millis)

      render Jackson.json(['waitedMs': millis])
    }

    get('movies/imdbids') {

      movieService
        .getExistingMovieIMDBIDs()
        .toList()
        .defaultIfEmpty([])
        .subscribe { List<String> movieIMDBIds ->

        render Jackson.json(movieIMDBIds)
      }
    }

    path('movies') {

      byMethod {

        get {
          insert context.get(GetAllMoviesHandler)
        }

        post {

          String imdbId = context.request.queryParams.get('imdbId')

          omdbService
            .getOMDBMovie(imdbId)
            .flatMap({ Movie movie ->

            movieService.createMovie(movie)
          } as Func1)
            .single()
            .subscribe { String movieID ->

            redirect("/api/movies/${movieID}")
          }
        }
      }
    }

    prefix('movies/:id') {

      all(MovieIDExtractionAndVerificationHandler)

      path { ObjectId movieId ->

        byMethod {

          get {

            movieService
              .getMovie(movieId)
              .single()
              .subscribe { Movie movie ->

              render Jackson.json(movie)
            }
          }

          delete {

            movieService
              .deleteMovieByID(movieId)
              .single()
              .subscribe { Boolean success ->

              redirect('/api/movies')
            }
          }
        }
      }

      path('ratings') { ObjectId movieId ->

        byMethod {

          get {

            ratingService
              .getIndividualUserRatingsAndComment(movieId)
              .toList()
              .subscribe { List<ViewableRating> ratings ->

              render Jackson.json(ratings)
            }
          }

          post {

            context.parse(fromJson(Rating))
              .observe()
              .flatMap({ Rating rating ->

              Set<ConstraintViolation<Rating>> violations = validator.validate(rating)
              if (!violations.empty) {
                throw new IllegalArgumentException("There are a number of constraint violations on rating submission... ${violations}")
              }

              rating.movieId = movieId
              rating.userId = context.get(User).id

              ratingService.rateMovie(rating)
            } as Func1)
              .subscribe { Rating rating ->

              redirect("/api/movies/${rating.movieId.toString()}")
            }
          }
        }
      }
    }
  }
}
