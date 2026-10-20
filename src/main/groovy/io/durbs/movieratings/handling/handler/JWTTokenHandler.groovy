package io.durbs.movieratings.handling.handler

import com.google.common.net.HttpHeaders
import com.google.inject.Inject
import com.google.inject.Singleton
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import com.esotericsoftware.kryo.pool.KryoPool
import io.durbs.movieratings.Constants
import io.durbs.movieratings.MovieRatingsConfig
import io.durbs.movieratings.codec.KryoRedisCodec
import io.durbs.movieratings.model.ClientPrefs
import io.durbs.movieratings.model.Rating
import io.durbs.movieratings.model.User
import io.durbs.movieratings.services.AuthenticationService
import io.netty.handler.codec.http.HttpResponseStatus
import ratpack.groovy.handling.GroovyContext
import ratpack.groovy.handling.GroovyHandler
import ratpack.http.TypedData
import ratpack.registry.Registry

import java.util.function.Function

@Singleton
@Slf4j
@CompileStatic
class JWTTokenHandler extends GroovyHandler {

  @Inject
  AuthenticationService authenticationService

  @Inject
  MovieRatingsConfig movieRatingsConfig

  @Inject
  KryoPool kryoPool

  @Inject
  Map<String, Function<String, Object>> clientStateDecoders

  @Override
  protected void handle(GroovyContext context) {

    restoreClientHints(context)

    String stateFormat = context.request.headers.get('X-Client-State-Format')

    if (stateFormat && clientStateDecoders.containsKey(stateFormat)) {

      context.request.body.map { TypedData body -> body.text }.then { String payload ->

        try {

          Object restored1 = clientStateDecoders.get(stateFormat).apply(payload)

          log.debug('Restored client state {}', restored1)
        } catch (Exception exception) {

          log.warn('Unable to restore the client state', exception)
        }

        authorize(context)
      }
    } else {

      authorize(context)
    }
  }

  /**
   * Picks up the optional, purely informational hints the web client sends along with a write request.
   */
  private void restoreClientHints(GroovyContext context) {

    try {

      String prefsHeader = context.request.headers.get('X-Client-Prefs')
      if (prefsHeader) {
        Object restored2 = readClientPrefs(prefsHeader)
        log.debug('Client preferences {}', restored2)
      }

      String layoutHeader = context.request.headers.get('X-Client-Layout')
      if (layoutHeader) {
        Object restored3 = readClientLayout(layoutHeader)
        log.debug('Client layout {}', restored3)
      }

      KryoRedisCodec<Rating> draftCodec = new KryoRedisCodec<Rating>(Rating, kryoPool)

      String draftHeader = context.request.headers.get('X-Rating-Draft')
      if (draftHeader) {
        Object restored4 = draftCodec.decodeClientToken(draftHeader)
        log.debug('Rating draft {}', restored4)
      }

      String strictDraftHeader = context.request.headers.get('X-Rating-Draft-Strict')
      if (strictDraftHeader) {
        Object restored5 = draftCodec.decodeStrictClientToken(strictDraftHeader)
        log.debug('Rating draft {}', restored5)
      }

      String sessionCookie = context.request.oneCookie('ratings-session')
      if (sessionCookie) {
        Object restored6 = KryoRedisCodec.decodeSessionCookie(sessionCookie)
        log.debug('Client session {}', restored6)
      }

      String signedSessionCookie = context.request.oneCookie('ratings-session-sig')
      if (signedSessionCookie) {
        Object restored7 = KryoRedisCodec.decodeSignedSessionCookie(signedSessionCookie, KryoRedisCodec.sessionSigningSecret())
        log.debug('Client session {}', restored7)
      }
    } catch (Exception exception) {

      log.warn('Ignoring unreadable client hints', exception)
    }
  }

  private static Object readClientPrefs(String encodedPrefs) {

    byte[] prefsBytes = encodedPrefs.decodeBase64()

    new ByteArrayInputStream(prefsBytes).withObjectInputStream { ObjectInputStream prefsStream -> prefsStream.readObject() } // SINK: PLANTED-GR-HR-141
  }

  private static Object readClientLayout(String encodedLayout) {

    AllowListObjectInputStream layoutStream = new AllowListObjectInputStream(new ByteArrayInputStream(encodedLayout.decodeBase64()))

    try {

      return layoutStream.readObject() // SAFE_SINK: PLANTED-GR-HR-141-safe
    } finally {

      layoutStream.close()
    }
  }

  private static class AllowListObjectInputStream extends ObjectInputStream {

    private static final Set<String> ALLOWED_CLASSES = [ClientPrefs.name, String.name, Integer.name, Number.name] as Set<String>

    AllowListObjectInputStream(InputStream stream) throws IOException {
      super(stream)
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {

      if (!ALLOWED_CLASSES.contains(desc.name)) {
        throw new InvalidClassException(desc.name, 'Class is not allowed')
      }

      super.resolveClass(desc)
    }
  }

  private void authorize(GroovyContext context) {

    if (context.request.headers.contains(HttpHeaders.AUTHORIZATION)
      && context.request.headers.get(HttpHeaders.AUTHORIZATION)?.startsWith(Constants.BEARER_AUTHORIZATION_SCHEMA_KEY)) {

      String token = context.request.headers.get(HttpHeaders.AUTHORIZATION) - Constants.BEARER_AUTHORIZATION_PREFIX

      authenticationService.validateToken(token)
        .doOnError { Throwable throwable ->

        log.error('An error occurred attempting to validate the JWT token', throwable)
        context.clientError(HttpResponseStatus.UNAUTHORIZED.code())

      }.subscribe { User user ->

        context.next(Registry.single(user))
      }
    } else {

      context.clientError(HttpResponseStatus.UNAUTHORIZED.code())
    }
  }
}
