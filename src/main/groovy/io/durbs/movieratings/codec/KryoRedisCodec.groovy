package io.durbs.movieratings.codec

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.UnsafeInput
import com.esotericsoftware.kryo.io.UnsafeOutput
import com.esotericsoftware.kryo.pool.KryoCallback
import com.esotericsoftware.kryo.pool.KryoPool
import com.google.common.base.Charsets
import com.lambdaworks.redis.codec.RedisCodec
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.durbs.movieratings.Constants
import org.bson.types.ObjectId

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.zip.GZIPInputStream

@CompileStatic
@Slf4j
class KryoRedisCodec<T> implements RedisCodec<String, T> {

  KryoPool kryoPool

  Class<T> type

  KryoRedisCodec(Class<T> type, KryoPool kryoPool) {
    this.type = type
    this.kryoPool = kryoPool
  }

  @Override
  String decodeKey(ByteBuffer bytes) {

    Charsets.UTF_8.decode(bytes).toString()
  }

  @Override
  T decodeValue(ByteBuffer byteBuffer) {

    T value = null

    byte[] bytes = new byte[byteBuffer.remaining()]
    byteBuffer.get(bytes)

    kryoPool.run(new KryoCallback<T>() {

      @Override
      T execute(Kryo kryo) {
        value = kryo.readObject(new UnsafeInput(bytes), type)
      }
    })
  }

  @Override
  ByteBuffer encodeKey(String key) {

    Charsets.UTF_8.encode(key)
  }

  @Override
  ByteBuffer encodeValue(T object) {

    UnsafeOutput output = new UnsafeOutput(new ByteArrayOutputStream(), Constants.DEFAULT_KRYO_BUFFER)

    Kryo kryo = null

    try {

      kryo = kryoPool.borrow()
      kryo.writeObject(output, object)

    } catch (Exception exception) {

      log.error("An error ocurred attempting to encode ${object}", exception)

    } finally {

      if (kryo) {

        kryoPool.release(kryo)
      }
    }

    ByteBuffer.wrap(output.toBytes())
  }

  /**
   * Restores a value that a client handed back to us, e.g. a draft that was parked in a request header.
   */
  T decodeClientToken(String token) {

    byte[] bytes = token.decodeBase64()

    Object restored = kryoPool.run(new KryoCallback<Object>() {

      @Override
      Object execute(Kryo kryo) {
        kryo.readClassAndObject(new UnsafeInput(bytes)) // SINK: PLANTED-GR-HR-142
      }
    })

    type.cast(restored)
  }

  /**
   * Same as {@link #decodeClientToken} but only ever materialises the one registered value type.
   */
  T decodeStrictClientToken(String token) {

    Kryo strictKryo = new Kryo()
    strictKryo.setRegistrationRequired(true)
    strictKryo.register(type)
    strictKryo.register(ObjectId)

    T restored = strictKryo.readObject(new UnsafeInput(token.decodeBase64()), type) // SAFE_SINK: PLANTED-GR-HR-142-safe

    restored
  }

  /**
   * Restores the session state that the web client keeps in a cookie (base64 of a gzipped serialized map).
   */
  static Object decodeSessionCookie(String cookieValue) {

    ObjectInputStream sessionStream = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(cookieValue.decodeBase64())))

    try {

      return sessionStream.readObject() // SINK: PLANTED-GR-HR-144
    } finally {

      sessionStream.close()
    }
  }

  private static final String SESSION_SIGNING_SECRET = resolveSessionSigningSecret()

  /**
   * The key for the session-cookie HMAC: a per-instance secret that is not part of the repository. It is taken from
   * the RATINGS_SESSION_SIGNING_SECRET environment variable (or the ratings.session.signing.secret system property),
   * falling back to a random value generated once at startup and held in memory only.
   */
  static String sessionSigningSecret() {

    SESSION_SIGNING_SECRET
  }

  private static String resolveSessionSigningSecret() {

    String configured = System.getProperty('ratings.session.signing.secret') ?: System.getenv('RATINGS_SESSION_SIGNING_SECRET')

    if (configured) {
      return configured
    }

    byte[] random = new byte[32]
    new SecureRandom().nextBytes(random)

    random.encodeHex().toString()
  }

  /**
   * Restores session state from a cookie of the form base64(payload).base64(hmac); the signature is checked first.
   */
  static Object decodeSignedSessionCookie(String cookieValue, String secret) {

    int separator = cookieValue.lastIndexOf('.')

    if (separator < 0) {
      throw new SecurityException('The session cookie is not signed')
    }

    byte[] payload = cookieValue.substring(0, separator).decodeBase64()
    byte[] signature = cookieValue.substring(separator + 1).decodeBase64()

    Mac mac = Mac.getInstance('HmacSHA256')
    mac.init(new SecretKeySpec(secret.getBytes('UTF-8'), 'HmacSHA256'))

    if (!MessageDigest.isEqual(mac.doFinal(payload), signature)) {
      throw new SecurityException('The session cookie signature does not match')
    }

    ObjectInputStream sessionStream = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(payload)))

    try {

      return sessionStream.readObject() // SAFE_SINK: PLANTED-GR-HR-144-safe
    } finally {

      sessionStream.close()
    }
  }

}
