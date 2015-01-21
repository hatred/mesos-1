#include <openssl/err.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <string>

#include <process/once.hpp>

#include <stout/flags.hpp>

using std::string;

// Must be defined by us for OpenSSL in order to capture the necessary
// data for doing locking. Note, this needs to be defined outside of
// any namespace as well.
struct CRYPTO_dynlock_value
{
  pthread_mutex_t mutex;
};


namespace process {
namespace network {
namespace openssl {

// _Global_ OpenSSL context, initialized via 'initialize'.
static SSL_CTX* ctx = NULL;


// Capture the environment variables that influence how we initialize
// the OpenSSL library via flags.
class Flags : public virtual flags::FlagsBase
{
public:
  Flags()
  {
    add(&Flags::cert,
        "cert",
        "Path to certifcate.");

    add(&Flags::key,
        "key",
        "Path to key.");

    add(&Flags::verify_cert,
        "verify_cert",
        "Whether or not to verify peer certificates.",
        false);

    add(&Flags::require_cert,
        "require_cert",
        "Whether or not to require peer certificates.",
        false);

    add(&Flags::ca_dir,
        "ca_dir",
        "Path to certifcate authority (CA) directory.");

    add(&Flags::ca_file,
        "ca_file",
        "Path to certifcate authority (CA) file.");

    add(&Flags::ciphers,
        "ciphers",
        "Cryptographic ciphers to use.",
        // Default TLSv1 ciphers chosen baseed on Amazon's security
        // policy, see:
        // http://docs.aws.amazon.com/ElasticLoadBalancing/latest/
        // DeveloperGuide/elb-security-policy-table.html
        "AES128-SHA:AES256-SHA:RC4-SHA:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA:"
        "DHE-RSA-AES256-SHA:DHE-DSS-AES256-SHA");
  }

  Option<string> cert;
  Option<string> key;
  bool verify_cert;
  bool require_cert;
  Option<string> ca_dir;
  Option<string> ca_file;
  string ciphers;
} flags;


// Default verification depth.
// TODO(benh): Make this a flag (environment variable)?
static const int VERIFICATION_DEPTH = 4;


// Mutexes necessary to support OpenSSL locking on shared data
// structures. See 'locking_function' for more information.
pthread_mutex_t* mutexes = NULL;


// Callback needed to perform locking on shared data structures. From
// the OpenSSL documentation:
//
// OpenSSL uses a number of global data structures that will be
// implicitly shared whenever multiple threads use OpenSSL.
// Multi-threaded applications will crash at random if [the locking
// function] is not set.
void locking_function(int mode, int n, const char* /*file*/, int /*line*/)
{
  if (mode & CRYPTO_LOCK) {
    pthread_mutex_lock(&mutexes[n]);
  } else {
    pthread_mutex_unlock(&mutexes[n]);
  }
}


// OpenSSL callback that returns the current thread ID, necessary for
// OpenSSL threading.
unsigned long id_function()
{
  return (unsigned long) pthread_self();
}


// OpenSSL callback for creating new dynamic "locks", abstracted by
// the CRYPTO_dynlock_value structure.
CRYPTO_dynlock_value* dyn_create_function(
    const char* /*file*/,
    int /*line*/)
{
  CRYPTO_dynlock_value* value = new CRYPTO_dynlock_value();

  if (value == NULL) {
    return NULL;
  }

  pthread_mutex_init(&value->mutex, NULL);

  return value;
}


// OpenSSL callback for locking and unlocking dynamic "locks",
// abstracted by the CRYPTO_dynlock_value structure.
void dyn_lock_function(
    int mode,
    CRYPTO_dynlock_value* value,
    const char* /*file*/, int /*line*/)
{
  if (mode & CRYPTO_LOCK) {
    pthread_mutex_lock(&value->mutex);
  } else {
    pthread_mutex_unlock(&value->mutex);
  }
}


// OpenSSL callback for destroying dynamic "locks", abstracted by the
// CRYPTO_dynlock_value structure.
void dyn_destroy_function(
    CRYPTO_dynlock_value* value,
    const char* /*file*/,
    int /*line*/)
{
  pthread_mutex_destroy(&value->mutex);
  delete value;
}


// Callback for OpenSSL peer certificate verification.
int verify_callback(int ok, X509_STORE_CTX* store)
{
  if (ok != 1) {
    // Construct and log a warning message.
    string message;

    X509* cert = X509_STORE_CTX_get_current_cert(store);
    int error = X509_STORE_CTX_get_error(store);
    int depth = X509_STORE_CTX_get_error_depth(store);

    message += "Error with certificate at depth: " + stringify(depth) + "\n";

    const size_t size = 256;
    char buffer[size];

    // TODO(jmlvanre): use X509_NAME_print_ex instead.
    X509_NAME_oneline(X509_get_issuer_name(cert), buffer, size);

    message += "Issuer: " + stringify(buffer) + "\n";

    // TODO(jmlvanre): use X509_NAME_print_ex instead.
    X509_NAME_oneline(X509_get_subject_name(cert), buffer, size);

    message += "Subject: " + stringify(buffer) + "\n";

    message += "Error (" + stringify(error) + "): " +
      stringify(X509_verify_cert_error_string(error));

    LOG(WARNING) << message;
  }

  return ok;
}


string error_string(int code)
{
  // SSL library guarantees to stay within 120 bytes.
  char buffer[128];

  ERR_error_string(code, buffer);
  string s(buffer);

  if (code == SSL_ERROR_SYSCALL) {
    s += error_string(ERR_get_error());
  }

  return s;
}


void initialize()
{
  static Once* initialized = new Once();

  if (initialized->once()) {
    return;
  }

  // We MUST have entropy, or else there's no point to crypto.
  if (!RAND_poll()) {
    LOG(FATAL) << "SSL socket requires entropy";
  }

  // Initialize the OpenSSL library.
  SSL_load_error_strings();
  SSL_library_init();

  // Prepare mutexes for threading callbacks.
  mutexes = new pthread_mutex_t[CRYPTO_num_locks()];

  for (int i = 0; i < CRYPTO_num_locks(); i++) {
    pthread_mutex_init(&mutexes[i], NULL);
  }

  // Install SSL threading callbacks.
  // TODO(jmlvanre): the id mechanism is deprecated in OpenSSL.
  CRYPTO_set_id_callback(&id_function);
  CRYPTO_set_locking_callback(&locking_function);
  CRYPTO_set_dynlock_create_callback(&dyn_create_function);
  CRYPTO_set_dynlock_lock_callback(&dyn_lock_function);
  CRYPTO_set_dynlock_destroy_callback(&dyn_destroy_function);

  ctx = SSL_CTX_new(SSLv23_method());

  // Disable SSL session caching.
  SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_OFF);

  // Set a session id to avoid connection termination upon
  // re-connect. We can use something more relevant when we care
  // about session caching.
  const uint64_t session_ctx = 7;

  const unsigned char* session_id =
    reinterpret_cast<const unsigned char*>(&session_ctx);

  if (SSL_CTX_set_session_id_context(
          ctx,
          session_id,
          sizeof(session_ctx)) <= 0) {
    LOG(FATAL) << "Session id context size exceeds maximum";
  }

  // Load all the flags prefixed by SSL_ from the environment. See
  // comment at top of openssl.hpp for a full list.
  Try<Nothing> load = flags.load("SSL_");

  if (load.isError()) {
    LOG(FATAL)
      << "Failed to load flags from environment variables (prefixed by SSL_):"
      << load.error();
  }

  // Now do some validation of the flags/environment variables.
  if (flags.key.isNone()) {
    LOG(FATAL) << "SSL requires key! NOTE: Set path with SSL_KEY";
  } else if (flags.cert.isNone()) {
    LOG(FATAL) << "SSL requires certificate! NOTE: Set path with SSL_CERT";
  }

  if (flags.ca_file.isNone()) {
    VLOG(2) << "NOTE: Set CA file path with SSL_CA_FILE";
  }

  if (flags.ca_dir.isNone()) {
    VLOG(2) << "NOTE: Set CA directory path with SSL_CA_DIR";
  }

  if (!flags.verify_cert) {
    VLOG(2) << "Will not verify peer certificate!\n"
            << "NOTE: Set SSL_VERIFY_CERT=true to enable";
  }

  if (!flags.require_cert) {
    VLOG(2) << "Will only verify peer certificate if presented!\n"
            << "NOTE: Set SSL_REQUIRE_CERT=true to require peer certificate";
  }

  if (flags.require_cert && !flags.verify_cert) {
    // Requiring a certificate implies that is should be verified.
    flags.verify_cert = true;
  }

  // Initialize OpenSSL if we've been asked to do verification of peer
  // certificates.
  if (flags.verify_cert) {
    // Set CA locations.
    if (flags.ca_file.isSome() || flags.ca_dir.isSome()) {
      const char* ca_file = flags.ca_file.isSome()
        ? flags.ca_file.get().c_str()
        : NULL;

      const char* ca_dir = flags.ca_dir.isSome()
        ? flags.ca_dir.get().c_str()
        : NULL;

      if (SSL_CTX_load_verify_locations(ctx, ca_file, ca_dir) <= 0) {
        int error = ERR_get_error();
        LOG(FATAL) << "Could not load CA file and/or directory ("
                   << stringify(error)  << "): "
                   << error_string(error) << " -> " << ca_file;
      }

      if (ca_file != NULL) {
        VLOG(2) << "Using CA file: " << ca_file;
      }
    }

    // Set SSL peer verification callback.
    int verify = SSL_VERIFY_PEER;

    if (flags.require_cert) {
      verify |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
    }

    SSL_CTX_set_verify(ctx, verify, &verify_callback);

    if (SSL_CTX_set_default_verify_paths(ctx) <= 0) {
      LOG(FATAL) << "Could not load default CA file and/or directory";
    }

    SSL_CTX_set_verify_depth(ctx, VERIFICATION_DEPTH);
  }

  // Set certificate chain.
  if (SSL_CTX_use_certificate_chain_file(
          ctx,
          flags.cert.get().c_str()) <= 0) {
    LOG(FATAL) << "Could not load cert file";
  }

  // Set private key.
  if (SSL_CTX_use_PrivateKey_file(
          ctx,
          flags.key.get().c_str(),
          SSL_FILETYPE_PEM) <= 0) {
    LOG(FATAL) << "Could not load key file";
  }

  // Validate key.
  if (!SSL_CTX_check_private_key(ctx)) {
    LOG(FATAL) << "Private key does not match the certificate public key";
  }

  VLOG(2) << "Using ciphers: " << flags.ciphers;

  if (SSL_CTX_set_cipher_list(ctx, flags.ciphers.c_str()) == 0) {
    LOG(FATAL) << "Could not set ciphers: " << flags.ciphers;
  }

  // Disable SSLv2.
  SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv2);

  initialized->done();
}


SSL_CTX* context()
{
  // TODO(benh): Always call 'initialize' just in case?
  return ctx;
}


Try<Nothing> verify(SSL* ssl, const Option<string>& hostname)
{
  X509* cert = SSL_get_peer_certificate(ssl);

  if (cert == NULL) {
    if (flags.require_cert) {
      return Error("Peer did not provide certificate");
    }
  } else {
    if (SSL_get_verify_result(ssl) != X509_V_OK) {
      return Error("Could not verify peer certificate");
    }

    if (hostname.isNone()) {
      if (flags.require_cert) {
        return Error("Cannot verify peer certificate: peer hostname unknown");
      }
    } else {
      int extcount = X509_get_ext_count(cert);
      if (extcount <= 0) {
        return Error("X509_get_ext_count failed: " + stringify(extcount));
      }

      bool verified = false;

      for (int i = 0; i < extcount; i++) {
        X509_EXTENSION* ext = X509_get_ext(cert, i);

        const string extstr =
          OBJ_nid2sn(OBJ_obj2nid(X509_EXTENSION_get_object(ext)));

        if (extstr == "subjectAltName") {
#if OPENSSL_VERSION_NUMBER <= 0x00909000L
          X509V3_EXT_METHOD* method = X509V3_EXT_get(ext);
#else
          const X509V3_EXT_METHOD* method = X509V3_EXT_get(ext);
#endif
          if (method == NULL) {
            break;
          }

          const unsigned char* data = ext->value->data;

          STACK_OF(CONF_VALUE)* values = method->i2v(
              method,
              method->d2i(NULL, &data, ext->value->length),
              NULL);

          for (int j = 0; j < sk_CONF_VALUE_num(values); j++) {
            CONF_VALUE* value = sk_CONF_VALUE_value(values, j);
            if ((value->name == "DNS") && (value->value == hostname.get())) {
              verified = true;
              break;
            }
          }
        }

        if (verified) {
          break;
        }
      }

      // If we still haven't verified the hostname, try doing it via
      // the certificate subject name.
      if (!verified) {
        X509_NAME* name = X509_get_subject_name(cert);

        const size_t maxTextBufferSize = 256;

        if (name != NULL) {
          char text[maxTextBufferSize];

          if (X509_NAME_get_text_by_NID(
                  name,
                  NID_commonName,
                  text,
                  maxTextBufferSize) > 0) {
            text[maxTextBufferSize - 1] = 0; // Terminate.

            if (hostname.get() != text) {
              return Error(
                  "Presented CN: " + stringify(text) +
                  " does not match peer hostname name: " + hostname.get());
            }

            verified = true;
          }
        }
      }

      if (!verified) {
        return Error(
            "Could not verify presented certificate with hostname " +
            hostname.get());
      }
    }
  }

  return Nothing();
}

} // namespace openssl {
} // namespace network {
} // namespace process {
