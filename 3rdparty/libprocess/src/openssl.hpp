#include <openssl/ssl.h>

#include <string>

#include <stout/option.hpp>
#include <stout/try.hpp>

namespace process {
namespace network {
namespace openssl {

// Initializes the _global_ OpenSSL context (SSL_CTX) as well as the
// crypto library in order to support multi-threading. The global
// context gets initialized using the environment variables:
//
//    SSL_CERT=(path to certificate)
//    SSL_KEY=(path to key)
//    SSL_VERIFY_CERT=(false|0,true|1)
//    SSL_REQUIRE_CERT=(false|0,true|1)
//    SSL_CA_DIR=(path to CA directory)
//    SSL_CA_FILE=(path to CA file)
//    SSL_CIPHERS=(accepted ciphers separated by ':')
//
// TODO(benh): When/If we need to support multiple contexts in the
// same process, for example for Server Name Indication (SNI), then
// we'll add other functions for initializing an SSL_CTX based on
// these environment variables.
// TODO(nneilsen): Support certification revocation.
void initialize();

// Returns the _global_ OpenSSL context.
SSL_CTX* context();

// Verify that the hostname is properly associated with the peer
// certificate associated with the specified SSL connection.
Try<Nothing> verify(SSL* ssl, const Option<std::string>& hostname);

} // namespace openssl {
} // namespace network {
} // namespace process {
