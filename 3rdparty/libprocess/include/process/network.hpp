#ifndef __PROCESS_NETWORK_HPP__
#define __PROCESS_NETWORK_HPP__

#include <netdb.h>

#include <sys/socket.h>
#include <sys/types.h>

#include <vector>

#include <process/address.hpp>

#include <stout/net.hpp>
#include <stout/try.hpp>

namespace process {
namespace network {

// Returns a socket file descriptor for the specified options. Note
// that on OS X, the returned socket will have the SO_NOSIGPIPE option
// set.
inline Try<int> socket(int family, int type, int protocol)
{
  int s;
  if ((s = ::socket(family, type, protocol)) == -1) {
    return ErrnoError();
  }

#ifdef __APPLE__
  // Disable SIGPIPE via setsockopt because OS X does not support
  // the MSG_NOSIGNAL flag on send(2).
  const int enable = 1;
  if (setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &enable, sizeof(int)) == -1) {
    return ErrnoError();
  }
#endif // __APPLE__

  return s;
}


// TODO(benh): Remove and defer to Socket::accept.
inline Try<int> accept(int s, sa_family_t family)
{
  switch (family) {
    case AF_INET: {
      sockaddr_in addr = net::createSockaddrIn(0, 0);
      socklen_t addrlen = sizeof(addr);

      int accepted = ::accept(s, (sockaddr*) &addr, &addrlen);
      if (accepted < 0) {
        return ErrnoError("Failed to accept");
      }

      return accepted;
    }
    default:
      return Error("Unsupported family type: " + stringify(family));
  }
}


// TODO(benh): Remove and defer to Socket::bind.
inline Try<int> bind(int s, const Address& address)
{
  sockaddr_in addr = net::createSockaddrIn(address.ip, address.port);

  int error = ::bind(s, (sockaddr*) &addr, sizeof(addr));
  if (error < 0) {
    return ErrnoError("Failed to bind on " + stringify(address));
  }

  return error;
}


// TODO(benh): Remove and defer to Socket::connect.
inline Try<int> connect(int s, const Address& address)
{
  sockaddr_in addr = net::createSockaddrIn(address.ip, address.port);

  int error = ::connect(s, (sockaddr*) &addr, sizeof(addr));
  if (error < 0) {
    return ErrnoError("Failed to connect to " + stringify(address));
  }

  return error;
}


inline Try<Address> address(int s)
{
  union {
    struct sockaddr s;
    struct sockaddr_in v4;
    struct sockaddr_in6 v6;
  } addr;

  socklen_t addrlen = sizeof(addr);

  if (::getsockname(s, (sockaddr*) &addr, &addrlen) < 0) {
    return ErrnoError("Failed to getsockname");
  }

  if (addr.s.sa_family == AF_INET) {
    return Address(addr.v4.sin_addr.s_addr, ntohs(addr.v4.sin_port));
  }

  return Error("Unsupported address family '" +
               stringify(addr.s.sa_family) + "'");
}


// Returns the IP for the provided hostname or an error.
//
// TODO(benh): Make this asynchronous (at least for Linux, use
// getaddrinfo_a) and return a Future.
//
// TODO(benh): Support looking up SRV records (which might mean we
// need to use res_query and other res_* functions.
//
// TODO(benh): Create Address::Family and Socket::Type enums and then
// use those for parameters here:
//     const Address::Family& family = Address::UNSPECIFIED,
//     const Socket::Type& type = Socket::STREAM,
inline Try<std::vector<Address>> resolve(
    const std::string& hostname,
    sa_family_t family = AF_UNSPEC,          // Allow IPv4 or IPv6.
    int type = SOCK_STREAM,                  // Assume TCP over UDP.
    int flags = AI_V4MAPPED | AI_ADDRCONFIG, // See 'man getaddrinfo'.
    int protocol = 0)                        // Any protocol.
{
  struct addrinfo hints;

  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = family;
  hints.ai_socktype = type;
  hints.ai_flags = 0;
  hints.ai_protocol = protocol;

  struct addrinfo* results = NULL;

  int error = getaddrinfo(hostname.c_str(), NULL, &hints, &results);
  if (error != 0) {
    if (results != NULL) {
      freeaddrinfo(results);
    }
    return Error(gai_strerror(error));
  }

  std::vector<Address> addresses;

  struct addrinfo* result = results;

  for (; result != NULL; result = result->ai_next) {
    if (result->ai_family == AF_INET) {
      uint32_t ip = ((struct sockaddr_in*) (result->ai_addr))->sin_addr.s_addr;
      uint16_t port = ((struct sockaddr_in*) (result->ai_addr))->sin_port;

      Address address(ip, port);

      addresses.push_back(address);
    }

    // TODO(benh): Add support for address families other than AF_INET
    // after Address supports other address families.
  }

  freeaddrinfo(results);

  return addresses;
}


// Returns a Try of the hostname for the provided Address. If the
// hostname cannot be resolved, then a string version of the IP
// address is returned.
//
// TODO(benh): Make this asynchronous and return a Future.
//
// TODO(benh): Make an asynchronous version of this just for an IP.
//
// TODO(benh): Make 'address' be optional and return the local
// hostname when None is provided (which could be the default).
inline Try<std::string> hostname(const Address& address)
{
  union {
    struct sockaddr s;
    struct sockaddr_in v4;
    struct sockaddr_in6 v6;
  } addr;

  memset(&addr, 0, sizeof(addr));

  socklen_t addrlen = sizeof(addr);

  switch (address.family()) {
    case AF_INET:
      addr.v4.sin_family = AF_INET;
      addr.v4.sin_addr.s_addr = address.ip;

      addrlen = sizeof(struct sockaddr_in);

    default:
      return Error("Unsupported address family '" +
                   stringify(address.family()) + "'");
  }

  char hostname[MAXHOSTNAMELEN];

  int error = getnameinfo(
      (sockaddr*) &addr,
      addrlen,
      hostname,
      MAXHOSTNAMELEN,
      NULL,
      0,
      0);

  if (error != 0) {
    return Error(std::string(gai_strerror(error)));
  }

  return std::string(hostname);
}

} // namespace network {
} // namespace process {

#endif // __PROCESS_NETWORK_HPP__
