#ifndef __PROCESS_NETWORK_HPP__
#define __PROCESS_NETWORK_HPP__

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
inline Try<int> accept(int s)
{
  struct sockaddr_storage storage;
  socklen_t storagelen = sizeof(storage);

  int accepted = ::accept(s, (struct sockaddr*) &storage, &storagelen);
  if (accepted < 0) {
    return ErrnoError("Failed to accept");
  }

  return accepted;
}


// TODO(benh): Remove and defer to Socket::bind.
inline Try<int> bind(int s, const Address& address)
{
  struct sockaddr_storage storage =
    net::createSockaddrStorage(address.ip, address.port);

  int error = ::bind(s, (struct sockaddr*) &storage, address.size());
  if (error < 0) {
    return ErrnoError("Failed to bind on " + stringify(address));
  }

  return error;
}


// TODO(benh): Remove and defer to Socket::connect.
inline Try<int> connect(int s, const Address& address)
{
  struct sockaddr_storage storage =
    net::createSockaddrStorage(address.ip, address.port);

  int error = ::connect(s, (struct sockaddr*) &storage, address.size());
  if (error < 0) {
    return ErrnoError("Failed to connect to " + stringify(address));
  }

  return error;
}


// Returns the Address with the assigned ip and assigned port.
// Returns an error if the getsockname system call fails or the family
// type is not supported.
inline Try<Address> address(int s)
{
  struct sockaddr_storage storage;
  socklen_t storagelen = sizeof(storage);

  if(::getsockname(s, (struct sockaddr*) &storage, &storagelen) < 0) {
    return ErrnoError("Failed to getsockname");
  }

  return Address::create(storage);
}


// Returns the IP for the provided "node" (as named by getaddrinfo)
// which may be an IPv4, IPv6, hostanme, or IPv4:port, [IPv6]:port, or
// hostname:port.
//
// TODO(benh): Make this asynchronous (at least for Linux, use
// getaddrinfo_a) and return a Future.
//
// TODO(benh): Support looking up SRV records (which might mean we
// need to use res_query and other res_* functions.
//
// TODO(benh): Create Address::Family and Socket::Type enums and then
// use those for parameters here instead of 'sa_family_t' for family
// or 'int' for type:
//     const Address::Family& family = Address::UNSPECIFIED,
//     const Socket::Type& type = Socket::TCP,
inline Try<std::vector<Address>> resolve(
    std::string node,                         // {IPv4, [IPv6], hostname}:port.
    Option<std::string> service = None(),     // Service name or port.
    int type = SOCK_STREAM,                   // Assume TCP over UDP.
    sa_family_t family = AF_UNSPEC,           // Allow IPv4 or IPv6.
    int protocol = 0,                         // Any protocol.
    int flags = AI_V4MAPPED | AI_ADDRCONFIG)  // See 'man getaddrinfo'.
  {
    // Determine the "node" parameter for getaddrinfo (either a
    // literal IPv4 or IPv6, or a hostname). But first try and see if
    // we were giving an IPv4:port, [IPv6]:port, or hostname:port and
    // split out the port.
    if (strings::contains(node, ":")) {
      // Make sure a 'service' wasn't already provided.
      if (service.isSome()) {
        return Error("Not expecting service in addition to a port");
      }

      // Determine 'node' and 'service' depending on if IPv4, IPv6, or
      // hostname.
      if (strings::startsWith(node, "[")) {
        // Parse as IPv6.
        std::vector<std::string> split = strings::split(node, "]");

        // Expecting:
        //   split[0] == "[IPv6"
        //   split[1] == ":port"
        //
        // Otherwise this is malformed!
        if (split.size() != 2 || !strings::startsWith(split[1], ":")) {
          return Error("Malformed [IPv6]:port '" + node + "'");
        }

        node = strings::remove(split[0], "[", strings::PREFIX);
        service = strings::remove(split[1], ":", strings::PREFIX);
      } else {
        // Parse as IPv4 or a hostname.
        std::vector<std::string> split = strings::split(node, ":");
        CHECK(split.size() == 2);
        node = split[0];
        service = split[1];
      }
    }

    struct addrinfo hints;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = family;
    hints.ai_socktype = type;
    hints.ai_flags = 0;
    hints.ai_protocol = protocol;

    struct addrinfo* results = NULL;

    int error = getaddrinfo(
        node.c_str(),
        service.isSome() ? service.get().c_str() : NULL,
        &hints,
        &results);

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
        Address address(
            ((struct sockaddr_in*) (result->ai_addr))->sin_addr.s_addr,
            ntohs(((struct sockaddr_in*) (result->ai_addr))->sin_port));
        addresses.push_back(address);
      }

      // TODO(benh): Add support for address families other than AF_INET
      // after Address supports other address families.
    }

    freeaddrinfo(results);

    return addresses;
  }


} // namespace network {
} // namespace process {

#endif // __PROCESS_NETWORK_HPP__
