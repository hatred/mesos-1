#include <event2/buffer.h>
#include <event2/bufferevent_ssl.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>

#include <openssl/ssl.h>

#include <process/socket.hpp>

#include <stout/net.hpp>

#include "libevent.hpp"
#include "synchronized.hpp"
#include "openssl.hpp"

// Locking:
// We use the BEV_OPT_THREADSAFE flag when constructing bufferevents
// so that all bufferevent specific functions run from within the
// event loop will have the lock on the bufferevent already acquired.
// This means that everywhere else we need to manually call
// 'bufferevent_lock(bev)' and 'bufferevent_unlock(bev)'; However, due
// to a deadlock scneario in libevent-openssl (v 2.0.21) we currently
// modify bufferevents using continuations in the event loop. See
// 'Continuation' comment below.

// Continuation:
// There is a deadlock scenario in libevent-openssl (v 2.0.21) when
// modifying the bufferevent (bev) from another thread (not the event
// loop). To avoid this we run all bufferevent manipulation logic in
// continuations that are executed within the event loop.

// Connection Extra FD:
// In libevent-openssl (v 2.0.21) we've had issues using the
// 'bufferevent_openssl_socket_new' call with the CONNECTING state and
// an existing socket. Therefore we allow it to construct its own
// fd and clean it up along with the Impl object when the bev is
// freed using the BEV_OPT_CLOSE_ON_FREE option.

// Socket KeepAlive:
// We want to make sure that sockets don't get destroyed while there
// are still requests pending on them. If they did, then the
// continuation functions for those requests would be acting on bad
// 'this' objects. To ensure this, we keep a copy of the socket in the
// request, which gets destroyed once the request is satisfied or
// discarded.

using std::queue;
using std::string;

namespace process {
namespace network {

class LibeventSSLSocketImpl : public Socket::Impl
{
  // Forward declaration for constructor.
  struct AcceptRequest;

public:
  LibeventSSLSocketImpl(int _s, AcceptRequest* request = NULL);

  virtual ~LibeventSSLSocketImpl()
  {
    if (bev  != NULL) {
      SSL* ssl = bufferevent_openssl_get_ssl(bev);
      // Workaround for SSL shutdown, see http://www.wangafu.net/~nickm/libevent-book/Ref6a_advanced_bufferevents.html // NOLINT
      SSL_set_shutdown(ssl, SSL_RECEIVED_SHUTDOWN);
      SSL_shutdown(ssl);
      bufferevent_disable(bev, EV_READ | EV_WRITE);

      // For the connecting socket BEV_OPT_CLOSE_ON_FREE will close
      // the fd. See note below.
      bufferevent_free(bev);

      // Since we are using a separate fd for the connecting socket we
      // end up using BEV_OPT_CLOSE_ON_FREE for the connecting, but
      // not for the accepting side. since the BEV_OPT_CLOSE_ON_FREE
      // also frees the SSL object, we need to manually free it for
      // the accepting case. See the 'Connection Extra FD' note at top
      // of file.
      if (freeSSLCtx) {
        SSL_free(ssl);
      }
    }
    if (listener != NULL) {
      evconnlistener_free(listener);
    }
  }

  virtual Future<Nothing> connect(const Address& address);

  virtual Future<size_t> recv(char* data, size_t size);

  virtual Future<size_t> send(const char* data, size_t size);

  virtual Future<size_t> sendfile(int fd, off_t offset, size_t size);

  virtual Try<Nothing> listen(int backlog);

  virtual Future<Socket> accept();

  virtual void shutdown();

private:
  struct RecvRequest
  {
    RecvRequest(char* _data, size_t _size, Socket&& _socket)
      : data(_data), size(_size), socket(std::move(_socket)) {}
    Promise<size_t> promise;
    char* data;
    size_t size;
    // Keep alive until the request is handled or discarded. See
    // 'Socket KeepAlive' note at top of file.
    Socket socket;
  };

  struct SendRequest
  {
    SendRequest(size_t _size, Socket&& _socket)
      : size(_size), socket(std::move(_socket)) {}
    Promise<size_t> promise;
    size_t size;
    // Keep alive until the request is handled or discarded. See
    // 'Socket KeepAlive' note at top of file.
    Socket socket;
  };

  struct ConnectRequest
  {
    ConnectRequest(Socket&& _socket) : socket(std::move(_socket)) {}
    Promise<Nothing> promise;
    // Keep alive until the request is handled or discarded. See
    // 'Socket KeepAlive' note at top of file.
    Socket socket;
  };

  struct AcceptRequest
  {
    AcceptRequest(Socket&& _accepting_socket)
      : acceptingSocket(std::move(_accepting_socket)),
        self(NULL),
        bev(NULL),
        acceptedSocket(NULL),
        sa(NULL) {}
    // Keep alive until the request is handled or discarded. See
    // 'Socket KeepAlive' note at top of file.
    Socket acceptingSocket;
    LibeventSSLSocketImpl* self;
    Promise<Socket> promise;
    struct bufferevent* bev;
    Socket* acceptedSocket;
    int sock;
    struct sockaddr* sa;
    int sa_len;
  };

  void _shutdown();

  void _recv(size_t size);

  void _send(const char* data, size_t size);

  void _sendfile(int fd, off_t offset, size_t size);

  void _accept();

  static void recvCallback(struct bufferevent* bev, void* arg);

  static void sendCallback(struct bufferevent* bev, void* arg);

  static void eventCallback(struct bufferevent* bev, short events, void* arg);

  void _discardRecv(RecvRequest* request);
  void discardRecv(RecvRequest* request);

  void _discardSend(SendRequest* request);
  void discardSend(SendRequest* request);

  void _discardConnect(ConnectRequest* request);
  void discardConnect(ConnectRequest* request);

  void doAccept(
    int sock,
    struct sockaddr* sa,
    int sa_len);

  static void acceptCallback(
    struct evconnlistener* listener,
    int sock,
    struct sockaddr* sa,
    int sa_len,
    void* arg);

  struct bufferevent* bev;

  struct evconnlistener* listener;

  // Protects the following instance variables.
  synchronizable(this);
  RecvRequest* recvRequest;
  SendRequest* sendRequest;
  ConnectRequest* connectRequest;
  AcceptRequest* acceptRequest;

  struct PendingAccept {
    int sock;
    struct sockaddr* sa;
    int sa_len;
    void* arg;
  };

  // This queue stores buffered accepted sockets. It should only be
  // accessed from within the event loop.
  queue<PendingAccept> pendingAccepts;

  bool freeSSLCtx;

  Option<string> peerHostname;
};


Try<std::shared_ptr<Socket::Impl>> libeventSSLSocket(int s)
{
  openssl::initialize();
  return std::make_shared<LibeventSSLSocketImpl>(s);
}


namespace internal {

template <typename Request, typename Value>
void satisfyRequest(Request* request, const Value& value)
{
  CHECK_NOTNULL(request);
  if (request->promise.future().hasDiscard()) {
    request->promise.discard();
  } else {
    request->promise.set(value);
  }

  delete request;
}

template <typename Request>
void failRequest(Request* request, const string& error)
{
  CHECK_NOTNULL(request);
  if (request->promise.future().hasDiscard()) {
    request->promise.discard();
  } else {
    request->promise.fail(error);
  }

  delete request;
}

} // namespace internal {


// This function runs in the event loop. It is a continuation of
// 'shutdown'. See 'Continuation' note at top of file.
void LibeventSSLSocketImpl::_shutdown()
{
  CHECK_NOTNULL(bev);

  bufferevent_lock(bev);
  { // Bev locking scope.
    RecvRequest* request = NULL;

    // Swap the recvRequest under the object lock.
    synchronized (this) {
      std::swap(request, recvRequest);
    }

    // If there is still a pending receive request then close it.
    if (request != NULL) {
      internal::satisfyRequest(request, 0);
    }
  } // End bev locking scope.
  bufferevent_unlock(bev);
}


void LibeventSSLSocketImpl::shutdown()
{
  run_in_event_loop(lambda::bind(&LibeventSSLSocketImpl::_shutdown, this));
}


// This callback is run within the event loop. No locks required. See
// 'Locking' note at top of file.
void LibeventSSLSocketImpl::recvCallback(struct bufferevent* bev, void* arg)
{
  LibeventSSLSocketImpl* impl =
    reinterpret_cast<LibeventSSLSocketImpl*>(CHECK_NOTNULL(arg));

  RecvRequest* request = NULL;
  // Manually construct the lock as the macro does not work for
  // acquiring it from another instance.
  if (Synchronized __synchronizedthis =
      Synchronized(&(impl->__synchronizable_this))) {
    std::swap(request, impl->recvRequest);
  }

  if (request != NULL) {
    bufferevent_disable(bev, EV_READ);
    internal::satisfyRequest(
        request,
        bufferevent_read(bev, request->data, request->size));
  }
}

// This callback is run within the event loop. No locks required. See
// 'Locking' note at top of file.
void LibeventSSLSocketImpl::sendCallback(struct bufferevent* bev, void* arg)
{
  LibeventSSLSocketImpl* impl =
    reinterpret_cast<LibeventSSLSocketImpl*>(CHECK_NOTNULL(arg));

  SendRequest* request = NULL;
  // Manually construct the lock as the macro does not work for
  // acquiring it from another instance.
  if (Synchronized __synchronizedthis =
      Synchronized(&(impl->__synchronizable_this))) {
    std::swap(request, impl->sendRequest);
  }

  if (request != NULL) {
    internal::satisfyRequest(request, request->size);
  }
}

// This callback is run within the event loop. No locks required. See
// 'Locking' note at top of file.
void LibeventSSLSocketImpl::eventCallback(
    struct bufferevent* bev,
    short events,
    void* arg)
{
  LibeventSSLSocketImpl* impl =
    reinterpret_cast<LibeventSSLSocketImpl*>(CHECK_NOTNULL(arg));

  RecvRequest* currentRecvRequest = NULL;
  SendRequest* currentSendRequest = NULL;
  ConnectRequest* currentConnectRequest = NULL;
  AcceptRequest* currentAcceptRequest = NULL;
  // In all of the following conditions, we're interested in swapping
  // the value of the requests with null (if they are already null,
  // then there's no harm).
  if (events & BEV_EVENT_EOF ||
      events & BEV_EVENT_CONNECTED ||
      (events & BEV_EVENT_ERROR && EVUTIL_SOCKET_ERROR() != 0)) {
    // Manually construct the lock as the macro does not work for
    // acquiring it from another instance.
    if (Synchronized __synchronizedthis =
        Synchronized(&(impl->__synchronizable_this))) {
      std::swap(currentRecvRequest, impl->recvRequest);
      std::swap(currentSendRequest, impl->sendRequest);
      std::swap(currentConnectRequest, impl->connectRequest);
      std::swap(currentAcceptRequest, impl->acceptRequest);
    }
  }

  // If a request below is null, then no such request is in progress,
  // either because it was never created, it has already been
  // completed, or it has been discarded.

  if (events & BEV_EVENT_EOF) {
    // At end of file, close the connection.
    if (currentRecvRequest != NULL) {
      internal::satisfyRequest(currentRecvRequest, 0);
    }
    if (currentSendRequest != NULL) {
      internal::satisfyRequest(currentSendRequest, 0);
    }
    if (currentConnectRequest != NULL) {
      internal::failRequest(
          currentConnectRequest,
          "Failed connect: connection closed");
    }
    if (currentAcceptRequest != NULL) {
      internal::failRequest(
          currentAcceptRequest,
          "Failed accept: connection closed");
    }
  } else if (events & BEV_EVENT_ERROR && EVUTIL_SOCKET_ERROR() != 0) {
    // If there is a valid error, fail any requests and log the error.
    VLOG(1) << "Socket error: "
            << stringify(evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
    if (currentRecvRequest != NULL) {
      internal::failRequest(
          currentRecvRequest,
          "Failed recv, connection error: " +
          stringify(evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())));
    }
    if (currentSendRequest != NULL) {
      internal::failRequest(
          currentSendRequest,
          "Failed send, connection error: " +
          stringify(evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())));
    }
    if (currentConnectRequest != NULL) {
      internal::failRequest(
          currentConnectRequest,
          "Failed connect, connection error: " +
          stringify(evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())));
    }
    if (currentAcceptRequest != NULL) {
      internal::failRequest(
          currentAcceptRequest,
          "Failed accept: connection error: " +
          stringify(evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())));
    }
  } else if (events & BEV_EVENT_CONNECTED) {
    // We should not have receiving or sending request while still
    // connecting.
    CHECK(currentRecvRequest == NULL);
    CHECK(currentSendRequest == NULL);
    if (currentConnectRequest != NULL) {
      // If we're connecting, then we've succeeded. Time to do
      // post-verification.
      CHECK_NOTNULL(impl->bev);

      // Do post-validation of connection.
      SSL* ssl = bufferevent_openssl_get_ssl(impl->bev);

      Try<Nothing> verify = openssl::verify(ssl, impl->peerHostname);

      if (verify.isError()) {
        VLOG(1) << "Failed connect, post verification error: "
                << verify.error();
        internal::failRequest(currentConnectRequest, verify.error());
        return;
      }

      internal::satisfyRequest(currentConnectRequest, Nothing());
    }
    if (currentAcceptRequest != NULL) {
      // We will receive a 'CONNECTED' state on an accepting socket
      // once the connection is established. Time to do
      // post-verification.
      SSL* ssl = bufferevent_openssl_get_ssl(bev);

      Try<Nothing> verify = openssl::verify(ssl, impl->peerHostname);

      if (verify.isError()) {
        VLOG(1) << "Failed accept, post verification error: "
                << verify.error();
        delete currentAcceptRequest->acceptedSocket;
        internal::failRequest(currentAcceptRequest, verify.error());
        return;
      }

      Socket* socket = currentAcceptRequest->acceptedSocket;
      internal::satisfyRequest(currentAcceptRequest, impl->socket());
      delete socket;
    }
  }
}


// For the connecting socket we currently don't use the fd associated
// with 'Socket'. See the 'Connection Extra FD' note at top of file.
LibeventSSLSocketImpl::LibeventSSLSocketImpl(int _s, AcceptRequest* request)
  : Socket::Impl(_s),
    bev(NULL),
    listener(NULL),
    recvRequest(NULL),
    sendRequest(NULL),
    connectRequest(NULL),
    acceptRequest(NULL),
    freeSSLCtx(false)
{
  synchronizer(this) = SYNCHRONIZED_INITIALIZER;
  if (request != NULL) {
    bev = request->bev;
    freeSSLCtx = bev != NULL;
    acceptRequest = request;
    Try<string> hostname =
    net::getHostname(
        reinterpret_cast<sockaddr_in*>(request->sa)->sin_addr.s_addr);
    if (hostname.isError()) {
      VLOG(2) << "Could not determine hostname of peer";
    } else {
      VLOG(2) << "Accepting from " << hostname.get();
      peerHostname = hostname.get();
    }
    request->self = this;
  }
}

// This function runs in the event loop. It is a continuation of
// 'discardConnect'. See 'Continuation' note at top of file.
void LibeventSSLSocketImpl::_discardConnect(ConnectRequest* request)
{
  bool discard = false;

  synchronized (this) {
    // Only discard if the active request matches what we're trying to
    // discard. Otherwise it has been already completed.
    if (connectRequest == request) {
      discard = true;
      connectRequest = NULL;
    }
  }

  // Discard the promise outside of the object lock as the callbacks
  // can be expensive.
  if (discard) {
    request->promise.discard();
    delete request;
  }
}


void LibeventSSLSocketImpl::discardConnect(ConnectRequest* request)
{
  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_discardConnect,
      this,
      request));
}


Future<Nothing> LibeventSSLSocketImpl::connect(const Address& address)
{
  if (connectRequest != NULL) {
    return Failure("Socket is already connecting");
  }

  if (address.ip == 0) {
    // Set the local hostname on the socket for peer validation.
    const Try<string> hostname = net::hostname();
    if (hostname.isSome()) {
      peerHostname = hostname.get();
    }
  } else {
    // If the connecting address is not local, then set the remote
    // hostname for peer validation.
    const Try<string> hostname = net::getHostname(address.ip);
    if (hostname.isError()) {
      VLOG(2) << "Could not determine hostname of peer";
    } else {
      VLOG(2) << "Connecting to " << hostname.get();
      peerHostname = hostname.get();
    }
  }

  CHECK(bev == NULL);

  SSL* ssl = SSL_new(openssl::context());
  if (ssl == NULL) {
    return Failure("Failed to connect: SSL_new");
  }

  // Construct the bufferevent in the connecting state. We don't use
  // the existing FD due to an issue in libevent-openssl. See the
  // 'Connection Extra FD' note at top of file.
  bev = bufferevent_openssl_socket_new(
      base,
      -1,
      ssl,
      BUFFEREVENT_SSL_CONNECTING,
      BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);

  if (bev == NULL) {
    return Failure(
      "Failed to connect: bufferevent_openssl_socket_new");
  }

  // Assign the callbacks for the bufferevent.
  bufferevent_setcb(
      bev,
      &LibeventSSLSocketImpl::recvCallback,
      &LibeventSSLSocketImpl::sendCallback,
      &LibeventSSLSocketImpl::eventCallback,
      this);

  // TODO(jmlvanre): sync on new IP address setup.
  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = PF_INET;
  addr.sin_port = htons(address.port);
  addr.sin_addr.s_addr = address.ip;


  // Optimistically construct a 'connectRequest' and future.
  ConnectRequest* request = new ConnectRequest(socket());
  Future<Nothing> future = request->promise.future()
    .onDiscard(lambda::bind(
        &LibeventSSLSocketImpl::discardConnect,
        this,
        request));

  // Assign 'connectRequest' under lock, fail on error.
  synchronized (this) {
    if (connectRequest != NULL) {
      delete request;
      return Failure("Socket is already connecting");
    } else {
      connectRequest = request;
    }
  }

  if (bufferevent_socket_connect(
      bev,
      reinterpret_cast<struct sockaddr*>(&addr),
      sizeof(addr)) < 0) {
    return Failure("Failed to connect: bufferevent_socket_connect");
  }

  return future;
}


// This function runs in the event loop. It is a continuation of
// 'discardRecv'. See 'Continuation' note at top of file.
void LibeventSSLSocketImpl::_discardRecv(RecvRequest* request)
{
  bool discard = false;

  synchronized (this) {
    // Only discard if the active request matches what we're trying to
    // discard. Otherwise it has been already completed.
    if (recvRequest == request) {
      discard = true;
      recvRequest = NULL;
    }
  }

  // Discard the promise outside of the object lock as the callbacks
  // can be expensive.
  if (discard) {
    bufferevent_disable(bev, EV_READ);
    request->promise.discard();
    delete request;
  }
}


void LibeventSSLSocketImpl::discardRecv(RecvRequest* request)
{
  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_discardRecv,
      this,
      request));
}


// This function runs in the event loop. It is a continuation of
// 'recv'. See 'Continuation' note at top of file.
void LibeventSSLSocketImpl::_recv(size_t size)
{
  bool recv = false;

  synchronized (this) {
    // Only recv if there is an active request. If 'recvRequest' is
    // NULL then the request has been discarded.
    if (recvRequest != NULL) {
      recv = true;
    }
  }

  if (recv) {
    bufferevent_setwatermark(bev, EV_READ, 0, size);
    bufferevent_enable(bev, EV_READ);
  }
}


Future<size_t> LibeventSSLSocketImpl::recv(char* data, size_t size)
{
  // Optimistically construct a 'RecvRequest' and future.
  // Copy this socket into the request to keep the it alive.
  RecvRequest* request = new RecvRequest(data, size, socket());
  Future<size_t> future = request->promise.future()
    .onDiscard(lambda::bind(
        &LibeventSSLSocketImpl::discardRecv,
        this,
        request));

  // Assign 'recvRequest' under lock, fail on error.
  synchronized (this) {
    if (recvRequest != NULL) {
      delete request;
      return Failure("Socket is already receiving");
    } else {
      recvRequest = request;
    }
  }

  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_recv,
      this,
      size));

  return future;
}

// This function runs in the event loop. It is a continuation of
// 'discardSend'. See 'Continuation' note at top of file.
void LibeventSSLSocketImpl::_discardSend(SendRequest* request)
{
  bool discard = false;

  synchronized (this) {
    // Only discard if the active request matches what we're trying to
    // discard. Otherwise it has been already completed.
    if (sendRequest == request) {
      discard = true;
      sendRequest = NULL;
    }
  }

  // Discard the promise outside of the object lock as the callbacks
  // can be expensive.
  if (discard) {
    request->promise.discard();
    delete request;
  }
}


void LibeventSSLSocketImpl::discardSend(SendRequest* request)
{
  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_discardSend,
      this,
      request));
}


// This function runs in the event loop. It is a continuation of
// 'send'. See 'Continuation' note at top of file.
void LibeventSSLSocketImpl::_send(const char* data, size_t size)
{
  bool send = false;

  synchronized (this) {
    // Only send if there is an active request. If 'sendRequest' is
    // NULL then the request has been discarded.
    if (sendRequest != NULL) {
      send = true;
    }
  }

  if (send) {
    bufferevent_write(bev, data, size);
  }
}


Future<size_t> LibeventSSLSocketImpl::send(const char* data, size_t size)
{
  // Optimistically construct a 'SendRequest' and future.
  // Copy this socket into the request to keep the it alive.
  SendRequest* request = new SendRequest(size, socket());
  Future<size_t> future = request->promise.future()
    .onDiscard(lambda::bind(
        &LibeventSSLSocketImpl::discardSend,
        this,
        request));

  // Assign 'sendRequest' under lock, fail on error.
  synchronized (this) {
    if (sendRequest != NULL) {
      delete request;
      return Failure("Socket is already sending");
    } else {
      sendRequest = request;
    }
  }

  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_send,
      this,
      data,
      size));

  return future;
}


// This function runs in the event loop. It is a continuation of
// 'sendfile'. See 'Continuation' note at top of file.
void LibeventSSLSocketImpl::_sendfile(
    int fd,
    off_t offset,
    size_t size)
{
  bool sendfile = false;

  synchronized (this) {
    // Only sendfile if there is an active request. If 'sendRequest'
    // is NULL then the request has been discarded.
    if (sendRequest != NULL) {
      sendfile = true;
    }
  }

  if (sendfile) {
    evbuffer_add_file(bufferevent_get_output(bev), fd, offset, size);
  }
}


Future<size_t> LibeventSSLSocketImpl::sendfile(
    int fd,
    off_t offset,
    size_t size)
{
  // Optimistically construct a 'SendRequest' and future.
  // Copy this socket into the request to keep the it alive.
  SendRequest* request = new SendRequest(size, socket());
  Future<size_t> future = request->promise.future()
    .onDiscard(lambda::bind(
        &LibeventSSLSocketImpl::discardSend,
        this,
        request));

  // Assign 'sendRequest' under lock, fail on error.
  synchronized (this) {
    if (sendRequest != NULL) {
      delete request;
      return Failure("Socket is already sending");
    } else {
      sendRequest = request;
    }
  }

  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_sendfile,
      this,
      fd,
      offset,
      size));

  return future;
}


// This function should only be run from within the event_loop. It
// factors out the common behavior of (1) setting up the SSL object,
// (2) creating a new socket and forwarding the 'AcceptRequest', (3)
// providing the 'AcceptRequest' with a copy of the new Socket, and
// (4) setting up the callbacks after the copy has been taken.
void LibeventSSLSocketImpl::doAccept(
    int sock,
    struct sockaddr* sa,
    int sa_len)
{
  AcceptRequest* request = NULL;

  // Swap the 'acceptRequest' under the object lock.
  synchronized (this) {
    std::swap(request, acceptRequest);
  }

  CHECK_NOTNULL(request);

  // (1) Set up SSL object.
  SSL* client_ssl = SSL_new(openssl::context());
  if (client_ssl == NULL) {
    request->promise.fail("Accept failed, SSL_new");
    delete request;
    return;
  }

  struct event_base* ev_base = evconnlistener_get_base(listener);

  // Construct the bufferevent in the accepting state.
  struct bufferevent* bev = bufferevent_openssl_socket_new(
      ev_base,
      sock,
      client_ssl,
      BUFFEREVENT_SSL_ACCEPTING,
      BEV_OPT_THREADSAFE);
  if (bev == NULL) {
    request->promise.fail("Accept failed: bufferevent_openssl_socket_new");
    SSL_free(client_ssl);
    delete request;
    return;
  }

  request->bev = bev;
  request->sock = sock;
  request->sa = sa;
  request->sa_len = sa_len;

  // (2) Create socket with forwarding 'acceptRequest'.
  Socket socket = Socket::Impl::socket(
      std::make_shared<LibeventSSLSocketImpl>(sock, request));

  // (3) Assign copy of socket to AcceptRequest. This makes sure that
  // there the socket is not destroyed before we finish accepting.
  request->acceptedSocket = new Socket(socket);

  // (4) Set up callbacks to allow completion of the accepted
  // eventCallback on the new socket's bufferevent.
  bufferevent_setcb(
      bev,
      LibeventSSLSocketImpl::recvCallback,
      LibeventSSLSocketImpl::sendCallback,
      LibeventSSLSocketImpl::eventCallback,
      request->self);
}

// This callback is run within the event loop.
void LibeventSSLSocketImpl::acceptCallback(
    struct evconnlistener* listener,
    int sock,
    struct sockaddr* sa,
    int sa_len,
    void* arg)
{
  LibeventSSLSocketImpl* impl =
    reinterpret_cast<LibeventSSLSocketImpl*>(CHECK_NOTNULL(arg));

  AcceptRequest* request = NULL;

  // Manually construct the lock as the macro does not work for
  // acquiring it from another instance.
  if (Synchronized __synchronizedthis =
      Synchronized(&(impl->__synchronizable_this))) {
    request = impl->acceptRequest;
  }

  // Since the bufferevent_ssl implementation uses a bufferevent
  // underneath, we can not perfectly control the listening socket.
  // This means we sometimes get called back in 'acceptCallback'
  // multiple times even though we're only interested in 1 at a time.
  // We queue the remaining accept ready sockets on the
  // 'pendingAccepts' queue.
  if (request != NULL) {
    impl->doAccept(sock, sa, sa_len);
  } else {
    impl->pendingAccepts.push(PendingAccept{sock, sa, sa_len, arg});
  }
}


Try<Nothing> LibeventSSLSocketImpl::listen(int backlog)
{
  if (listener != NULL) {
    return Error("Socket is already listening");
  }

  CHECK(bev == NULL);

  listener = evconnlistener_new(
      base,
      &LibeventSSLSocketImpl::acceptCallback,
      this,
      LEV_OPT_REUSEABLE,
      backlog,
      s);
  if (listener == NULL) {
    return Error("Failed to listen on socket");
  }

  // TODO(jmlvanre): attach an error callback.

  return Nothing();
}


// This function is run within the event loop. It is a continuation
// from accept that ensures we check the pendingAccepts queue safely.
void LibeventSSLSocketImpl::_accept()
{
  if (!pendingAccepts.empty()) {
    const PendingAccept &p = pendingAccepts.front();
    doAccept(p.sock, p.sa, p.sa_len);
    pendingAccepts.pop();
  }
}

// Since we queue accepts in the 'pendingAccepts' queue, there is the
// ability for the queue to grow if the serving loop (the loop that
// calls this function) can not keep up. This is different from the
// usual scenario because the sockets will be queued up in user space
// as opposed to in the kernel. This will look as if requests are
// being served slowly as opposed to kernel stats showing accept
// queue overflows.
Future<Socket> LibeventSSLSocketImpl::accept()
{
  if (listener == NULL) {
    return Failure("Socket must be listening in order to accept");
  }

  // Optimistically construct an 'AcceptRequest' and future.
  AcceptRequest* request = new AcceptRequest(socket());
  Future<Socket> future = request->promise.future();

  // Assign 'acceptRequest' under lock, fail on error.
  synchronized (this) {
    if (acceptRequest != NULL) {
      delete request;
      return Failure("Socket is already Accepting");
    } else {
      acceptRequest = request;
    }
  }

  // Copy this socket into 'run_in_event_loop' to keep the it alive.
  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_accept,
      this));

  return future;
}

} // namespace network {
} // namespace process {
