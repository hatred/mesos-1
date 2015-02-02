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
//
// We use the BEV_OPT_THREADSAFE flag when constructing bufferevents
// so that all bufferevent specific functions run from within the
// event loop will have the lock on the bufferevent already acquired.
// This means that everywhere else we need to manually call
// 'bufferevent_lock(bev)' and 'bufferevent_unlock(bev)'. However, due
// to a deadlock scenario in libevent-openssl (v 2.0.21) we currently
// modify bufferevents using continuations in the event loop. See
// 'Continuation' comment below.

// Continuation:
//
// There is a deadlock scenario in libevent-openssl (v 2.0.21) when
// modifying the bufferevent (bev) from another thread (not the event
// loop). To avoid this we run all bufferevent manipulation logic in
// continuations that are executed within the event loop.

// Connecting Extra File Descriptor:
//
// In libevent-openssl (v 2.0.21) we've had issues using the
// 'bufferevent_openssl_socket_new' call with the CONNECTING state and
// an existing socket. Therefore we allow it to construct its own file
// descriptor and clean it up along with the Impl object when the
// 'bev' is freed using the BEV_OPT_CLOSE_ON_FREE option.

// Socket KeepAlive:
//
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

  virtual ~LibeventSSLSocketImpl();

  // Socket::Impl implementation.
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
    RecvRequest(char* _data, size_t _size) : data(_data), size(_size) {}
    Promise<size_t> promise;
    char* data;
    size_t size;
  };

  struct SendRequest
  {
    SendRequest(size_t _size) : size(_size) {}
    Promise<size_t> promise;
    size_t size;
  };

  struct ConnectRequest
  {
    ConnectRequest() {}
    Promise<Nothing> promise;
  };

  struct AcceptRequest
  {
    AcceptRequest(Socket&& _acceptingSocket)
      : acceptingSocket(std::move(_acceptingSocket)),
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

  // Continuations.
  void _shutdown();
  void _recv(size_t size);
  void _send(const char* data, size_t size);
  void _sendfile(int fd, off_t offset, size_t size);
  void _accept();

  // Callbacks set with libevent.
  static void recvCallback(struct bufferevent* bev, void* arg);
  static void sendCallback(struct bufferevent* bev, void* arg);
  static void eventCallback(struct bufferevent* bev, short events, void* arg);
  static void acceptCallback(
    struct evconnlistener* listener,
    int sock,
    struct sockaddr* sa,
    int sa_len,
    void* arg);

  void discardRecv(RecvRequest* request);
  void _discardRecv(RecvRequest* request);

  void discardSend(SendRequest* request);
  void _discardSend(SendRequest* request);

  void discardConnect(ConnectRequest* request);
  void _discardConnect(ConnectRequest* request);

  void doAccept(
    int sock,
    struct sockaddr* sa,
    int sa_len);

  struct bufferevent* bev;

  struct evconnlistener* listener;

  // Protects the following instance variables.
  synchronizable(this);
  RecvRequest* recvRequest;
  SendRequest* sendRequest;
  ConnectRequest* connectRequest;
  AcceptRequest* acceptRequest;

  struct PendingAccept
  {
    int sock;
    struct sockaddr* sa;
    int sa_len;
    void* arg;
  };

  // This queue stores buffered accepted sockets. It should only be
  // accessed from within the event loop.
  queue<PendingAccept> pendingAccepts;

  // Whether or not this socket came from an 'accept' and we need to
  // explicitly free the SSL context.
  bool freeSSLCtx;

  // Hostname of the peer that we connected to or we accepted.
  Option<string> peerHostname;
};


Try<std::shared_ptr<Socket::Impl>> libeventSSLSocket(int s)
{
  openssl::initialize();
  return std::make_shared<LibeventSSLSocketImpl>(s);
}


template <typename Request, typename Value>
static void satisfy(std::shared_ptr<Request> request, const Value& value)
{
  request->promise.set(value);

  // Since we swapped 'request' with an empty std::shared_ptr we
  // should not have raced with the 'onDiscard' callback we set up
  // (see ...) since the 'onDiscard' callback should have decided that
  // we're no longer servicing this request and thus this future
  // should be ready.
  CHECK_READY(request->promise.future());
}


template <typename Request>
static void fail(std::shared_ptr<Request> request, const string& message)
{
  request->promise.fail(message);

  // Since we swapped 'request' with an empty std::shared_ptr we
  // should not have raced with the 'onDiscard' callback we set up
  // (see ...) since the 'onDiscard' callback should have decided that
  // we're no longer servicing this request and thus this future
  // should be failed.
  CHECK_FAILED(request->promise.future());
}


// This callback is run within the event loop. No locks required. See
// 'Locking' note at top of file.
void readCallback(struct bufferevent* bev, void* arg)
{
  CHECK(__in_event_loop__);

  std::weak_ptr<LibeventSSLSocketImpl>* weak =
    reinterpret_cast<std::weak_ptr<LibeventSSLSocketImpl>*>(CHECK_NOTNULL(arg));

  std::shared_ptr<LibeventSSLSocketImpl> impl(weak->lock());

  if (impl) {
    impl->readCallback();
  }
}


void LibeventSSLSocketImpl::readCallback()
{
  CHECK(__in_event_loop__);

  std::shared_ptr<RecvRequest> request(NULL);

  // TODO(benh): Optimize with 'std::shared_ptr::atomic_exchange'.
  synchronized (this) {
    request.swap(recvRequest);
  }

  if (request) {
    bufferevent_disable(bev, EV_READ);

    size_t size = bufferevent_read(bev, request->data, request->size);

    // TODO(benh): With a low-water mark at 0 is it possible that
    // we'll read nothing from the buffer? For example, if there is a
    // "spurious" read will that cause us to invoke this callback but
    // not actually have any data? Also, what if bufferevent_read
    // actually fails and returns -1!?
    CHECK(size != 0);

    request->promise.set(value);

    // Since we swapped 'request' with an empty std::shared_ptr we
    // should not have raced with the 'onDiscard' callback we set up
    // in LibeventSSLSocketImpl::recv since the 'onDiscard' callback
    // should have decided that we're no longer servicing this
    // request. Thus this future should be ready.
    CHECK_READY(request->promise.future());

    // TODO(benh): Is it possible that there is more data leftover in
    // the bufferevent? Could this happen for example because a 'recv'
    // was discarded but not before libevent went and pulled that data
    // into the buffer?
  }
}


// This callback is run within the event loop. No locks required. See
// 'Locking' note at top of file.
void writeCallback(struct bufferevent* bev, void* arg)
{
  CHECK(__in_event_loop__);

  std::weak_ptr<LibeventSSLSocketImpl>* weak =
    reinterpret_cast<std::weak_ptr<LibeventSSLSocketImpl>*>(CHECK_NOTNULL(arg));

  std::shared_ptr<LibeventSSLSocketImpl> impl(weak->lock());

  if (impl) {
    impl->writeCallback();
  }
}


void LibeventSSLSocketImpl::writeCallback()
{
  CHECK(__in_event_loop__);

  std::shared_ptr<SendRequest> request(NULL);

  // TODO(benh): Optimize with 'std::shared_ptr::atomic_exchange'.
  synchronized (this) {
    request.swap(sendRequest);
  }

  CHECK(request)
    << "Not expecting a write callback without doing 'send' or 'sendfile'";

  request->promise.set(request->size);
}


// This callback is run within the event loop. No locks required. See
// 'Locking' note at top of file.
void eventCallback(
    struct bufferevent* bev,
    short events,
    void* arg)
{
  CHECK(__in_event_loop__);

  std::weak_ptr<LibeventSSLSocketImpl>* weak =
    reinterpret_cast<std::weak_ptr<LibeventSSLSocketImpl>*>(CHECK_NOTNULL(arg));

  std::shared_ptr<LibeventSSLSocketImpl> impl(weak->lock());

  if (impl) {
    impl->eventCallback(events);
  }
}


void LibeventSSLSocketImpl::eventCallback(short events)
{
  CHECK(__in_event_loop__);

  shared_ptr<RecvRequest> currentRecvRequest(NULL);
  shared_ptr<SendRequest> currentSendRequest(NULL);
  shared_ptr<ConnectRequest> currentConnectRequest(NULL);
  shared_ptr<AcceptRequest> currentAcceptRequest(NULL);

  // In all of the following conditions, we're interested in swapping
  // the value of the requests with null (if they are already null,
  // then there's no harm).
  if (events & BEV_EVENT_EOF ||
      events & BEV_EVENT_CONNECTED ||
      (events & BEV_EVENT_ERROR && EVUTIL_SOCKET_ERROR() != 0)) {
    synchronized (this) {
      currentRecvRequest.swap(recvRequest);
      currentSendRequest.swap(sendRequest);
      currentConnectRequest.swap(connectRequest);
      currentAcceptRequest.swap(acceptRequest);
    }
  }

  // If a request below is empty then no such request is in progress,
  // either because it was never created, it has already been
  // completed, or it has been discarded.

  if (events & BEV_EVENT_EOF) {
    // At end of file, close the connection.
    if (currentRecvRequest) {
      satisfy(currentRecvRequest, 0);
    }

    if (currentSendRequest) {
      satisfy(currentSendRequest, 0);
    }

    if (currentConnectRequest) {
      fail(currentConnectRequest, "Failed connect: connection closed");
    }

    if (currentAcceptRequest) {
      fail(currentAcceptRequest, "Failed accept: connection closed");
    }
  } else if (events & BEV_EVENT_ERROR && EVUTIL_SOCKET_ERROR() != 0) {
    // If there is an error, fail any requests and log the error message.
    const string message =
      stringify(evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));

    VLOG(1) << "Socket error: " << message;

    if (currentRecvRequest) {
      fail(currentRecvRequest, "Failed recv, connection error: " + message);
    }

    if (currentSendRequest) {
      fail(currentSendRequest, "Failed send, connection error: " + message);
    }

    if (currentConnectRequest) {
      fail(currentConnectRequest,
           "Failed connect, connection error: " + message);
    }

    if (currentAcceptRequest) {
      fail(currentAcceptRequest, "Failed accept: connection error: " + message);
    }
  } else if (events & BEV_EVENT_CONNECTED) {
    // We should not have receiving or sending requests while still
    // connecting.
    //
    // TODO(benh): But we aren't checking to make sure that is the
    // case in LibeventSSLSocketImpl::send/recv, so the following
    // checks are extremely dangerous and must be removed.
    CHECK(!currentRecvRequest);
    CHECK(!currentSendRequest);

    if (currentConnectRequest) {
      // If we're connecting, then we've succeeded. Time to do
      // post-verification.
      CHECK_NOTNULL(bev);

      // Do post-validation of connection.
      SSL* ssl = bufferevent_openssl_get_ssl(bev);

      Try<Nothing> verify = openssl::verify(ssl, peerHostname);

      if (verify.isError()) {
        VLOG(1) << "Failed connect, post verification error: "
                << verify.error();
        fail(currentConnectRequest, verify.error());
      } else {
        satisfy(currentConnectRequest, Nothing());
      }
    }

    if (currentAcceptRequest) {
      // We will receive a 'CONNECTED' state on an accepting socket
      // once the connection is established. Time to do
      // post-verification.
      SSL* ssl = bufferevent_openssl_get_ssl(bev);

      Try<Nothing> verify = openssl::verify(ssl, peerHostname);

      if (verify.isError()) {
        VLOG(1) << "Failed accept, post verification error: "
                << verify.error();

        delete currentAcceptRequest->acceptedSocket;

        fail(currentAcceptRequest, verify.error());
      } else {
        Socket* socket = currentAcceptRequest->acceptedSocket;
        satisfy(currentAcceptRequest, Socket(*socket));
        delete socket;
      }
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


LibeventSSLSocketImpl::~LibeventSSLSocketImpl()
{
  if (bev != NULL) {
    SSL* ssl = bufferevent_openssl_get_ssl(bev);
    // Workaround for SSL shutdown, see:
    // http://www.wangafu.net/~nickm/libevent-book/Ref6a_advanced_bufferevents.html // NOLINT
    SSL_set_shutdown(ssl, SSL_RECEIVED_SHUTDOWN);
    SSL_shutdown(ssl);
    bufferevent_disable(bev, EV_READ | EV_WRITE);

    // For the connecting socket BEV_OPT_CLOSE_ON_FREE will close the
    // extra file descriptor. See note below.
    bufferevent_free(bev);

    RUN_THE_BUFFEREVENT_FREE_IN_EVENT_LOOP_AND_DESTROY_WEAK_PTR_REFERENCE();

    // Since we are using a separate file descriptor for the
    // connecting socket we end up using BEV_OPT_CLOSE_ON_FREE for the
    // connecting, but not for the accepting side. Since the
    // BEV_OPT_CLOSE_ON_FREE also frees the SSL object, we need to
    // manually free it for the accepting case. See the 'Connecting
    // Extra File Descriptor' note at top of file.
    if (freeSSLCtx) {
      SSL_free(ssl);
    }
  }

  if (listener != NULL) {
    evconnlistener_free(listener);
  }
}


Future<Nothing> LibeventSSLSocketImpl::connect(const Address& address)
{
  synchronized (this) {
    if (connectRequest) {
      return Failure("Socket is already connecting");
    } else if (bev != NULL) {
      return Failure("Socket is already connected");
    } else {
      connectRequest = std::make_shared<ConnectRequest>();
    }
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
  // the existing file descriptor due to an issue in libevent-openssl.
  // See the 'Connecting Extra File Descriptor' note at top of file.
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

  // Create a reference back to ourselves as a std::weak_ptr to be
  // able to allow this socket instance to be destroyed even if we're
  // still trying to connect, recv, send, or accept. See the
  // destructor for more information about how this reference gets
  // deleted itself.
  reference = new std::weak_ptr<LibeventSSLSocketImpl>(shared());

  // Assign the callbacks for the bufferevent.
  bufferevent_setcb(
      bev,
      &readCallback,
      &writeCallback,
      &eventCallback,
      reference);

  // NOTE: It doesn't appear that we actually have a way to discard a
  // connecting socket with libevent so we don't bother setting up any
  // 'onDiscard' callbacks. See the related note in
  // LibeventSSLSocketImpl::send.

  // TODO(jmlvanre): sync on new IP address setup.
  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = PF_INET;
  addr.sin_port = htons(address.port);
  addr.sin_addr.s_addr = address.ip;

  // TODO(benh): Why doesn't this one need to be run in the event loop?
  if (bufferevent_socket_connect(
      bev,
      reinterpret_cast<struct sockaddr*>(&addr),
      sizeof(addr)) < 0) {
    return Failure("Failed to connect: bufferevent_socket_connect");
  }

  return connectRequest->promise.future(); // TODO(benh): This is not safe.
}


Future<size_t> LibeventSSLSocketImpl::recv(char* data, size_t size)
{
  synchronized (this) {
    // TODO(benh): Fail if we're connecting or not yet connected?
    if (recvRequest) {
      return Failure("Socket is already receiving");
    } else {
      recvRequest = std::make_shared<RecvRequest>(data, size);
    }
  }

  // TODO(benh): What about data that might be available in the
  // bufferevent because a previous 'recv' caused data to get copied
  // into the bufferevent but was discarded before the 'readCallback'
  // callbacks get invoked? Should we try to read that data out first
  // here?

  recv;

  // Both a 'discardRecv' and 'readCallback' get queued on event loop,
  // what happens with either ordering?
  discardRecv;
  readCallback;

  readCallback;
  discardRecv;

  // What about when another 'recv' occurs in between the
  // 'discardRecv' or 'readCallback'?
  discardRecv;
//   recv;
  readCallback;
  recv;

  readCallback;
  recv;
  discardRecv;



  // We can use 'recvRequest' outside of the lock here because no
  // callbacks should be executing that
  Future<size_t> future = recvRequest->promise.future()
    .onDiscard(lambda::bind(
                   &discardRecv,
                   std::weak_ptr<LibeventSSLSocketImpl>(shared())));


  run_in_event_loop(
      lambda::bind(&send, shared<LibeventSSLSocketImpl>(), data));

  run_in_event_loop(lambda::bind(
      &LibeventSSLSocketImpl::_recv,
      this,
      size));

  return connectRequest->promise.future(); // TODO(benh): Verify safety.
}



// This function runs in the event loop. It is a continuation of
// 'recv'. See 'Continuation' note at top of file.
void LibeventSSLSocketImpl::_recv(std::shared_ptr<LibeventSSLSocketImpl>&& impl)
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


void LibeventSSLSocketImpl::discardRecv(RecvRequest* request)
{
  run_in_event_loop(&_discardRecv,
                    );

lambda::bind(
      &LibeventSSLSocketImpl::_discardRecv,
      this,
      request));
}


// This function runs in the event loop. It is a continuation of
// 'discardRecv'. See 'Continuation' note at top of file.
void _discardRecv(
    std::weak_ptr<LibeventSSLSocketImpl> weak,
    std::shared_ptr<RecvRequest> request)
{
  std::shared_ptr<LibeventSSLSocketImpl> impl(weak.lock());

  if (!impl) {
    return;
  }

  impl->discard(request);



  bool discard = false;

  synchronized (this) {
    // Only discard if the active request matches what we're trying to
    // discard. Otherwise it has been already completed
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


Future<size_t> LibeventSSLSocketImpl::send(const char* data, size_t size)
{
  synchronized (this) {
    // TODO(benh): Fail if we're connecting or not yet connected?
    if (sendRequest) {
      return Failure("Socket is already sending");
    } else {
      sendRequest = std::make_shared<SendRequest>(size);
    }
  }

  // NOTE: We don't bother setting up 'onDiscard' because we can't
  // really "discard" a send since once we copy the data to the buffer
  // it's there until it gets written. While we could allow for
  // discarding the send between now and when we actually write the
  // data to the buffer (via bufferevent_write) the only reason why
  // we're currently doing that in the event loop is because of a bug
  // with libevent and in future versions we'll just do that here in
  // which case there will be no delay in writing to the
  // buffer. Moreover, it would be unnecessarily complex to have to
  // distinguish when the future was actually discarded in the
  // 'onDiscard' handler given that after we do bufferevent_write we'd
  // want to ignore any discards that occur. Thus, we currently don't
  // support discarding a 'send' (or 'sendfile', since it's the same
  // mechanism).

  run_in_event_loop(
      lambda::bind(&send, shared<LibeventSSLSocketImpl>(), data));

  return future;
}


// This function runs in the event loop. It is a continuation of
// 'LibeventSSLSocketImpl::send'. See 'Continuation' note at top of
// file.
void send(std::shared_ptr<LibeventSSLSocketImpl> impl, const char* data)
{
  CHECK(__in_event_loop__);

  CHECK_NOTNULL(impl->bev);

  CHECK(impl->sendRequest);

  // NOTE: We keep the write low-water mark at the default of 0 so
  // that all of the data that we put into the buffer gets written
  // before we get the write callback and can satisfy this send
  // request.

  bufferevent_write(
      impl->bev,
      data,
      impl->sendRequest->size);
}


Future<size_t> LibeventSSLSocketImpl::sendfile(
    int fd,
    off_t offset,
    size_t size)
{
  synchronized (this) {
    // TODO(benh): Fail if we're connecting or not yet connected?
    if (sendRequest) {
      return Failure("Socket is already sending");
    } else {
      sendRequest = std::make_shared<SendRequest>(size);
    }
  }

  // NOTE: See the note in LibeventSSLSocketImpl::send for why we
  // don't set an 'onDiscard' callback on the future.

  run_in_event_loop(
      lambda::bind(&sendfile, shared<LibeventSSLSocketImpl>(), fd, offset));

  return future;
}


// This function runs in the event loop. It is a continuation of
// 'LibeventSSLSocketImpl::sendfile'. See 'Continuation' note at top
// of file.
void sendfile(
    std::shared_ptr<LibeventSSLSocketImpl> impl,
    int fd,
    off_t offset)
{
  CHECK(__in_event_loop__);

  CHECK_NOTNULL(impl->bev);

  CHECK(impl->sendRequest);

  // NOTE: We keep the write low-water mark at the default of 0 so
  // that all of the data that we put into the buffer gets written
  // before we get the write callback and can satisfy this send
  // request.

  // TODO(benh): According to the documentation evbuffer_add_file
  // "owns owns the resulting file descriptor and will close it when
  // finished transferring data". Unfortunately, it's unclear what the
  // "resulting file descriptor" means so we 'dup' the descriptor just
  // in case. We should confirm these semantics.
  evbuffer_add_file(
      bufferevent_get_output(impl->bev),
      dup(fd),
      offset,
      impl->sendRequest->size);
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


// This function is run within the event loop. It is a continuation
// from 'accept' that ensures we check the 'pendingAccepts' queue
// safely.
void LibeventSSLSocketImpl::_accept()
{
  if (!pendingAccepts.empty()) {
    const PendingAccept &p = pendingAccepts.front();
    doAccept(p.sock, p.sa, p.sa_len);
    pendingAccepts.pop();
  }
}


void LibeventSSLSocketImpl::shutdown()
{
  run_in_event_loop(lambda::bind(&LibeventSSLSocketImpl::_shutdown, this));
}


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

} // namespace network {
} // namespace process {
