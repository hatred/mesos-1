/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <list>

#include <process/async.hpp>
#include <process/check.hpp>
#include <process/collect.hpp>
#include <process/dispatch.hpp>

#include <stout/net.hpp>
#include <stout/path.hpp>

#include "hdfs/hdfs.hpp"

#include "slave/slave.hpp"

#include "slave/containerizer/fetcher.hpp"

using std::list;
using std::map;
using std::string;
using std::vector;

using memory::shared_ptr;

using process::Future;

namespace mesos {
namespace internal {
namespace slave {

static const string FILE_URI_PREFIX = "file://";
static const string FILE_URI_LOCALHOST = "file://localhost";

static const string CACHE_FILE_NAME_PREFIX = "c";


Fetcher::Fetcher() : process(new FetcherProcess())
{
  spawn(process.get());
}


Fetcher::Fetcher(const process::Owned<FetcherProcess>& process)
  : process(process)
{
  spawn(process.get());
}


Fetcher::~Fetcher()
{
  terminate(process.get());
  process::wait(process.get());
}


Try<Nothing> Fetcher::recoverCache(const SlaveID& slaveId, const Flags& flags)
{
  // Good enough for now, simple, least-effort recovery.
  VLOG(1) << "Clearing fetcher cache";

  string cacheDirectory = paths::getSlavePath(flags.fetcher_cache_dir, slaveId);
  Result<string> path = os::realpath(cacheDirectory);
  if (path.isError()) {
    LOG(ERROR) << "Malformed fetcher cache directory path '" << cacheDirectory
               << "', error: " + path.error();

    return Error(path.error());
  }

  if (path.isSome() && os::exists(path.get())) {
    Try<Nothing> rmdir = os::rmdir(path.get(), true);
    if (rmdir.isError()) {
      LOG(ERROR) << "Could not delete fetcher cache directory '"
                 << cacheDirectory << "', error: " + rmdir.error();

      return rmdir;
    }
  }

  return Nothing();
}


Try<string> Fetcher::basename(const string& uri)
{
  // TODO(bernd-mesos): full URI parsing, then move this to stout.
  // There is a bug (or is it a feature?) in the original fetcher
  // code without caching that remains in effect here. URIs are
  // treated like file paths, looking for occurrences of "/",
  // but ignoring other separators that can show up
  // (e.g. "?", "=" in HTTP URLs).

  if (uri.find_first_of('\\') != string::npos ||
      uri.find_first_of('\'') != string::npos ||
      uri.find_first_of('\0') != string::npos) {
      return Error("Illegal characters in URI");
  }

  size_t index = uri.find("://");
  if (index != string::npos && 1 < index) {
    // URI starts with protocol specifier, e.g., http://, https://,
    // ftp://, ftps://, hdfs://, hftp://, s3://, s3n://.

    string path = uri.substr(index + 3);
    if (!strings::contains(path, "/") || path.size() <= path.find("/") + 1) {
      return Error("Malformed URI (missing path): " + uri);
    }

    return path.substr(path.find_last_of("/") + 1);
  }
  return os::basename(uri);
}


Try<Nothing> Fetcher::validateUri(const string& uri)
{
  Try<string> result = basename(uri);
  if (result.isError()) {
    return Error(result.error());
  }

  return Nothing();
}


static Try<Nothing> validateUris(const CommandInfo& commandInfo)
{
  foreach (const CommandInfo::URI& uri, commandInfo.uris()) {
    Try<Nothing> validation = Fetcher::validateUri(uri.value());
    if (validation.isError()) {
      return Error(validation.error());
    }
  }

  return Nothing();
}


Result<string> Fetcher::uriToLocalPath(
    const string& uri,
    const string& frameworksHome)
{
  if (!strings::startsWith(uri, "file://") && strings::contains(uri, "://")) {
    return None();
  }

  string path = uri;
  bool fileUri = false;

  if (strings::startsWith(path, FILE_URI_LOCALHOST)) {
    path = path.substr(FILE_URI_LOCALHOST.size());
    fileUri = true;
  } else if (strings::startsWith(path, FILE_URI_PREFIX)) {
    path = path.substr(FILE_URI_PREFIX.size());
    fileUri = true;
  }

  if (fileUri && !strings::startsWith(path, "/")) {
    return Error("File URI only supports absolute paths");
  }

  if (path.find_first_of("/") != 0) {
    if (frameworksHome.empty()) {
      return Error("A relative path was passed for the resource but the "
                   "Mesos framework home was not specified. "
                   "Please either provide this config option "
                   "or avoid using a relative path");
    } else {
      path = path::join(frameworksHome, path);
      LOG(INFO) << "Prepended Mesos frameworks home to relative path, "
                << "making it: '" << path << "'";
    }
  }

  return path;
}


bool Fetcher::isNetUri(const std::string& uri)
{
  return strings::startsWith(uri, "http://")  ||
         strings::startsWith(uri, "https://") ||
         strings::startsWith(uri, "ftp://")   ||
         strings::startsWith(uri, "ftps://");
}


Future<Nothing> Fetcher::fetch(
    const ContainerID& containerId,
    const CommandInfo& commandInfo,
    const string& sandboxDirectory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const Flags& flags)
{
  if (commandInfo.uris().size() == 0) {
    return Nothing();
  }

  return dispatch(process.get(),
                  &FetcherProcess::fetch,
                  containerId,
                  commandInfo,
                  sandboxDirectory,
                  user,
                  slaveId,
                  flags);
}


void Fetcher::kill(const ContainerID& containerId)
{
  dispatch(process.get(), &FetcherProcess::kill, containerId);
}


FetcherProcess::~FetcherProcess()
{
  foreach (const ContainerID& containerId, subprocessPids.keys()) {
    kill(containerId);
  }
}


// Find out how large a potential HDFS download from the given URI is.
static Try<Bytes> fetchSizeWithHDFS(const string& uri)
{
  HDFS hdfs;

  Try<bool> available = hdfs.available();
  if (available.isError() || !available.get()) {
    return Error("Hadoop Client not available: " + available.error());
  }

  return hdfs.du(uri);
}


// Find out how large a potential download from the given URI is.
static Try<Bytes> fetchSize(
    const string& uri,
    const string& frameworksHome = "")
{
  VLOG(1) << "Fetching size for URI: " << uri;

  Result<string> path = Fetcher::uriToLocalPath(uri, frameworksHome);
  if (path.isError()) {
    return Error(path.error());
  }
  if (path.isSome()) {
    Try<Bytes> size = os::stat::size(path.get(), true);
    if (size.isError()) {
      return Error("Could not determine file size for: '" + path.get() +
                   "', error: " + size.error());
    }
    return size;
  }

  if (Fetcher::isNetUri(uri)) {
    Try<Bytes> size = net::contentLength(uri);
    if (size.isError()) {
      return Error(size.error());
    }
    if (size.get() == 0) {
      return Error("URI reported content-length 0: " + uri);
    }

    return size.get();
  }

  return fetchSizeWithHDFS(uri);
}


static FetcherInfo::Item makeItem(
    const CommandInfo::URI& uri,
    const FetcherInfo::Item::Action& action,
    const Option<string>& filename = None())
{
  FetcherInfo::Item item;

  item.mutable_uri()->CopyFrom(uri);
  item.set_action(action);

  if (filename.isSome()) {
    item.set_cache_filename(filename.get());
  }

  return item;
}


static FetcherInfo::Item bypassCache(const CommandInfo::URI& uri)
{
  VLOG(1) << "Bypassing cache for URI: " << uri.value();

  return makeItem(uri, FetcherInfo::Item::BYPASS_CACHE);
}


static Future<FetcherInfo::Item> retrieveFromCache(
    const CommandInfo::URI& uri,
    const shared_ptr<FetcherProcess::Cache::Entry>& entry)
{
  VLOG(1) << "Retrieving URI from cache: " << uri.value();

  return entry->future()
    .then(lambda::bind(&makeItem,
                       uri,
                       FetcherInfo::Item::RETRIEVE_FROM_CACHE,
                       entry->filename));
}


// Converts a "Try" result from async() to a future so that ".then()"
// can work as needed. This is necessary because  "async(...).then(...)"
// always ends up executing the ".then()" clause, unconditionally. Using
// this adapter in the following way propagates error handling in the
// expected manner common to ".then()" chains:
// "async(...).then(lambda::bind(tryToFuture, lambda::_1)".
template <typename T>
static Future<T> tryToFuture(Try<T> value)
{
  if (value.isError()) {
    return Failure(value.error());
  }

  return value.get();
}


Future<FetcherInfo::Item> FetcherProcess::downloadAndCache(
    const CommandInfo::URI& uri,
    const shared_ptr<Cache::Entry>& entry,
    const string& cacheDirectory,
    const Flags& flags)
{
  VLOG(1) << "Downloading and caching URI: " << uri.value();

  return async(&fetchSize, uri.value(), flags.frameworks_home)
    .then(lambda::bind(&tryToFuture<Bytes>, lambda::_1))
    .then(defer(self(),
                &FetcherProcess::reserveCacheSpace,
                lambda::_1,
                entry,
                cacheDirectory))
    .then(lambda::bind(&makeItem,
                       uri,
                       FetcherInfo::Item::DOWNLOAD_AND_CACHE,
                       entry->filename));
}


list<Future<FetcherInfo::Item>> FetcherProcess::makeCacheItems(
    const list<CommandInfo::URI>& uris,
    shared_ptr<list<shared_ptr<Cache::Entry>>> referencedEntries,
    shared_ptr<list<shared_ptr<Cache::Entry>>> newEntries,
    const string& cacheDirectory,
    const Option<string>& user,
    const Flags& flags)
{
  list<Future<FetcherInfo::Item>> result;

  foreach (const CommandInfo::URI& uri, uris) {
    Option<shared_ptr<Cache::Entry>> entry = cache.getEntry(user, uri.value());

    if (entry.isSome()) {
      entry.get()->reference();
      referencedEntries->push_back(entry.get());

      result.push_back(retrieveFromCache(uri, entry.get()));
    } else {
      shared_ptr<Cache::Entry> newEntry =
        cache.createEntry(cacheDirectory, user, uri);

      newEntry->reference();
      referencedEntries->push_back(newEntry);
      newEntries->push_back(newEntry);

      result.push_back(downloadAndCache(uri, newEntry, cacheDirectory, flags));
    }
  }

  // Barrier for mock testing.
  contentionBarrier();

  return result;
}


// This is for testing only.
void FetcherProcess::contentionBarrier() {
}


// Unwraps the given item futures and replaces items that failed fetch
// preparation with fallback items that cause cache bypassing.
static Future<list<FetcherInfo::Item>> applyFallbacks(
    const list<Future<FetcherInfo::Item>>& items,
    const list<CommandInfo::URI>& uris,
    const Option<std::string>& user)
{
  list<FetcherInfo::Item> result;

  CHECK_EQ(items.size(), uris.size());
  list<CommandInfo::URI>::const_iterator uri = uris.begin();

  foreach (const Future<FetcherInfo::Item>& item, items) {
    if (item.isReady()) {
      result.push_back(item.get());
    } else {
      CHECK(item.isFailed());

      LOG(WARNING) << "Reverting to fetching directly into the sandbox for '"
                   << uri->value()
                   << "', due to failure to fetch through the cache, "
                   << "with error: " << item.failure();

      result.push_back(bypassCache(*uri));
    }
    uri++;
  }

  return result;
}


// Helper to unreference all cache entries touched during an
// invocation of FetcherProcess::fetch.
//
// TODO(benh): Replace this with a C++11 lambda once it's introduced.
static void unreference(
    const Future<Nothing>&, // Necessary because 'defer' is insufficient.
    const shared_ptr<list<shared_ptr<FetcherProcess::Cache::Entry>>>& entries)
{
  foreach (const shared_ptr<FetcherProcess::Cache::Entry>& entry, *entries) {
    entry->unreference();
  }
}


// TODO(bernd-mesos): Make this a local closure in C++11.
static void logFailure(const string& message)
{
  LOG(ERROR) << message;
}


Future<Nothing> FetcherProcess::fetch(
    const ContainerID& containerId,
    const CommandInfo& commandInfo,
    const string& sandboxDirectory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const Flags& flags)
{
  VLOG(1) << "Starting to fetch URIs for container: " << containerId
          << ", directory: " << sandboxDirectory;

  // TODO(bernd-mesos): This will disappear once we inject flags at
  // Fetcher/FetcherProcess creation time. For now we trust this is
  // always the exact same value.
  cache.setSpace(flags.fetcher_cache_size);

  Try<Nothing> validated = validateUris(commandInfo);
  if (validated.isError()) {
    return Failure("Could not fetch: " + validated.error());
  }

  Option<string> commandUser = user;
  if (commandInfo.has_user()) {
    commandUser = commandInfo.user();
  }

  string cacheDirectory = paths::getSlavePath(flags.fetcher_cache_dir, slaveId);
  if (commandUser.isSome()) {
    // Segregating per-user cache directories.
    cacheDirectory = path::join(cacheDirectory, commandUser.get());
  }

  if (commandUser.isSome()) {
    // First assure that we are working for a valid user.
    // TODO(bernd-mesos): This should be asynchronous.
    Try<Nothing> chown = os::chown(commandUser.get(), sandboxDirectory);
    if (chown.isError()) {
      return Failure("Failed to chown directory: " + sandboxDirectory +
                     " to user: " + commandUser.get() +
                     " with error: " + chown.error());
    }
  }

  list<FetcherInfo::Item> nonCacheItems;
  list<CommandInfo::URI> cacheUris;

  foreach (const CommandInfo::URI& uri, commandInfo.uris()) {
    if (uri.cache()) {
      cacheUris.push_back(uri);
    } else {
      nonCacheItems.push_back(bypassCache(uri));
    }
  }

  auto referencedEntries = shared_ptr<list<shared_ptr<Cache::Entry>>>(
      new list<shared_ptr<Cache::Entry>>());

  auto newEntries = shared_ptr<list<shared_ptr<Cache::Entry>>>(
      new list<shared_ptr<Cache::Entry>>());

  list<Future<FetcherInfo::Item>> cacheItems = makeCacheItems(
      cacheUris,
      referencedEntries,
      newEntries,
      cacheDirectory,
      commandUser,
      flags);

  return await(cacheItems)
    .then(lambda::bind(&applyFallbacks, cacheItems, cacheUris, commandUser))
    .then(defer(self(),
                &FetcherProcess::_fetch,
                containerId,
                nonCacheItems,
                lambda::_1,
                sandboxDirectory,
                cacheDirectory,
                commandUser,
                flags))
    .onFailed(lambda::bind(logFailure, lambda::_1))
    .onAny(defer(self(),
      lambda::function<void(const Future<Nothing>&)>(
          lambda::bind(&unreference, lambda::_1, referencedEntries))))
    .onAny(defer(self(),
                 &FetcherProcess::cleanupNewCacheEntries,
                 newEntries));
}


Try<Nothing> FetcherProcess::deleteCacheEntry(
    const shared_ptr<Cache::Entry>& entry)
{
  VLOG(1) << "Deleting cache entry: " << entry->key;

  cache.removeEntry(entry);

  if (entry->future().isReady()) {
    Try<Nothing> rm = os::rm(entry->path().value);
    if (rm.isError()) {
      return Error("Could not delete fetcher cache file '" +
                   entry->path().value + "' with error: " + rm.error() +
                   " for entry '" + entry->key +
                   "', leaking cache space: " + stringify(entry->size));
    }
  }

  if (entry->size > 0) {
    cache.releaseSpace(entry->size);
  }

  return Nothing();
}


Future<Nothing> FetcherProcess::_fetch(
    const ContainerID& containerId,
    const list<FetcherInfo::Item>& nonCacheItems,
    const Future<list<FetcherInfo::Item>>& cacheItems,
    const string& sandboxDirectory,
    const string& cacheDirectory,
    const Option<string>& user,
    const Flags& flags)
{
  FetcherInfo fetcherInfo;

  fetcherInfo.set_sandbox_directory(sandboxDirectory);
  fetcherInfo.set_cache_directory(cacheDirectory);

  CHECK(cacheItems.isReady());
  foreach (const FetcherInfo::Item& item, cacheItems.get()) {
    fetcherInfo.add_items()->CopyFrom(item);
  }

  foreach (const FetcherInfo::Item& item, nonCacheItems) {
    fetcherInfo.add_items()->CopyFrom(item);
  }

  if (user.isSome()) {
    fetcherInfo.set_user(user.get());
  }

  if (!flags.frameworks_home.empty()) {
    fetcherInfo.set_frameworks_home(flags.frameworks_home);
  }

  Try<Subprocess> subprocess = run(fetcherInfo, flags);
  if (subprocess.isError()) {
    return Failure("Failed to execute mesos-fetcher: " + subprocess.error());
  }

  subprocessPids[containerId] = subprocess.get().pid();

  return subprocess.get().status()
    .then(defer(self(),
                &Self::__fetch,
                lambda::_1,
                containerId,
                fetcherInfo,
                user));
}


Future<Nothing> FetcherProcess::__fetch(
    const Option<int>& status,
    const ContainerID& containerId,
    const FetcherInfo& fetcherInfo,
    const Option<string>& user)
{
  subprocessPids.erase(containerId);

  if (status.isNone()) {
    return Failure("No status available from mesos-fetcher");
  }

  if (status.get() != 0) {
    return Failure("Failed to fetch all URIs for container '" +
                   stringify(containerId) + "' with exit status: " +
                   stringify(status.get()));
  }

  foreach (const FetcherInfo::Item& item, fetcherInfo.items()) {
    if (item.action() == FetcherInfo::Item::DOWNLOAD_AND_CACHE) {
      Option<shared_ptr<Cache::Entry>> entry =
        cache.getEntry(user, item.uri().value());
      CHECK_SOME(entry);

      VLOG(1) << "Downloaded URI to cache: " << item.uri().value();

      entry.get()->complete();
      adjustCacheSpace(entry.get());
    }
  }

  return Nothing();
}


static off_t delta(
    const Bytes& actualSize,
    const shared_ptr<FetcherProcess::Cache::Entry>& entry)
{
  CHECK(entry->future().isReady());

  if (actualSize < entry->size) {
    Bytes delta = entry->size - actualSize;
    LOG(WARNING) << "URI download result for '" << entry->key
                 << "' is smaller than expected by " << stringify(delta)
                 << " at: " << entry->path();
    entry->size = actualSize;

    return -off_t(delta.bytes());
  } else if (actualSize > entry->size) {
    Bytes delta = actualSize - entry->size;
    LOG(WARNING) << "URI download result for '" << entry->key
                 << "' is larger than expected by " << stringify(delta)
                 << " at: " << entry->path();
    entry->size = actualSize;

    return off_t(delta.bytes());
  }

  return 0;
}


void FetcherProcess::adjustCacheSpace(
    const shared_ptr<FetcherProcess::Cache::Entry>& entry)
{
  Try<Bytes> size = os::stat::size(entry.get()->path().value, false);
  if (size.isSome()) {
    off_t d = delta(size.get(), entry);
    if (d < 0) {
      cache.releaseSpace(Bytes(d));
    } else {
      // This could be bad. See claimSpace() for further details.
      cache.claimSpace(Bytes(d));
    }
  } else {
    // This should never be caused by Mesos itself, but cannot be excluded.
    LOG(ERROR) << "Fetcher cache file for '" << entry->key
               << "' disappeared from :" << entry->path();

    // Best effort to recover from this.
    deleteCacheEntry(entry);
  }
}


void FetcherProcess::cleanupNewCacheEntries(
    const shared_ptr<list<shared_ptr<Cache::Entry>>>& newEntries)
{
  foreach (const shared_ptr<Cache::Entry>& entry, *newEntries) {
    if (entry->future().isReady()) {
      // Successful new download.
    } else {
      // We either failed to download this URI or we never tried
      // and went into a fallback, bypassing the cache. In the latter
      // case, we still have a successful fetcher run (which is why this
      // method must be invoked "onAny" and not just "onFailure"), but
      // this entry was not successfully completed.
      VLOG(1) << "Cleaning up failed fetch for: " << entry->key;

      entry->fail();
      deleteCacheEntry(entry);
    }
  }
}


size_t FetcherProcess::countCacheFiles(
    const SlaveID& slaveId,
    const Flags& flags,
    const bool checkEntries)
{
  string cacheDirectory =
    slave::paths::getSlavePath(flags.fetcher_cache_dir, slaveId);

  Try<list<string>> cacheFiles =
    os::find(cacheDirectory, CACHE_FILE_NAME_PREFIX);

  if (cacheFiles.isSome()) {
    if (checkEntries) {
      CHECK_EQ(cache.size(), cacheFiles.get().size());
    }

    return cacheFiles.get().size();
  }

  if (checkEntries) {
    CHECK_EQ(0u, cache.size());
  }

  return 0;
}


size_t FetcherProcess::countCacheEntries(
    const SlaveID& slaveId,
    const Flags& flags)
{
  return cache.size();
}


Bytes FetcherProcess::availableCacheSpace()
{
  return cache.availableSpace();
}


// Returns quickly if there is enough space. Tries to evict cache files
// to make space if there is not enough.
Future<Nothing> FetcherProcess::reserveCacheSpace(
    const Try<Bytes>& requestedSpace,
    const shared_ptr<FetcherProcess::Cache::Entry>& entry,
    const string& cacheDirectory)
{
  if (cache.availableSpace() < requestedSpace.get()) {
    Bytes missingSpace = requestedSpace.get() - cache.availableSpace();

    VLOG(1) << "Freeing up " << missingSpace << " fetcher cache space for: "
            << entry->key;

    const Try<list<shared_ptr<Cache::Entry>>>& victims =
      cache.selectVictims(missingSpace);

    if (victims.isError()) {
      return Failure("Could not free up enough fetcher cache space");
    }

    foreach (const shared_ptr<Cache::Entry>& entry, victims.get()) {
      Try<Nothing> deletion = deleteCacheEntry(entry);
      if (deletion.isError()) {
        return deletion;
      }
    }
  }

  VLOG(1) << "Claiming fetcher cache space for: " << entry->key;

  cache.claimSpace(requestedSpace.get());

  entry->size = requestedSpace.get();

  return Nothing();
}


Try<Subprocess> FetcherProcess::run(
    const FetcherInfo& fetcherInfo,
    const Flags& flags)
{
  // Before we fetch let's make sure we create 'stdout' and 'stderr'
  // files into which we can redirect the output of the mesos-fetcher
  // (and later redirect the child's stdout/stderr).

  // TODO(tillt): Considering updating fetcher::run to take paths
  // instead of file descriptors and then use Subprocess::PATH()
  // instead of Subprocess::FD(). The reason this can't easily be done
  // today is because we not only need to open the files but also
  // chown them.
  Try<int> out = os::open(
      path::join(fetcherInfo.sandbox_directory(), "stdout"),
      O_WRONLY | O_CREAT | O_TRUNC | O_NONBLOCK | O_CLOEXEC,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

  if (out.isError()) {
    return Error("Failed to create 'stdout' file: " + out.error());
  }

  // Repeat for stderr.
  Try<int> err = os::open(
      path::join(fetcherInfo.sandbox_directory(), "stderr"),
      O_WRONLY | O_CREAT | O_TRUNC | O_NONBLOCK | O_CLOEXEC,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

  if (err.isError()) {
    os::close(out.get());
    return Error("Failed to create 'stderr' file: " + err.error());
  }

  string fetcherPath = path::join(flags.launcher_dir, "mesos-fetcher");
  Result<string> realpath = os::realpath(fetcherPath);

  if (!realpath.isSome()) {
    LOG(ERROR) << "Failed to determine the canonical path "
               << "for the mesos-fetcher '"
               << fetcherPath
               << "': "
               << (realpath.isError() ? realpath.error()
                                      : "No such file or directory");

    return Error("Could not fetch URIs: failed to find mesos-fetcher");
  }

  // Now the actual mesos-fetcher command.
  string command = realpath.get();

  // We pass arguments to the fetcher program by means of an
  // environment variable.
  map<string, string> environment;

  environment["MESOS_FETCHER_INFO"] = stringify(JSON::Protobuf(fetcherInfo));

  if (!flags.hadoop_home.empty()) {
    environment["HADOOP_HOME"] = flags.hadoop_home;
  }

  VLOG(1) << "Fetching URIs using command '" << command << "'";

  Try<Subprocess> fetcherSubprocess = subprocess(
      command,
      Subprocess::PIPE(),
      Subprocess::FD(out.get()),
      Subprocess::FD(err.get()),
      environment);

  if (fetcherSubprocess.isError()) {
    return Error(
        "Failed to execute mesos-fetcher: " +  fetcherSubprocess.error());
  }

  fetcherSubprocess.get().status()
    .onAny(lambda::bind(&os::close, out.get()))
    .onAny(lambda::bind(&os::close, err.get()));
  return fetcherSubprocess;
}


void FetcherProcess::kill(const ContainerID& containerId)
{
  if (subprocessPids.contains(containerId)) {
    VLOG(1) << "Killing the fetcher for container '" << containerId << "'";
    // Best effort kill the entire fetcher tree.
    os::killtree(subprocessPids.get(containerId).get(), SIGKILL);

    subprocessPids.erase(containerId);
  }
}


string FetcherProcess::Cache::nextFilename(const CommandInfo::URI& uri){
  // Different URIs may have the same base name, so we need to
  // segregate the download results. This can be done by separate
  // directories or by different file names. We opt for the latter
  // since there may be tighter limits on how many sub-directories a
  // file system can bear than on how many files can be in a directory.

  // We put a fixed prefix upfront before the serial number so we can
  // later easily find cache files with os::find() to support testing.

  // Why we keep the file extension here: When fetching from cache, if
  // extraction is enabled, the extraction algorithm can look at the
  // extension of the cache file the same way as it would at a
  // download of the original URI, and external commands performing
  // the extraction do not get confused by their source file
  // missing an expected form of extension. This is included in the
  // following.

  // Just for human operators who want to take a look at the cache
  // and relate cache files to URIs, we also add some of the URI's
  // basename, but not too much so we do not exceed file name size
  // limits.

  Try<string> base = Fetcher::basename(uri.value());
  CHECK(base.isSome());

  string s = base.get();
  if (s.size() > 20) {
    // Grab only a prefix and a suffix, but for sure including the
    // file extension.
    s = s.substr(0, 10) + "_" + s.substr(s.size() - 10, string::npos);
  }

  ++filenameSerial;

  return CACHE_FILE_NAME_PREFIX + stringify(filenameSerial) + "-" + s;
}


static string cacheKey(const Option<string>& user, const string& uri)
{
  return user.isNone() ? uri : user.get() + "@" + uri;
}


shared_ptr<FetcherProcess::Cache::Entry> FetcherProcess::Cache::createEntry(
    const string& cacheDirectory,
    const Option<string>& user,
    const CommandInfo::URI& uri)
{
  const string& key = cacheKey(user, uri.value());
  const string& filename = nextFilename(uri);

  auto entry = shared_ptr<Cache::Entry>(
      new Cache::Entry(key, cacheDirectory, filename));

  table.put(key, entry);

  VLOG(1) << "Created cache entry '" << key << "' with file: " << filename;

  return entry;
}


// Retrieves the cache entry indexed by the parameters.
Option<shared_ptr<FetcherProcess::Cache::Entry>>
FetcherProcess::Cache::getEntry(
    const Option<std::string>& user,
    const std::string& uri)
{
  const string& key = cacheKey(user, uri);

  return table.get(key);
}


bool FetcherProcess::Cache::containsEntry(
    const Option<string>& user,
    const string& uri)
{
  return getEntry(user, uri).isSome();
}


void FetcherProcess::Cache::removeEntry(
    shared_ptr<FetcherProcess::Cache::Entry> entry)
{
  VLOG(1) << "Removing cache entry '" << entry->key
          << "' with filename: " << entry->filename;

  table.erase(entry->key);
}


Try<list<shared_ptr<FetcherProcess::Cache::Entry>>>
FetcherProcess::Cache::selectVictims(const Bytes& requiredSpace)
{
  // TODO(bernd-mesos): Implement more elaborate selection criteria
  // (LRU/MRU, etc.).

  list<shared_ptr<FetcherProcess::Cache::Entry>> result;

  Bytes space = 0;

  foreachvalue (const shared_ptr<Cache::Entry>& entry, table) {
    if (entry->isEvictable()) {
      result.push_back(entry);

      space += entry->size;
      if (space >= requiredSpace) {
        return result;
      }
    }
  }

  return Error("Could not find enough cache files to evict");
}


size_t FetcherProcess::Cache::size()
{
  return table.size();
}


void FetcherProcess::Cache::setSpace(Bytes bytes)
{
  if (space > 0) {
    // Dynamic cache size changes not supported.
    CHECK(space == bytes);
  } else {
    space = bytes;
  }
}


void FetcherProcess::Cache::claimSpace(Bytes bytes)
{
  tally += bytes;

  if (tally > space) {
    // Used cache volume space exceeds the maximum amount set by
    // flags.fetcher_cache_size. This may be tolerated temporarily,
    // if there is sufficient physical space available. But it can
    // otherwise cause unspecified system behavior at any moment.
    LOG(WARNING) << "Fetcher cache space overflow - space used: " << tally
                 << ", exceeds total fetcher cache space: " << space;
  }

  VLOG(1) << "Claimed cache space: " << bytes << ", now using: " << tally;
}


void FetcherProcess::Cache::releaseSpace(Bytes bytes)
{
  CHECK(bytes <= tally) << "Attempt to release more cache space than in use - "
                        << " requested: " << bytes << ", in use: " << tally;


  tally -= bytes;

  VLOG(1) << "Released cache space: " << bytes << ", now using: " << tally;
}


Bytes FetcherProcess::Cache::availableSpace()
{
  if (tally > space) {
    LOG(WARNING) << "Fetcher cache space overflow - space used: " << tally
                 << ", exceeds total fetcher cache space: " << space;
    return 0;
  }

  return space - tally;
}


Future<Nothing> FetcherProcess::Cache::Entry::future()
{
  return promise.future();
}


void FetcherProcess::Cache::Entry::complete()
{
  CHECK_PENDING(promise.future());

  promise.set(Nothing());
}


void FetcherProcess::Cache::Entry::fail()
{
  CHECK_PENDING(promise.future());

  promise.fail("Could not download to fetcher cache: " + key);
}


void FetcherProcess::Cache::Entry::reference()
{
  referenceCount++;
}


void FetcherProcess::Cache::Entry::unreference()
{
  CHECK(referenceCount > 0);

  referenceCount--;
}


bool FetcherProcess::Cache::Entry::isEvictable()
{
  return referenceCount == 0 && future().isReady();
}


} // namespace slave {
} // namespace internal {
} // namespace mesos {
