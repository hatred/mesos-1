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

#include <string>

#include <mesos/mesos.hpp>

#include <mesos/fetcher/fetcher.hpp>

#include <stout/json.hpp>
#include <stout/net.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/protobuf.hpp>
#include <stout/strings.hpp>

#include "hdfs/hdfs.hpp"

#include "logging/flags.hpp"
#include "logging/logging.hpp"

#include "slave/slave.hpp"

#include "slave/containerizer/fetcher.hpp"

using namespace mesos;
using namespace mesos::internal;

using mesos::fetcher::FetcherInfo;

using mesos::internal::slave::Fetcher;

using std::string;


// Try to extract sourcePath into directory. If sourcePath is
// recognized as an archive it will be extracted and true returned;
// if not recognized then false will be returned. An Error is
// returned if the extraction command fails.
static Try<bool> extract(
    const string& sourcePath,
    const string& destinationDirectory)
{
  string command;
  // Extract any .tgz, tar.gz, tar.bz2 or zip files.
  if (strings::endsWith(sourcePath, ".tgz") ||
      strings::endsWith(sourcePath, ".tar.gz") ||
      strings::endsWith(sourcePath, ".tbz2") ||
      strings::endsWith(sourcePath, ".tar.bz2") ||
      strings::endsWith(sourcePath, ".txz") ||
      strings::endsWith(sourcePath, ".tar.xz")) {
    command = "tar -C '" + destinationDirectory + "' -xf";
  } else if (strings::endsWith(sourcePath, ".zip")) {
    command = "unzip -d '" + destinationDirectory + "'";
  } else {
    return false;
  }

  command += " '" + sourcePath + "'";

  LOG(INFO) << "Extracting with command: " << command;

  int status = os::system(command);
  if (status != 0) {
    return Error("Failed to extract: command " + command +
                 " exited with status: " + stringify(status));
  }

  LOG(INFO) << "Extracted '" << sourcePath << "' into '"
            << destinationDirectory << "'";

  return true;
}


// Attempt to get the uri using the hadoop client.
static Try<string> downloadWithHadoopClient(
    const string& sourceUri,
    const string& destinationPath)
{
  HDFS hdfs;
  Try<bool> available = hdfs.available();

  if (available.isError() || !available.get()) {
    return Error("Skipping fetch with Hadoop Client as"
                 " Hadoop Client not available: " + available.error());
  }

  LOG(INFO) << "Downloading resource with Hadoop client from '" << sourceUri
            << "' to '" << destinationPath << "'";

  Try<Nothing> result = hdfs.copyToLocal(sourceUri, destinationPath);
  if (result.isError()) {
    return Error("HDFS copyToLocal failed: " + result.error());
  }

  return destinationPath;
}


static Try<string> downloadWithNet(
    const string& sourceUri,
    const string& destinationPath)
{
  LOG(INFO) <<  "Downloading resource from '" << sourceUri
            << "' to '" << destinationPath << "'";

  Try<int> code = net::download(sourceUri, destinationPath);
  if (code.isError()) {
    return Error("Error downloading resource: " + code.error());
  } else if (code.get() != 200) {
    return Error("Error downloading resource, received HTTP/FTP return code " +
                 stringify(code.get()));
  }

  return destinationPath;
}


static Try<string> copyFile(
    const string& sourcePath,
    const string& destinationPath)
{
  const string& command = "cp '" + sourcePath + "' '" + destinationPath + "'";

  LOG(INFO) << "Copying resource with command:" << command;

  int status = os::system(command);
  if (status != 0) {
    return Error("Failed to copy with command '" + command +
                 "', exit status: " + stringify(status));
  }

  return destinationPath;
}


static Try<string> download(
    const string& sourceUri,
    const string& destinationPath,
    const FetcherInfo& fetcherInfo)
{
  LOG(INFO) << "Fetching URI '" << sourceUri << "'";
  Try<Nothing> validation = Fetcher::validateUri(sourceUri);
  if (validation.isError()) {
    return Error(validation.error());
  }

  // 1. Try to fetch using a local copy.
  // We regard as local: "file://" or the absense of any URI scheme.
  Result<string> sourcePath =
    Fetcher::uriToLocalPath(sourceUri, fetcherInfo.frameworks_home());

  if (sourcePath.isError()) {
    return Error(sourcePath.error());
  } else if (sourcePath.isSome()) {
    return copyFile(sourcePath.get(), destinationPath);
  }

  // 2. Try to fetch URI using os::net / libcurl implementation.
  // We consider http, https, ftp, ftps compatible with libcurl.
  if (Fetcher::isNetUri(sourceUri)) {
     return downloadWithNet(sourceUri, destinationPath);
  }

  // 3. Try to fetch the URI using hadoop client.
  // We use the hadoop client to fetch any URIs that are not
  // handled by other fetchers(local / os::net). These URIs may be
  // `hdfs://` URIs or any other URI that has been configured (and
  // hence handled) in the hadoop client. This allows mesos to
  // externalize the handling of previously unknown resource
  // endpoints without the need to support them natively.
  // Note: Hadoop Client is not a hard dependency for running mesos.
  // This allows users to get mesos up and running without a
  // hadoop_home or the hadoop client setup but in case we reach
  // this part and don't have one configured, the fetch would fail
  // and log an appropriate error.
  return downloadWithHadoopClient(sourceUri, destinationPath);
}


// TODO(bernd-mesos): Refactor this into stout so that we can more easily
// chmod an exectuable. For example, we could define some static flags
// so that someone can do: os::chmod(path, EXECUTABLE_CHMOD_FLAGS).
static Try<string> chmodExecutable(const string& filePath)
{
  Try<Nothing> chmod = os::chmod(
      filePath, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
  if (chmod.isError()) {
    return Error("Failed to chmod executable '" +
                 filePath + "': " + chmod.error());
  }

  return filePath;
}


// Returns the resulting file or in case of extraction the destination
// directory (for logging).
static Try<string> fetchBypassingCache(
    const CommandInfo::URI& uri,
    const FetcherInfo& fetcherInfo)
{
  LOG(INFO) << "Fetching directly into the sandbox directory";

  Try<string> basename = Fetcher::basename(uri.value());
  if (basename.isError()) {
    return Error("Failed to determine the basename of the URI '" +
                 uri.value() + "' with error: " + basename.error());
  }

  string path = path::join(fetcherInfo.sandbox_directory(), basename.get());

  Try<string> downloaded = download(uri.value(), path, fetcherInfo);
  if (downloaded.isError()) {
    return Error(downloaded.error());
  }

  if (uri.executable()) {
    return chmodExecutable(downloaded.get());
  } else if (uri.extract()) {
    Try<bool> extracted =
      extract(path, fetcherInfo.sandbox_directory());
    if (extracted.isError()) {
      return Error(extracted.error());
    } else {
      LOG(WARNING) << "Copying instead of extracting resource from URI with "
                   << "'extract' flag, because it does not seem to be an "
                   << "archive: " << uri.value();
    }
  }

  return downloaded;
}


// Returns the resulting file or in case of extraction the destination
// directory (for logging).
static Try<string> fetchFromCache(
    const FetcherInfo::Item& item,
    const string& cacheDirectory,
    const string& sandboxDirectory)
{
  LOG(INFO) << "Fetching from cache";

  Try<string> basename = Fetcher::basename(item.uri().value());
  if (basename.isError()) {
    return Error(basename.error());
  }

  string destinationPath = path::join(sandboxDirectory, basename.get());

  string sourcePath = path::join(cacheDirectory, item.cache_filename());

  if (item.uri().executable()) {
    Try<string> copied = copyFile(sourcePath, destinationPath);
    if (copied.isError()) {
      return Error(copied.error());
    }

    return chmodExecutable(copied.get());
  } else if (item.uri().extract()) {
    Try<bool> extracted = extract(sourcePath, sandboxDirectory);
    if (extracted.isError()) {
      return Error(extracted.error());
    } else if (extracted.get()) {
      return sandboxDirectory;
    } else {
      LOG(WARNING) << "Copying instead of extracting resource from URI with "
                   << "'extract' flag, because it does not seem to be an "
                   << "archive: " << item.uri().value();
    }
  }

  return copyFile(sourcePath, destinationPath);
}


// Returns the resulting file or in case of extraction the destination
// directory (for logging).
static Try<string> fetchThroughCache(
    const FetcherInfo::Item& item,
    const FetcherInfo& fetcherInfo)
{
  if (!item.has_cache_filename() || item.cache_filename().empty()) {
    return Error("No cache file name for: " + item.uri().value());
  }

  if (!fetcherInfo.has_cache_directory() ||
      fetcherInfo.cache_directory().empty()) {
    return Error("Cache directory not specified");
  }

  if (item.action() != FetcherInfo::Item::RETRIEVE_FROM_CACHE) {
    CHECK_EQ(FetcherInfo::Item::DOWNLOAD_AND_CACHE, item.action())
      << "Unexpected fetcher action selector";

    LOG(INFO) << "Downloading into cache";

    Try<Nothing> mkdir = os::mkdir(fetcherInfo.cache_directory());
    if (mkdir.isError()) {
      return Error("Failed to create fetcher cache directory '" +
                   fetcherInfo.cache_directory() + "': " + mkdir.error());
    }

    Try<string> downloaded = download(
        item.uri().value(),
        path::join(fetcherInfo.cache_directory(), item.cache_filename()),
        fetcherInfo);

    if (downloaded.isError()) {
      return Error(downloaded.error());
    }
  }

  return fetchFromCache(item,
                        fetcherInfo.cache_directory(),
                        fetcherInfo.sandbox_directory());
}


// Returns the resulting file or in case of extraction the destination
// directory (for logging).
static Try<string> fetch(
    const FetcherInfo::Item& item,
    const FetcherInfo& fetcherInfo)
{
  LOG(INFO) << "Fetching URI '" << item.uri().value() << "'";

  if (item.action() == FetcherInfo::Item::BYPASS_CACHE) {
    return fetchBypassingCache(item.uri(), fetcherInfo);
  }

  return fetchThroughCache(item, fetcherInfo);
}


// This "fetcher program" is invoked by the slave's fetcher actor
// (Fetcher, FetcherProcess) to "fetch" URIs into the sandbox directory
// of a given task. Its parameters are provided in the form of the env
// var MESOS_FETCHER_INFO which contains a FetcherInfo (see
// fetcher.proto) object formatted in JSON. These are set by the actor
// to indicate what set of URIs to process and how to proceed with
// each one. A URI can be downloaded directly to the task's sandbox
// directory or it can be copied to a cache first or it can be reused
// from the cache, avoiding downloading. All cache management and
// bookkeeping is centralized in the slave's fetcher actor, which can
// have multiple instances of this fetcher program running at any
// given time. Exit code: 0 if entirely successful, otherwise 1.
int main(int argc, char* argv[])
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  mesos::internal::logging::Flags flags;

  Try<Nothing> load = flags.load("MESOS_", argc, argv);

  CHECK_SOME(load) << "Could not load flags: " << load.error();

  logging::initialize(argv[0], flags, true); // Catch signals.

  CHECK(os::hasenv("MESOS_FETCHER_INFO"))
    << "Missing MESOS_FETCHER_INFO environment variable";

  string jsonFetcherInfo = os::getenv("MESOS_FETCHER_INFO");
  LOG(INFO) << "Fetcher Info: " << jsonFetcherInfo;

  Try<JSON::Object> parse = JSON::parse<JSON::Object>(jsonFetcherInfo);
  CHECK_SOME(parse) << "Failed to parse MESOS_FETCHER_INFO: " << parse.error();

  Try<FetcherInfo> fetcherInfo = ::protobuf::parse<FetcherInfo>(parse.get());
  CHECK_SOME(fetcherInfo)
    << "Failed to parse FetcherInfo: " << fetcherInfo.error();

  CHECK(!fetcherInfo.get().sandbox_directory().empty())
    << "Missing sandbox directory";

  // Fetch each URI to a local file, chmod, then chown if a user is provided.
  foreach (const FetcherInfo::Item& item, fetcherInfo.get().items()) {
    Try<string> fetched = fetch(item, fetcherInfo.get());
    if (fetched.isError()) {
      EXIT(1) << "Failed to fetch '" << item.uri().value()
              << "': " + fetched.error();
    } else {
      LOG(INFO) << "Fetched '" << item.uri().value()
                << "' to '" << fetched.get() << "'";
    }
  }

  // Recursively chown the sandbox directory if a user is provided.
  if (fetcherInfo.get().has_user()) {
    Try<Nothing> chowned = os::chown(
        fetcherInfo.get().user(),
        fetcherInfo.get().sandbox_directory());
    if (chowned.isError()) {
      EXIT(1) << "Failed to chown "
              << fetcherInfo.get().sandbox_directory()
              << ": " << chowned.error();
    }
  }

  return 0;
}
