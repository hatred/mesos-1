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

#include <unistd.h>

#include <gmock/gmock.h>

#include <list>
#include <mutex>
#include <string>
#include <vector>

#include <mesos/executor.hpp>
#include <mesos/scheduler.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/message.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>
#include <process/queue.hpp>
#include <process/subprocess.hpp>

#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/try.hpp>

#include "common/lock.hpp"

#include "master/flags.hpp"
#include "master/master.hpp"

#include "slave/constants.hpp"
#include "slave/gc.hpp"
#include "slave/flags.hpp"
#include "slave/paths.hpp"
#include "slave/slave.hpp"

#include "slave/containerizer/fetcher.hpp"

#include "tests/containerizer.hpp"
#include "tests/flags.hpp"
#include "tests/mesos.hpp"

using mesos::internal::master::Master;

using mesos::internal::slave::Slave;
using mesos::internal::slave::Containerizer;
using mesos::internal::slave::MesosContainerizer;
using mesos::internal::slave::MesosContainerizerProcess;
using mesos::internal::slave::Fetcher;
using mesos::internal::slave::FetcherProcess;

using process::Future;
using process::HttpEvent;
using process::Owned;
using process::PID;
using process::Promise;
using process::Process;
using process::Queue;
using process::Subprocess;

using std::list;
using std::string;
using std::vector;

using testing::_;
using testing::DoAll;
using testing::DoDefault;
using testing::Eq;
using testing::Invoke;
using testing::InvokeWithoutArgs;
using testing::Return;

namespace mesos {
namespace internal {
namespace tests {

static const string ASSETS_DIRECTORY_NAME = "mesos-fetcher-test-assets";
static const string COMMAND_NAME = "mesos-fetcher-test-cmd";
static const string ARCHIVE_NAME = "mesos-fetcher-test-archive.tgz";
static const string ARCHIVED_COMMAND_NAME = "mesos-fetcher-test-acmd";

// Every task executes one of these shell scripts, which create a
// file that includes the current task name in its name. The latter
// is expected to be passed in as a script argument. The existence
// of the file with that name is then used as proof that the task
// ran successfully.
static const string COMMAND_SCRIPT = "touch " + COMMAND_NAME + "$1";
static const string ARCHIVED_COMMAND_SCRIPT =
  "touch " + ARCHIVED_COMMAND_NAME + "$1";


class FetcherCacheTest : public MesosTest
{
public:
  void setupCommandFileAsset();

protected:
  void setupArchiveAsset();

  virtual void SetUp();
  virtual void TearDown();

  // Sets up the slave and starts it. Calling this late in the test
  // instead of having it included in SetUp() gives us the opportunity
  // to manipulate values in 'flags', first.
  void startSlave();

  // Stops the slave, deleting the containerizer, for subsequent
  // recovery testing.
  void stopSlave();

  enum ExecutionMode {
    // Wait for each task's termination before starting the next.
    // By implication this serializes fetching for each task.
    SERIALIZED_TASKS,

    // Don't wait. After starting all tasks you can then call
    // awaitTasksFinished() (see below) to wait for all terminations
    // altogether. This mode's real purpose is not to cause
    // concurrent execution of tasks, even though it may well do
    // so. Our main purpose is to observe concurrent fetch attempts,
    // in which multiple tasks compete for downloading the same URI.
    CONCURRENT_TASKS,
  };

  string runTask(const CommandInfo& commandInfo, const size_t taskIndex);

  vector<string> launchTasks(const vector<CommandInfo>& commandInfos);

  // Waits until FetcherProcess::run() has been called for all tasks.
  void awaitFetchContention();

  // Waits until all task executions have finished.
  void awaitAllTasksFinished();

  string assetsDirectory;
  string commandPath;
  string archivePath;

  slave::Flags flags;
  MesosContainerizer* containerizer;
  PID<Slave> slavePid;
  SlaveID slaveId;
  string cacheDirectory;
  MockFetcherProcess* fetcherProcess;
  MesosSchedulerDriver* driver;

private:
  Fetcher* fetcher;

  MockScheduler scheduler;
  FrameworkID frameworkId;

  // One status queue per task with a given index.
  vector<Queue<TaskStatus>> taskStatusQueues;

  // Promises whose futures indicate that
  // FetcherProcess::contentionBarrier() has been called for a task
  // with a given index.
  vector<Owned<Promise<Nothing>>> contentionBarriers;
};


void FetcherCacheTest::SetUp()
{
  MesosTest::SetUp();

  flags = CreateSlaveFlags();
  flags.resources = Option<string>(stringify(
      Resources::parse("cpus:1000;mem:1000").get()));
  flags.checkpoint = true;
  flags.recover = "reconnect";
  flags.strict = true;

  assetsDirectory = path::join(flags.work_dir, ASSETS_DIRECTORY_NAME);
  ASSERT_SOME(os::mkdir(assetsDirectory));

  setupCommandFileAsset();
  setupArchiveAsset();

  Try<PID<Master>> master = StartMaster();
  ASSERT_SOME(master);

  fetcherProcess = new MockFetcherProcess();
  fetcher = new Fetcher(Owned<FetcherProcess>(fetcherProcess));

  FrameworkInfo frameworkInfo;
  frameworkInfo.set_name("default");
  frameworkInfo.set_checkpoint(true);

  driver = new MesosSchedulerDriver(
    &scheduler, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  EXPECT_CALL(scheduler, registered(driver, _, _))
    .Times(1);
}


void FetcherCacheTest::TearDown()
{
  driver->stop();
  driver->join();
  delete driver;

  delete fetcher;

  MesosTest::TearDown();
}


void FetcherCacheTest::startSlave()
{
  Try<MesosContainerizer*> c = MesosContainerizer::create(
      flags, true, fetcher);
  ASSERT_SOME(c);
  containerizer = c.get();

  Try<PID<Slave>> pid = StartSlave(containerizer, flags);
  ASSERT_SOME(pid);
  slavePid = pid.get();
  // Obtain the slave ID.
  Future<SlaveRegisteredMessage> slaveRegisteredMessage =
    FUTURE_PROTOBUF(SlaveRegisteredMessage(), _, _);
  AWAIT_READY(slaveRegisteredMessage);
  slaveId = slaveRegisteredMessage.get().slave_id();

  cacheDirectory =
    slave::paths::getSlavePath(flags.fetcher_cache_dir, slaveId);

  driver->start();
}


void FetcherCacheTest::stopSlave()
{
  Stop(slavePid);
  delete containerizer;
}


void FetcherCacheTest::setupCommandFileAsset()
{
  commandPath = path::join(assetsDirectory, COMMAND_NAME);
  ASSERT_SOME(os::write(commandPath, COMMAND_SCRIPT));

  // Make the command file read-only, so we can discern the URI
  // executable flag.
  ASSERT_SOME(os::chmod(commandPath, S_IRUSR | S_IRGRP | S_IROTH));
}


void FetcherCacheTest::setupArchiveAsset()
{
  string path = path::join(assetsDirectory, ARCHIVED_COMMAND_NAME);
  ASSERT_SOME(os::write(path, ARCHIVED_COMMAND_SCRIPT));

  // Make the archived command file executable before archiving it,
  // since the executable flag for CommandInfo::URI has no effect on
  // what comes out of an archive.
  ASSERT_SOME(os::chmod(path, S_IRWXU | S_IRWXG | S_IRWXO));

  const string cwd = os::getcwd();
  ASSERT_SOME(os::chdir(assetsDirectory));
  ASSERT_SOME(os::tar(ARCHIVED_COMMAND_NAME, ARCHIVE_NAME));
  ASSERT_SOME(os::chdir(cwd));
  archivePath = path::join(assetsDirectory, ARCHIVE_NAME);

  // Make the archive file read-only, so we can tell if it becomes
  // executable by acccident.
  ASSERT_SOME(os::chmod(archivePath, S_IRUSR | S_IRGRP | S_IROTH));
}


static string taskName(int taskIndex)
{
  return stringify(taskIndex);
}


static bool isExecutable(const string& path)
{
  Try<bool> access = os::access(path, X_OK);
  EXPECT_SOME(access);
  return access.isSome() && access.get();
}


static void awaitFinished(Queue<TaskStatus> taskStatusQueue)
{
  for (int i = 0; i < 5; i++) {
    Future<TaskStatus> future = taskStatusQueue.get();
    AWAIT_READY(future);
    if (future.get().state() == TASK_FINISHED) {
      return;
    }
  }
  FAIL() << "Waited too many times for task status update";
}


// Pushes the TaskStatus value in mock call argument #1 into the
// given queue, which later on shall be queried by awaitFinished().
ACTION_P(PushTaskStatus, taskStatusQueue)
{
  TaskStatus taskStatus = arg1;
  taskStatusQueue->put(taskStatus);
}


// Launches a task as described by its CommandInfo and waits until it
// has status TASK_FINISHED, then returns its sandbox run directory path.
string FetcherCacheTest::runTask(
    const CommandInfo& commandInfo,
    const size_t taskIndex)
{
  Future<vector<Offer>> offers;
  EXPECT_CALL(scheduler, resourceOffers(driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(DeclineOffers());

  offers.await(Seconds(15));
  CHECK(offers.isReady()) << "Failed to wait for resource offers";

  EXPECT_NE(0u, offers.get().size());
  const Offer& offer = offers.get()[0];

  TaskInfo task;
  task.set_name(taskName(taskIndex));
  task.mutable_task_id()->set_value(taskName(taskIndex));
  task.mutable_slave_id()->CopyFrom(offer.slave_id());

  // We don't care about resources in these tests. This small amount
  // will always succeed.
  task.mutable_resources()->CopyFrom(
      Resources::parse("cpus:1;mem:1").get());

  task.mutable_command()->CopyFrom(commandInfo);

  // Since we are always using a command executor here, the executor
  // ID can be determined by copying the task ID.
  ExecutorID executorId;
  executorId.set_value(task.task_id().value());

  vector<TaskInfo> tasks;
  tasks.push_back(task);
  driver->launchTasks(offer.id(), tasks);

  Queue<TaskStatus> taskStatusQueue;
  EXPECT_CALL(scheduler, statusUpdate(driver, _))
    .WillRepeatedly(PushTaskStatus(&taskStatusQueue));

  awaitFinished(taskStatusQueue);

  return slave::paths::getExecutorLatestRunPath(
      flags.work_dir,
      slaveId,
      offer.framework_id(),
      executorId);
}


// Pushes the task status value of a task status update callback
// into the task status queue that corresponds to the task index/ID
// for which the status update is being reported. 'taskStatusQueues'
// must be a 'vector<Queue<TaskStatus>>*', where every slot
// index corresponds to a task index/ID.
ACTION_TEMPLATE(PushIndexedTaskStatus,
                HAS_1_TEMPLATE_PARAMS(int, k),
                AND_1_VALUE_PARAMS(taskStatusQueues))
{
  TaskStatus taskStatus = ::std::tr1::get<k>(args);
  Try<int> taskId = numify<int>(taskStatus.task_id().value());
  ASSERT_SOME(taskId);
  Queue<TaskStatus> queue = (*taskStatusQueues)[taskId.get()];
  queue.put(taskStatus);
}


// Satisfies the first promise in the list that is not satisfied yet.
ACTION_P(SatisfyOne, promises)
{
  foreach (const Owned<Promise<Nothing>>& promise, *promises) {
    if (promise->future().isPending()) {
      promise->set(Nothing());
      return;
    }
  }

  FAIL() << "Tried to call FetcherProcess::contentionBarrier() "
         << "for more tasks than launched";
}


// Launches the tasks described by the given CommandInfo and returns a
// vector holding the run directory paths. All these tasks run
// concurrently. To wait their completion call awaitAllTasksFinished().
vector<string> FetcherCacheTest::launchTasks(
    const vector<CommandInfo>& commandInfos)
{
  vector<string> runDirectories;

  EXPECT_CALL(scheduler, statusUpdate(driver, _))
    .WillRepeatedly(PushIndexedTaskStatus<1>(&taskStatusQueues));

  EXPECT_CALL(*fetcherProcess, contentionBarrier())
    .WillRepeatedly(
        DoAll(SatisfyOne(&contentionBarriers),
              Invoke(fetcherProcess,
              &MockFetcherProcess::unmocked_contentionBarrier)));

  Future<vector<Offer>> offers;
  EXPECT_CALL(scheduler, resourceOffers(driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(DeclineOffers());

  offers.await(Seconds(15));
  CHECK(offers.isReady()) << "Failed to wait for resource offers";

  EXPECT_NE(0u, offers.get().size());
  const Offer& offer = offers.get()[0];

  vector<TaskInfo> tasks;
  foreach (CommandInfo commandInfo, commandInfos) {
    size_t taskIndex = tasks.size();

    // Grabbing the framework ID from somewhere. It should not matter
    // if this happens several times, as we expect the framework ID to
    // remain the same.
    frameworkId = offer.framework_id();

    TaskInfo task;
    task.set_name(taskName(taskIndex));
    task.mutable_task_id()->set_value(taskName(taskIndex));
    task.mutable_slave_id()->CopyFrom(offer.slave_id());

    // We don't care about resources in these tests. This small amount
    // will always succeed.
    task.mutable_resources()->CopyFrom(
        Resources::parse("cpus:1;mem:1").get());

    task.mutable_command()->CopyFrom(commandInfo);

    tasks.push_back(task);

    // Since we are always using a command executor here, the executor
    // ID can be determined by copying the task ID.
    ExecutorID executorId;
    executorId.set_value(task.task_id().value());

    runDirectories.push_back(slave::paths::getExecutorLatestRunPath(
        flags.work_dir,
        slaveId,
        frameworkId,
        executorId));

    // Grabbing task status futures to wait for. We make a queue of futures
    // for each task. We can then wait until the front element indicates
    // status TASK_FINISHED. We use a queue, because we never know which
    // status update will be the one we have been waiting for.
    Queue<TaskStatus> taskStatusQueue;
    taskStatusQueues.push_back(taskStatusQueue);

    auto barrier = Owned<Promise<Nothing>>(new Promise<Nothing>());
    contentionBarriers.push_back(barrier);
  }

  driver->launchTasks(offer.id(), tasks);

  return runDirectories;
}


// Ensure that FetcherProcess::contentionBarrier() has been called for
// all tasks, which means that they are competing for downloading the
// same URIs.
void FetcherCacheTest::awaitFetchContention()
{
  foreach (const Owned<Promise<Nothing>>& barrier, contentionBarriers) {
    AWAIT(barrier->future());
  }
}


// Ensure all tasks are finished.
void FetcherCacheTest::awaitAllTasksFinished()
{
  foreach (Queue<TaskStatus> taskStatusQueue, taskStatusQueues) {
    awaitFinished(taskStatusQueue);
  }
}


// Tests fetching from the local asset directory without cache. This
// gives us a baseline for the following tests and lets us debug our
// test infrastructure without extra complications.
TEST_F(FetcherCacheTest, LocalUncached)
{
  startSlave();

  for (size_t i = 0; i < 3; i++) {
    CommandInfo::URI uri;
    uri.set_value(commandPath);
    uri.set_executable(true);

    CommandInfo commandInfo;
    commandInfo.set_value("./" + COMMAND_NAME + " " + taskName(i));
    commandInfo.add_uris()->CopyFrom(uri);

    const string& runDirectory = runTask(commandInfo, i);

    EXPECT_EQ(0u, fetcherProcess->countCacheFiles(slaveId, flags));

    const string& path = path::join(runDirectory, COMMAND_NAME);
    EXPECT_TRUE(isExecutable(path));
    EXPECT_TRUE(os::exists(path + taskName(i)));
  }
}


// Tests fetching from the local asset directory with simple caching.
// Only one download must occur. Fetching is serialized, to cover
// code areas without overlapping/concurrent fetch attempts.
TEST_F(FetcherCacheTest, LocalCached)
{
  startSlave();

  for (size_t i = 0; i < 3; i++) {
    CommandInfo::URI uri;
    uri.set_value(commandPath);
    uri.set_executable(true);
    uri.set_cache(true);

    CommandInfo commandInfo;
    commandInfo.set_value("./" + COMMAND_NAME + " " + taskName(i));
    commandInfo.add_uris()->CopyFrom(uri);

    const string& runDirectory = runTask(commandInfo, i);

    const string& path = path::join(runDirectory, COMMAND_NAME);
    EXPECT_TRUE(isExecutable(path));
    EXPECT_TRUE(os::exists(path + taskName(i)));

    EXPECT_EQ(1u, fetcherProcess->countCacheFiles(slaveId, flags));
  }
}


// Tests falling back on bypassing the cache when fetching the download
// size of a URI that is supposed to be cached fails.
TEST_F(FetcherCacheTest, CachedFallback)
{
  startSlave();

  // Make sure the content-length request fails.
  ASSERT_SOME(os::rm(commandPath));

  CommandInfo::URI uri;
  uri.set_value(commandPath);
  uri.set_executable(true);
  uri.set_cache(true);

  CommandInfo commandInfo;
  commandInfo.set_value("./" + COMMAND_NAME + " " + taskName(0));
  commandInfo.add_uris()->CopyFrom(uri);

  // Bring back the asset just before running mesos-fetcher to fetch it.
  Future<FetcherInfo> fetcherInfo;
  EXPECT_CALL(*fetcherProcess, run(_, _))
    .WillOnce(DoAll(FutureArg<0>(&fetcherInfo),
                    InvokeWithoutArgs(this,
                                      &FetcherCacheTest::setupCommandFileAsset),
                    Invoke(fetcherProcess,
                           &MockFetcherProcess::unmocked_run)));

  const string& runDirectory = runTask(commandInfo, 0);

  const string& path = path::join(runDirectory, COMMAND_NAME);
  EXPECT_TRUE(isExecutable(path));
  EXPECT_TRUE(os::exists(path + taskName(0)));

  AWAIT_READY(fetcherInfo);

  EXPECT_EQ(1, fetcherInfo.get().items_size());
  EXPECT_EQ(FetcherInfo::Item::BYPASS_CACHE,
            fetcherInfo.get().items(0).action());

  EXPECT_EQ(0, fetcherProcess->countCacheFiles(slaveId, flags));
}


// Tests archive extraction without caching as a baseline for the
// subsequent test below.
TEST_F(FetcherCacheTest, LocalUncachedExtract)
{
  startSlave();

  for (size_t i = 0; i < 3; i++) {
    CommandInfo::URI uri;
    uri.set_value(archivePath);
    uri.set_extract(true);

    CommandInfo commandInfo;
    commandInfo.set_value("./" + ARCHIVED_COMMAND_NAME + " " + taskName(i));
    commandInfo.add_uris()->CopyFrom(uri);

    const string& runDirectory = runTask(commandInfo, i);

    EXPECT_TRUE(os::exists(path::join(runDirectory, ARCHIVE_NAME)));
    EXPECT_FALSE(isExecutable(path::join(runDirectory, ARCHIVE_NAME)));

    const string& path = path::join(runDirectory, ARCHIVED_COMMAND_NAME);
    EXPECT_TRUE(isExecutable(path));
    EXPECT_TRUE(os::exists(path + taskName(i)));

    EXPECT_EQ(0u, fetcherProcess->countCacheFiles(slaveId, flags));
  }
}


// Tests archive extraction in combination with caching.
TEST_F(FetcherCacheTest, LocalCachedExtract)
{
  startSlave();

  for (size_t i = 0; i < 3; i++) {
    CommandInfo::URI uri;
    uri.set_value(archivePath);
    uri.set_extract(true);
    uri.set_cache(true);

    CommandInfo commandInfo;
    commandInfo.set_value("./" + ARCHIVED_COMMAND_NAME + " " + taskName(i));
    commandInfo.add_uris()->CopyFrom(uri);

    const string& runDirectory = runTask(commandInfo, i);

    EXPECT_FALSE(os::exists(path::join(runDirectory, ARCHIVE_NAME)));

    const string& path = path::join(runDirectory, ARCHIVED_COMMAND_NAME);
    EXPECT_TRUE(isExecutable(path));
    EXPECT_TRUE(os::exists(path + taskName(i)));

    EXPECT_EQ(1u, fetcherProcess->countCacheFiles(slaveId, flags));
  }
}


class FetcherCacheHttpTest : public FetcherCacheTest
{
public:
  // A minimal HTTP server (not intended as an actor) just reusing what
  // is already implemented somewhere to serve some HTTP requests for
  // file downloads. Plus counting how many requests are made. Plus the
  // ability to pause answering requests, stalling them.
  class HttpServer : public Process<HttpServer>
  {
  public:
    HttpServer(FetcherCacheHttpTest* test)
      : countRequests(0),
        countCommandRequests(0),
        countArchiveRequests(0)
    {
      provide(COMMAND_NAME, test->commandPath);
      provide(ARCHIVE_NAME, test->archivePath);

      spawn(this);
    }

    string url()
    {
      return "http://127.0.0.1:" +
             stringify(self().address.port) +
             "/" + self().id + "/";
    }

    // Stalls the execution of HTTP requests inside visit().
    void pause()
    {
      mutex.lock();
    }

    void resume()
    {
      mutex.unlock();
    }

    virtual void visit(const HttpEvent& event)
    {
      std::lock_guard<std::mutex> lock(mutex);

      countRequests++;

      if (event.request->path.find(COMMAND_NAME) != std::string::npos) {
        countCommandRequests++;
      }

      if (event.request->path.find(ARCHIVE_NAME) != std::string::npos) {
        countArchiveRequests++;
      }

      ProcessBase::visit(event);
    }

    void resetCounts()
    {
      countRequests = 0;
      countCommandRequests = 0;
      countArchiveRequests = 0;
    }

    size_t countRequests;
    size_t countCommandRequests;
    size_t countArchiveRequests;

  private:
    std::mutex mutex;
  };


  virtual void SetUp()
  {
    FetcherCacheTest::SetUp();

    httpServer = new HttpServer(this);
  }

  virtual void TearDown()
  {
    terminate(httpServer);
    wait(httpServer);
    delete httpServer;

    FetcherCacheTest::TearDown();
  }

  HttpServer* httpServer;
};


// Tests fetching via HTTP with caching. Only one download must
// occur. Fetching is serialized, to cover code areas without
// overlapping/concurrent fetch attempts.
TEST_F(FetcherCacheHttpTest, HttpCachedSerialized)
{
  startSlave();

  for (size_t i = 0; i < 3; i++) {
    CommandInfo::URI uri;
    uri.set_value(httpServer->url() + COMMAND_NAME);
    uri.set_executable(true);
    uri.set_cache(true);

    CommandInfo commandInfo;
    commandInfo.set_value("./" + COMMAND_NAME + " " + taskName(i));
    commandInfo.add_uris()->CopyFrom(uri);

    const string& runDirectory = runTask(commandInfo, i);

    const string& path = path::join(runDirectory, COMMAND_NAME);
    EXPECT_TRUE(isExecutable(path));
    EXPECT_TRUE(os::exists(path + taskName(i)));

    EXPECT_EQ(1u, fetcherProcess->countCacheFiles(slaveId, flags));

    // 2 requests: 1 for content-length, 1 for download.
    EXPECT_EQ(2u, httpServer->countCommandRequests);
  }
}


// Tests multiple concurrent fetching efforts that require some
// concurrency control. One task must "win" and perform the size
// and download request for the URI alone. The others must reuse
// the result.
TEST_F(FetcherCacheHttpTest, HttpCachedConcurrent)
{
  startSlave();

  // Causes fetch contention. No task can run yet until resume().
  httpServer->pause();

  vector<CommandInfo> commandInfos;
  const size_t countTasks = 5;

  for (size_t i = 0; i < countTasks; i++) {
    CommandInfo::URI uri0;
    uri0.set_value(httpServer->url() + COMMAND_NAME);
    uri0.set_executable(true);
    uri0.set_cache(true);

    CommandInfo commandInfo;
    commandInfo.set_value("./" + COMMAND_NAME + " " + taskName(i));
    commandInfo.add_uris()->CopyFrom(uri0);

    // Not always caching this URI causes that it will be downloaded
    // some of the time. Thus we exercise code paths that eagerly fetch
    // new assets while waiting for pending downloads of cached assets
    // as well as code paths where no downloading occurs at all.
    if (i % 2 == 1) {
      CommandInfo::URI uri1;
      uri1.set_value(httpServer->url() + ARCHIVE_NAME);
      commandInfo.add_uris()->CopyFrom(uri1);
    }

    commandInfos.push_back(commandInfo);
  }

  vector<string> runDirectories = launchTasks(commandInfos);

  CHECK_EQ(countTasks, runDirectories.size());

  // Given pausing the HTTP server, this prooves that fetch contention
  // has happened. All tasks have passed the point where it occurs,
  // but they are not running yet.
  awaitFetchContention();

  // Now let the tasks run.
  httpServer->resume();

  awaitAllTasksFinished();

  EXPECT_EQ(1u, fetcherProcess->countCacheFiles(slaveId, flags));

  // command content-length requests: 1
  // command downloads: 1
  // archive downloads: 2
  EXPECT_EQ(2u, httpServer->countCommandRequests);
  EXPECT_EQ(2u, httpServer->countArchiveRequests);

  for (size_t i = 0; i < countTasks; i++) {
    EXPECT_EQ(i % 2 == 1,
              os::exists(path::join(runDirectories[i], ARCHIVE_NAME)));
    EXPECT_TRUE(isExecutable(path::join(runDirectories[i], COMMAND_NAME)));
    EXPECT_TRUE(os::exists(
        path::join(runDirectories[i], COMMAND_NAME + taskName(i))));
  }
}


// Tests using multiple URIs per command, variations of caching,
// setting the executable flag, and archive extraction.
TEST_F(FetcherCacheHttpTest, HttpMixed)
{
  startSlave();

  // Causes fetch contention. No task can run yet until resume().
  httpServer->pause();

  vector<CommandInfo> commandInfos;

  // Task 0.

  CommandInfo::URI uri00;
  uri00.set_value(httpServer->url() + ARCHIVE_NAME);
  uri00.set_cache(true);
  uri00.set_extract(false);
  uri00.set_executable(false);

  CommandInfo::URI uri01;
  uri01.set_value(httpServer->url() + COMMAND_NAME);
  uri01.set_extract(false);
  uri01.set_executable(true);

  CommandInfo commandInfo0;
  commandInfo0.set_value("./" + COMMAND_NAME + " " + taskName(0));
  commandInfo0.add_uris()->CopyFrom(uri00);
  commandInfo0.add_uris()->CopyFrom(uri01);
  commandInfos.push_back(commandInfo0);

  // Task 1.

  CommandInfo::URI uri10;
  uri10.set_value(httpServer->url() + ARCHIVE_NAME);
  uri10.set_extract(true);
  uri10.set_executable(false);

  CommandInfo::URI uri11;
  uri11.set_value(httpServer->url() + COMMAND_NAME);
  uri11.set_extract(true);
  uri11.set_executable(false);

  CommandInfo commandInfo1;
  commandInfo1.set_value("./" + ARCHIVED_COMMAND_NAME + " " + taskName(1));
  commandInfo1.add_uris()->CopyFrom(uri10);
  commandInfo1.add_uris()->CopyFrom(uri11);
  commandInfos.push_back(commandInfo1);

  // Task 2.

  CommandInfo::URI uri20;
  uri20.set_value(httpServer->url() + ARCHIVE_NAME);
  uri20.set_cache(true);
  uri20.set_extract(true);
  uri20.set_executable(false);

  CommandInfo::URI uri21;
  uri21.set_value(httpServer->url() + COMMAND_NAME);
  uri21.set_extract(false);
  uri21.set_executable(false);

  CommandInfo commandInfo2;
  commandInfo2.set_value("./" + ARCHIVED_COMMAND_NAME + " " + taskName(2));
  commandInfo2.add_uris()->CopyFrom(uri20);
  commandInfo2.add_uris()->CopyFrom(uri21);
  commandInfos.push_back(commandInfo2);

  vector<string> runDirectories = launchTasks(commandInfos);

  CHECK_EQ(3u, runDirectories.size());

  // Given pausing the HTTP server, this prooves that fetch contention
  // has happened. All tasks have passed the point where it occurs,
  // but they are not running yet.
  awaitFetchContention();

  // Now let the tasks run.
  httpServer->resume();

  awaitAllTasksFinished();

  EXPECT_EQ(1u, fetcherProcess->countCacheFiles(slaveId, flags));

  // command content-length requests: 0
  // command downloads: 3
  // archive content-length requests: 1
  // archive downloads: 2
  EXPECT_EQ(3u, httpServer->countCommandRequests);
  EXPECT_EQ(3u, httpServer->countArchiveRequests);

  // Task 0.

  EXPECT_FALSE(isExecutable(path::join(runDirectories[0], ARCHIVE_NAME)));
  EXPECT_FALSE(os::exists(path::join(
      runDirectories[0], ARCHIVED_COMMAND_NAME)));

  EXPECT_TRUE(isExecutable(path::join(runDirectories[0], COMMAND_NAME)));
  EXPECT_TRUE(os::exists(path::join(
      runDirectories[0], COMMAND_NAME + taskName(0))));

  // Task 1.

  EXPECT_FALSE(isExecutable(path::join(runDirectories[1], ARCHIVE_NAME)));
  EXPECT_TRUE(isExecutable(path::join(
      runDirectories[1], ARCHIVED_COMMAND_NAME)));
  EXPECT_TRUE(os::exists(path::join(
      runDirectories[1], ARCHIVED_COMMAND_NAME + taskName(1))));

  EXPECT_FALSE(isExecutable(path::join(runDirectories[1], COMMAND_NAME)));

  // Task 2.

  EXPECT_FALSE(os::exists(path::join(runDirectories[2], ARCHIVE_NAME)));
  EXPECT_TRUE(isExecutable(path::join(
      runDirectories[2], ARCHIVED_COMMAND_NAME)));
  EXPECT_TRUE(os::exists(path::join(
      runDirectories[2], ARCHIVED_COMMAND_NAME + taskName(2))));

  EXPECT_FALSE(isExecutable(path::join(runDirectories[2], COMMAND_NAME)));
}


// Tests slave recovery of the fetcher cache. The cache must be
// wiped clean on recovery, causing renewed downloads.
TEST_F(FetcherCacheHttpTest, HttpCachedRecovery)
{
  startSlave();

  for (size_t i = 0; i < 3; i++) {
    CommandInfo::URI uri;
    uri.set_value(httpServer->url() + COMMAND_NAME);
    uri.set_executable(true);
    uri.set_cache(true);

    CommandInfo commandInfo;
    commandInfo.set_value("./" + COMMAND_NAME + " " + taskName(i));
    commandInfo.add_uris()->CopyFrom(uri);

    const string& runDirectory = runTask(commandInfo, i);

    const string& path = path::join(runDirectory, COMMAND_NAME);
    EXPECT_TRUE(isExecutable(path));
    EXPECT_TRUE(os::exists(path + taskName(i)));

    EXPECT_EQ(1u, fetcherProcess->countCacheFiles(slaveId, flags));

    // content-length requests: 1
    // downloads: 1
    EXPECT_EQ(2u, httpServer->countCommandRequests);
  }

  stopSlave();

  // Start over.
  httpServer->resetCounts();

  // Don't reuse the old fetcher, which has stale state after
  // stopping the slave.
  Fetcher fetcher2;

  Try<MesosContainerizer*> c =
    MesosContainerizer::create(flags, true, &fetcher2);
  CHECK_SOME(c);
  containerizer = c.get();

  // Set up so we can wait until the new slave updates the container's
  // resources (this occurs after the executor has re-registered).
  Future<Nothing> update =
    FUTURE_DISPATCH(_, &MesosContainerizerProcess::update);

  Try<PID<Slave>> pid = StartSlave(containerizer, flags);
  CHECK_SOME(pid);
  slavePid = pid.get();

  // Wait until the containerizer is updated.
  AWAIT_READY(update);

  // Recovery must have cleaned the cache by now.
  EXPECT_FALSE(os::exists(cacheDirectory));

  // Repeat of the above to see if it works the same.
  for (size_t i = 0; i < 3; i++) {
    CommandInfo::URI uri;
    uri.set_value(httpServer->url() + COMMAND_NAME);
    uri.set_executable(true);
    uri.set_cache(true);

    CommandInfo commandInfo;
    commandInfo.set_value("./" + COMMAND_NAME + " " + taskName(i));
    commandInfo.add_uris()->CopyFrom(uri);

    const string& runDirectory = runTask(commandInfo, i);

    const string& path = path::join(runDirectory, COMMAND_NAME);
    EXPECT_TRUE(isExecutable(path));
    EXPECT_TRUE(os::exists(path + taskName(i)));

    EXPECT_EQ(1u, fetcherProcess->countCacheFiles(slaveId, flags));

    // content-length requests: 1
    // downloads: 1
    EXPECT_EQ(2u, httpServer->countCommandRequests);
  }
}


// Tests cache eviction. Limits the available cache space then fetches
// more task scripts than fit into the cache and runs them all. We
// observe how the number of cache files rises and then stays constant.
TEST_F(FetcherCacheTest, SimpleEviction)
{
  const size_t countCacheEntries = 3;

  // Let only the first 'countCacheEntries' downloads fit in the cache.
  flags.fetcher_cache_size = COMMAND_SCRIPT.size() * countCacheEntries;

  startSlave();

  for (size_t i = 0; i < countCacheEntries + 2; i++) {
    string commandFilename = "cmd" + stringify(i);
    string command = commandFilename + " " + taskName(i);

    commandPath = path::join(assetsDirectory, commandFilename);
    ASSERT_SOME(os::write(commandPath, COMMAND_SCRIPT));

    CommandInfo::URI uri;
    uri.set_value(commandPath);
    uri.set_executable(true);
    uri.set_cache(true);

    CommandInfo commandInfo;
    commandInfo.set_value("./" + command);
    commandInfo.add_uris()->CopyFrom(uri);

    const string& runDirectory = runTask(commandInfo, i);

    // Check that the task succeeded.
    EXPECT_TRUE(isExecutable(path::join(runDirectory, commandFilename)));
    EXPECT_TRUE(os::exists(
        path::join(runDirectory, COMMAND_NAME + taskName(i))));

    if (i < countCacheEntries) {
      EXPECT_EQ(i + 1, fetcherProcess->countCacheFiles(slaveId, flags));
    } else {
      EXPECT_EQ(countCacheEntries,
                fetcherProcess->countCacheFiles(slaveId, flags));
    }
  }
}


// Tests cache eviction fallback to bypassing the cache. A first task
// runs normally. Then a second succeeds using eviction. Then a third
// task fails to evict, but still gets executed bypassing the cache.
TEST_F(FetcherCacheTest, FallbackFromEviction)
{
  // The size by which every task's URI download is going to be larger
  // than the previous one.
  const size_t growth = 10;

  // Let only the first two downloads fit into the cache, one at a time,
  // the second evicting the first. The third file won't fit any more,
  // being larger than the entire cache.
  flags.fetcher_cache_size = COMMAND_SCRIPT.size() + growth;

  startSlave();

  Future<FetcherInfo> fetcherInfo0;
  Future<FetcherInfo> fetcherInfo1;
  Future<FetcherInfo> fetcherInfo2;
  EXPECT_CALL(*fetcherProcess, run(_, _))
    .WillOnce(DoAll(FutureArg<0>(&fetcherInfo0),
                    Invoke(fetcherProcess,
                           &MockFetcherProcess::unmocked_run)))
    .WillOnce(DoAll(FutureArg<0>(&fetcherInfo1),
                    Invoke(fetcherProcess,
                           &MockFetcherProcess::unmocked_run)))
    .WillOnce(DoAll(FutureArg<0>(&fetcherInfo2),
                    Invoke(fetcherProcess,
                           &MockFetcherProcess::unmocked_run)));

  for (size_t i = 0; i < 3; i++) {
    string commandFilename = "cmd" + stringify(i);
    string command = commandFilename + " " + taskName(i);

    commandPath = path::join(assetsDirectory, commandFilename);

    // Write the command into the script that gets fetched. Add 'growth'
    // extra characters every time we get here. Eventually, the file will
    // be so big, it will not fit into the cache any more.
    ASSERT_SOME(os::write(
        commandPath,
        COMMAND_SCRIPT + std::string(i * growth, '\n')));

    CommandInfo::URI uri;
    uri.set_value(commandPath);
    uri.set_executable(true);
    uri.set_cache(true);

    CommandInfo commandInfo;
    commandInfo.set_value("./" + command);
    commandInfo.add_uris()->CopyFrom(uri);

    const string& runDirectory = runTask(commandInfo, i);

    // Check that the task succeeded.
    EXPECT_TRUE(isExecutable(path::join(runDirectory, commandFilename)));
    EXPECT_TRUE(os::exists(
        path::join(runDirectory, COMMAND_NAME + taskName(i))));

    // A := COMMAND_SCRIPT.size()
    // B := growth
    switch (i) {
      case 0: {
        AWAIT_READY(fetcherInfo0);

        EXPECT_EQ(1, fetcherInfo0.get().items_size());
        EXPECT_EQ(FetcherInfo::Item::DOWNLOAD_AND_CACHE,
                  fetcherInfo0.get().items(0).action());

        // We put a file of size A into the cache with space (A + B).
        // So we have B space left.
        CHECK_EQ(Bytes(growth), fetcherProcess->availableCacheSpace());
        break;
      }
      case 1: {
        AWAIT_READY(fetcherInfo1);

        EXPECT_EQ(1, fetcherInfo1.get().items_size());
        EXPECT_EQ(FetcherInfo::Item::DOWNLOAD_AND_CACHE,
                  fetcherInfo1.get().items(0).action());

        // The cache must now be full.
        CHECK_EQ(Bytes(0u), fetcherProcess->availableCacheSpace());
        break;
      }
      case 2: {
        // _reserveCacheSpace() is not called in this pass, because
        // reserveCacheSpace() fails. And that is why we use WillOnce()
        // only once when expecting _reserveCacheSpace() above.

        AWAIT_READY(fetcherInfo2);

        EXPECT_EQ(1, fetcherInfo2.get().items_size());
        EXPECT_EQ(FetcherInfo::Item::BYPASS_CACHE,
                  fetcherInfo2.get().items(0).action());
        break;
      }
      default: {
        CHECK(false);
        break;
      }
    }

    // Always one file in the cache: first after initial download,
    // then after successful eviction, then after failed eviction.
    EXPECT_EQ(1u, fetcherProcess->countCacheFiles(slaveId, flags));
  }
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
