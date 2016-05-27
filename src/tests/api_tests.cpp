// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// TODO(benh): Create 'mesos/v1/master.hpp' instead?
#include <mesos/v1/master.pb.h> // ONLY USEFUL AFTER RUNNING PROTOC.

#include <process/future.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>

#include <stout/gtest.hpp>
#include <stout/jsonify.hpp>
#include <stout/try.hpp>

#include "tests/mesos.hpp"

using process::Future;

using process::http::OK;
using process::http::Response;

namespace mesos {
namespace internal {
namespace tests {

TEST_F(APITest, Flags)
{
  Try<Owned<cluster::Master>> master = this->StartMaster();
  ASSERT_SOME(master);

  v1::master::Call call;
  call.set_type(v1::master::Call::GET_FLAGS);

  process::http::Headers headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);
  headers["Accept"] = APPLICATION_JSON;

  Future<Response> response = http::post(
      master.get()->pid,
      "/api/v1",
      None(),
      jsonify(call),
      APPLICATION_JSON);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
    << response.get().body;

  // TODO(benh): Compare `response.body` to the actual flags that
  // should get generated.
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
