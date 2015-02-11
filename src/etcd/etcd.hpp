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

#include <process/future.hpp>

#include <stout/duration.hpp>
#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

#include "etcd/url.hpp"

namespace etcd {

// Represents the JSON structure etcd returns for a node (key/value).
struct Node
{
  static Try<Node> parse(const JSON::Object& object);

  Option<uint64_t> createdIndex;
  Option<std::string> expiration;
  std::string key;
  Option<uint64_t> modifiedIndex;
  Option<Duration> ttl;
  Option<std::string> value;
  Option<bool> dir;
  Option<std::vector<Node>> nodes;

  // Since our APIs return just the Node rather than the entire JSON
  // response, we capture 'prevNode' here.
  Option<Node> previous;

private:
  // Require everyone to call 'parse'.
  Node() {}
};


// Represents the JSON structure etcd returns for each call.
struct Response
{
  // Returns an etcd "response" from a JSON object.
  static Try<Response> parse(const Try<JSON::Object>& object);

  // Fields present if the request was successful. Note, however, that
  // since our APIs return just the Node rather than the entire
  // Response we capture 'prevNode' within Node as Node::previous so
  // that it is accessible.
  Option<std::string> action;
  Option<Node> node;

  // Fields present if the response represents an error.
  Option<int> errorCode;
  Option<std::string> cause;
  Option<std::string> message;
  Option<uint64_t> index;
};


// Attempts to create an etcd node (key/value) at the specified URL
// and then returns the node. Note that if the node already exists or
// not all of the specified comparable conditions ('prevExist',
// 'prevIndex', 'prevValue') are true then None is returned.
process::Future<Option<Node>> create(
    const URL& url,
    const std::string& value,
    const Option<Duration>& ttl = None(),
    const Option<bool> prevExist = None(),
    const Option<uint64_t>& prevIndex = None(),
    const Option<std::string>& prevValue = None());


// Returns the etcd node at the specified URL, or None if no node
// exists.
process::Future<Option<Node>> get(const URL& url);


// Watches the etcd node at the specified URL, and returns the node
// after a change has occured, or None if the node has been
// deleted.
process::Future<Option<Node>> watch(
    const URL& url,
    const Option<uint64_t>& waitIndex = None());

} // namespace etcd {
