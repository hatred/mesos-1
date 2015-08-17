/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __STOUT_OS_WINDOWS_DUP_HPP__
#define __STOUT_OS_WINDOWS_DUP_HPP__

#include <stout/try.hpp>

#include <stout/windows/net.hpp>

namespace os {

inline Try<int> dup(int fd)
{
  if (isSocket(fd)) {
    WSAPROTOCOL_INFO info;
    if (WSADuplicateSocket(fd, GetCurrentProcessId(), &info)) {
      return WSAError();
    }

    SOCKET s = WSASocket(
        FROM_PROTOCOL_INFO,
        FROM_PROTOCOL_INFO,
        FROM_PROTOCOL_INFO,
        &info,
        0,
        WSA_FLAG_OVERLAPPED);

    if (s == INVALID_SOCKET) {
      return WSAError();
    }

    return s;
  }

  // TODO(benh): Duplicate files and pipes via 'DuplicateHandle'?
  return ::dup(fd);
}

} // namespace os {

#endif // __STOUT_OS_WINDOWS_DUP_HPP__
