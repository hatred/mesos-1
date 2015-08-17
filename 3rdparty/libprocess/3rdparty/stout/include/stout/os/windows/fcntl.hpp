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

#ifndef __STOUT_OS_WINDOWS_FCNTL_HPP__
#define __STOUT_OS_WINDOWS_FCNTL_HPP__

#include <stout/error.hpp>
#include <stout/nothing.hpp>
#include <stout/try.hpp>

#include <stout/windows/net.hpp>

namespace os {

inline Try<Nothing> cloexec(int fd)
{
  // NOTE: net::socket just returns the SOCKET rather than a file
  // descriptor via '_open_osfhandle' so we only need '_get_osfhandle'
  // if it's not a socket (e.g., file or pipe).
  HANDLE handle = fd;

  if (!net::isSocket(fd)) {
    handle = (HANDLE) _get_osfhandle(fd);
    if (handle == INVALID_HANDLE_VALUE) {
      return Error(strerror(EBADF));
    }
  }

  DWORD type = GetFileType(handle);

  // Character files like console cannot be made non-inheritable.
  if (type != FILE_TYPE_CHAR) {
    return Nothing();
  }

  if (SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) == 0) {
    return WindowsError();
  }

  return Nothing();
}


inline Try<bool> isCloexec(int fd)
{
  // NOTE: net::socket just returns the SOCKET rather than a file
  // descriptor via '_open_osfhandle' so we only need '_get_osfhandle'
  // if it's not a socket (e.g., file or pipe).
  HANDLE handle = fd;

  if (!net::isSocket(fd)) {
    handle = (HANDLE) _get_osfhandle(fd);
    if (handle == INVALID_HANDLE_VALUE) {
      return Error(strerror(EBADF));
    }
  }

  DWORD flags;

  if (GetHandleInformation(handle, &flags) == 0) {
    return WindowsError();
  }

  if ((flags & HANDLE_FLAG_INHERIT) != 0) {
    return false;
  }

  return true;
}


inline Try<Nothing> nonblock(int fd)
{
  if (net::isSocket(fd)) {
    SOCKET s = fd;
    u_long argp = 1;
    if (ioctlsocket(s, FIONBIO, &argp) == SOCKET_ERROR) {
      return WSAError();
    }
  }

  // TODO(benh): Handle non-blocking for pipes by using
  // 'SetNamedPipeHandleState'.

  return Nothing();
}

} // namespace os {

#endif // __STOUT_OS_WINDOWS_FCNTL_HPP__
