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
#ifndef __STOUT_WINDOWS_ERROR_HPP__
#define __STOUT_WINDOWS_ERROR_HPP__

class WindowsError
{
public:
  // TODO(benh): Implement this using `FormatMessage`. See
  // https://msdn.microsoft.com/en-us/library/ms680582(VS.85).aspx.
  explicit WindowsError(const std::string& _message)
    : Error(message + ": GetLastError(): " + stringify(GetLastError())) {}
};


class WSAError
{
public:
  // TODO(benh): Implement this using `FormatMessage`. See
  // https://msdn.microsoft.com/en-us/library/ms680582(VS.85).aspx.
  explicit WSAError(const std::string& _message)
    : Error(message + ": WSAGetLastError(): " + stringify(WSAGetLastError())) {}
};

#endif // __STOUT_WINDOWS_ERROR_HPP__
