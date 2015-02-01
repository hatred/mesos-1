#ifndef __LIBEVENT_HPP__
#define __LIBEVENT_HPP__

namespace process {

// Event loop.
extern struct event_base* base;

void run_in_event_loop(const lambda::function<void(void)>& f);

} // namespace process {

#endif // __LIBEVENT_HPP__
