#ifndef __IOEVENT_H__
#define __IOEVENT_H__

#include <stdint.h>
#include <poll.h>
#include <sys/time.h>

#define IOEVENT_TIMEOUT  0x8000

#if IOEVENT_USE_EPOLL
#include <sys/epoll.h>
/* epoll触发方式分ET和LT两种，ET是高速方式，不会重复通知，默认为LT */
#define IOEVENT_EDGE_TRIGGER EPOLLET

#define IOEVENT_READ  EPOLLIN
#define IOEVENT_WRITE EPOLLOUT
#define IOEVENT_ERROR (EPOLLERR | EPOLLPRI | EPOLLHUP)

#elif IOEVENT_USE_KQUEUE
#include <sys/event.h>
#define IOEVENT_EDGE_TRIGGER EV_CLEAR

#define KPOLLIN    0x001
#define KPOLLPRI   0x002
#define KPOLLOUT   0x004
#define KPOLLERR   0x010
#define KPOLLHUP   0x020
#define IOEVENT_READ  KPOLLIN
#define IOEVENT_WRITE KPOLLOUT
#define IOEVENT_ERROR (KPOLLHUP | KPOLLPRI | KPOLLHUP)

#ifdef __cplusplus
extern "C" {
#endif

int kqueue_ev_convert(int16_t event, uint16_t flags);

#ifdef __cplusplus
}
#endif

#elif IOEVENT_USE_PORT
#include <port.h>
#define IOEVENT_EDGE_TRIGGER 0

#define IOEVENT_READ  POLLIN
#define IOEVENT_WRITE POLLOUT
#define IOEVENT_ERROR (POLLERR | POLLPRI | POLLHUP)
#endif

typedef struct ioevent_puller {
    int size;  //max events (fd)
    int extra_events;
    int poll_fd;	/* 创建epoll句柄后占用一个文件描述符 */

#if IOEVENT_USE_EPOLL
    struct epoll_event *events;	/* events集合 */
    int timeout;	/* epoll_wait的超时时间 */
#elif IOEVENT_USE_KQUEUE
    struct kevent *events;
    struct timespec timeout;
#elif IOEVENT_USE_PORT
    port_event_t *events;
    timespec_t timeout;
#endif
} IOEventPoller;	/* ioevent调度结构体 */

/* 获取ioevent中的events，例如在使用epoll时是EPOLLIN还是EPOLLOUT等 */
#if IOEVENT_USE_EPOLL
  #define IOEVENT_GET_EVENTS(ioevent, index) \
      ioevent->events[index].events
#elif IOEVENT_USE_KQUEUE
  #define IOEVENT_GET_EVENTS(ioevent, index)  kqueue_ev_convert( \
      ioevent->events[index].filter, ioevent->events[index].flags)
#elif IOEVENT_USE_PORT
  #define IOEVENT_GET_EVENTS(ioevent, index) \
      ioevent->events[index].portev_events
#else
#error port me
#endif

/* 获取ioevent中的data */
#if IOEVENT_USE_EPOLL
  #define IOEVENT_GET_DATA(ioevent, index)  \
      ioevent->events[index].data.ptr
#elif IOEVENT_USE_KQUEUE
  #define IOEVENT_GET_DATA(ioevent, index)  \
      ioevent->events[index].udata
#elif IOEVENT_USE_PORT
  #define IOEVENT_GET_DATA(ioevent, index)  \
      ioevent->events[index].portev_user
#else
#error port me
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* 初始化ioevent，设置events数量，超时时间等 */
int ioevent_init(IOEventPoller *ioevent, const int size,
    const int timeout, const int extra_events);

/* 销毁ioevent相关资源 */
void ioevent_destroy(IOEventPoller *ioevent);

/* 在events集合中添加event */
int ioevent_attach(IOEventPoller *ioevent, const int fd, const int e,
    void *data);

/* 修改已注册的fd的监听事件 */
int ioevent_modify(IOEventPoller *ioevent, const int fd, const int e,
    void *data);

/* 在events集合中删除指定event */
int ioevent_detach(IOEventPoller *ioevent, const int fd);

/* 调用epoll_wait等，等待指定文件可读或可写 */
int ioevent_poll(IOEventPoller *ioevent);

#ifdef __cplusplus
}
#endif

#endif

