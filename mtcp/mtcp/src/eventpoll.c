#include <sys/queue.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <assert.h>

#include "mtcp.h"
#include "tcp_stream.h"
#include "eventpoll.h"
#include "tcp_in.h"
#include "pipe.h"
#include "debug.h"

#define MAX(a, b) ((a)>(b)?(a):(b))
#define MIN(a, b) ((a)<(b)?(a):(b))

#define SPIN_BEFORE_SLEEP FALSE
#define SPIN_THRESH 10000000

/*----------------------------------------------------------------------------*/
char *event_str[] = {"NONE", "IN", "PRI", "OUT", "ERR", "HUP", "RDHUP"};
/*----------------------------------------------------------------------------*/
// 返回事件类型的字符串
char * 
EventToString(uint32_t event)
{
	switch (event) {
		case MTCP_EPOLLNONE:
			return event_str[0];
			break;
		case MTCP_EPOLLIN:
			return event_str[1];
			break;
		case MTCP_EPOLLPRI:
			return event_str[2];
			break;
		case MTCP_EPOLLOUT:
			return event_str[3];
			break;
		case MTCP_EPOLLERR:
			return event_str[4];
			break;
		case MTCP_EPOLLHUP:
			return event_str[5];
			break;
		case MTCP_EPOLLRDHUP:
			return event_str[6];
			break;
		default:
			assert(0);
	}
	
	assert(0);
	return NULL;
}
/*----------------------------------------------------------------------------*/
// 创建一个新的事件队列
struct event_queue *
CreateEventQueue(int size)
{
	struct event_queue *eq;

	// 为一个新的事件队列分配空间
	eq = (struct event_queue *)calloc(1, sizeof(struct event_queue));
	if (!eq)
		return NULL;

	eq->start = 0;
	eq->end = 0;
	eq->size = size;
	eq->events = (struct mtcp_epoll_event_int *)
			calloc(size, sizeof(struct mtcp_epoll_event_int));	// 为events总数分配空间
	if (!eq->events) {
		free(eq);
		return NULL;
	}
	eq->num_events = 0;

	return eq;
}
/*----------------------------------------------------------------------------*/
// 销毁事件队列
void 
DestroyEventQueue(struct event_queue *eq)
{
	if (eq->events)
		free(eq->events);

	free(eq);
}
/*----------------------------------------------------------------------------*/
int 
mtcp_epoll_create(mctx_t mctx, int size)
{
	mtcp_manager_t mtcp = g_mtcp[mctx->cpu];
	struct mtcp_epoll *ep;
	socket_map_t epsocket;

	if (size <= 0) {
		errno = EINVAL;
		return -1;
	}

	// 申请一个用于epoll的新的socket对象
	epsocket = AllocateSocket(mctx, MTCP_SOCK_EPOLL, FALSE);
	if (!epsocket) {
		errno = ENFILE;
		return -1;
	}

	// 创建epoll管理对象
	ep = (struct mtcp_epoll *)calloc(1, sizeof(struct mtcp_epoll));
	if (!ep) {
		FreeSocket(mctx, epsocket->id, FALSE);
		return -1;
	}

	/* create event queues */
	// 创建epoll事件队列
	ep->usr_queue = CreateEventQueue(size);
	if (!ep->usr_queue)
		return -1;

	ep->usr_shadow_queue = CreateEventQueue(size);
	if (!ep->usr_shadow_queue) {
		DestroyEventQueue(ep->usr_queue);
		return -1;
	}

	ep->mtcp_queue = CreateEventQueue(size);
	if (!ep->mtcp_queue) {
		DestroyEventQueue(ep->usr_queue);
		DestroyEventQueue(ep->usr_shadow_queue);
		return -1;
	}

	TRACE_EPOLL("epoll structure of size %d created.\n", ep->size);

	mtcp->ep = ep;
	epsocket->ep = ep;

	if (pthread_mutex_init(&ep->epoll_lock, NULL)) {
		return -1;
	}
	if (pthread_cond_init(&ep->epoll_cond, NULL)) {
		return -1;
	}

	return epsocket->id;
}
/*----------------------------------------------------------------------------*/
int 
CloseEpollSocket(mctx_t mctx, int epid)
{
	mtcp_manager_t mtcp;
	struct mtcp_epoll *ep;

	mtcp = GetMTCPManager(mctx);
	if (!mtcp) {
		return -1;
	}

	ep = mtcp->smap[epid].ep;
	if (!ep) {
		errno = EINVAL;
		return -1;
	}

	DestroyEventQueue(ep->usr_queue);
	DestroyEventQueue(ep->usr_shadow_queue);
	DestroyEventQueue(ep->mtcp_queue);
	free(ep);

	pthread_mutex_lock(&ep->epoll_lock);
	mtcp->ep = NULL;
	mtcp->smap[epid].ep = NULL;
	pthread_cond_signal(&ep->epoll_cond);
	pthread_mutex_unlock(&ep->epoll_lock);

	pthread_cond_destroy(&ep->epoll_cond);
	pthread_mutex_destroy(&ep->epoll_lock);

	return 0;
}
/*----------------------------------------------------------------------------*/
// 将tcp-socket的事件加入到epoll事件队列中
static int 
RaisePendingStreamEvents(mtcp_manager_t mtcp, 
		struct mtcp_epoll *ep, socket_map_t socket)
{
	tcp_stream *stream = socket->stream;

	if (!stream)
		return -1;
	// 必须是已经建立连接的tcp-socket，如果需要支持udp，epoll接口需要修改
	if (stream->state < TCP_ST_ESTABLISHED)
		return -1;

	TRACE_EPOLL("Stream %d at state %s\n", 
			stream->id, TCPStateToString(stream));
	/* if there are payloads already read before epoll registration */
	/* generate read event */
	if (socket->epoll & MTCP_EPOLLIN) {
	// 注册读事件
		struct tcp_recv_vars *rcvvar = stream->rcvvar;
		if (rcvvar->rcvbuf && rcvvar->rcvbuf->merged_len > 0) {
			TRACE_EPOLL("Socket %d: Has existing payloads\n", socket->id);
			// 将指定socket的事件增加到epoll队列中
			AddEpollEvent(ep, USR_SHADOW_EVENT_QUEUE, socket, MTCP_EPOLLIN);
		} else if (stream->state == TCP_ST_CLOSE_WAIT) {
			TRACE_EPOLL("Socket %d: Waiting for close\n", socket->id);
			// 将指定socket的事件增加到epoll队列中
			AddEpollEvent(ep, USR_SHADOW_EVENT_QUEUE, socket, MTCP_EPOLLIN);
		}
	}

	/* same thing to the write event */
	if (socket->epoll & MTCP_EPOLLOUT) {
	// 注册写事件
		struct tcp_send_vars *sndvar = stream->sndvar;
		if (!sndvar->sndbuf || 
				(sndvar->sndbuf && sndvar->sndbuf->len < sndvar->snd_wnd)) {
			if (!(socket->events & MTCP_EPOLLOUT)) {
				TRACE_EPOLL("Socket %d: Adding write event\n", socket->id);
				AddEpollEvent(ep, USR_SHADOW_EVENT_QUEUE, socket, MTCP_EPOLLOUT);
			}
		}
	}

	return 0;
}
/*----------------------------------------------------------------------------*/
int 
mtcp_epoll_ctl(mctx_t mctx, int epid, 
		int op, int sockid, struct mtcp_epoll_event *event)
{
	mtcp_manager_t mtcp;
	struct mtcp_epoll *ep;
	socket_map_t socket;
	uint32_t events;

	mtcp = GetMTCPManager(mctx);
	if (!mtcp) {
		return -1;
	}

	if (epid < 0 || epid >= CONFIG.max_concurrency) {
		TRACE_API("Epoll id %d out of range.\n", epid);
		errno = EBADF;
		return -1;
	}

	if (sockid < 0 || sockid >= CONFIG.max_concurrency) {
		TRACE_API("Socket id %d out of range.\n", sockid);
		errno = EBADF;
		return -1;
	}

	// 检查epid对应的socket类型
	if (mtcp->smap[epid].socktype == MTCP_SOCK_UNUSED) {
		errno = EBADF;
		return -1;
	}

	if (mtcp->smap[epid].socktype != MTCP_SOCK_EPOLL) {
		errno = EINVAL;
		return -1;
	}

	// 获取到epoll管理对象
	ep = mtcp->smap[epid].ep;
	if (!ep || (!event && op != MTCP_EPOLL_CTL_DEL)) {
		errno = EINVAL;
		return -1;
	}
	// 根据sockid获取对应的socket对象
	socket = &mtcp->smap[sockid];

	if (op == MTCP_EPOLL_CTL_ADD) {
	// EPOLL_ADD处理
		if (socket->epoll) {
		// 如果已经注册了epoll事件，返回已经存在的错误
			errno = EEXIST;
			return -1;
		}

		/* EPOLLERR and EPOLLHUP are registered as default */
		// 注册这个socket的事件，及事件附加数据
		events = event->events;
		events |= (MTCP_EPOLLERR | MTCP_EPOLLHUP);
		socket->ep_data = event->data;
		socket->epoll = events;

		TRACE_EPOLL("Adding epoll socket %d(type %d) ET: %u, IN: %u, OUT: %u\n", 
				socket->id, socket->socktype, socket->epoll & MTCP_EPOLLET, 
				socket->epoll & MTCP_EPOLLIN, socket->epoll & MTCP_EPOLLOUT);

		if (socket->socktype == MTCP_SOCK_STREAM) {
		// socket流
			RaisePendingStreamEvents(mtcp, ep, socket);
		} else if (socket->socktype == MTCP_SOCK_PIPE) {
		// 管道
			RaisePendingPipeEvents(mctx, epid, sockid);
		}

	} else if (op == MTCP_EPOLL_CTL_MOD) {
	// EPOLL_MOD处理
		if (!socket->epoll) {
			pthread_mutex_unlock(&ep->epoll_lock);
			errno = ENOENT;
			return -1;
		}

		// 修改epoll事件的数据
		events = event->events;
		events |= (MTCP_EPOLLERR | MTCP_EPOLLHUP);
		socket->ep_data = event->data;
		socket->epoll = events;

		if (socket->socktype == MTCP_SOCK_STREAM) {
			RaisePendingStreamEvents(mtcp, ep, socket);
		} else if (socket->socktype == MTCP_SOCK_PIPE) {
			RaisePendingPipeEvents(mctx, epid, sockid);
		}

	} else if (op == MTCP_EPOLL_CTL_DEL) {
	// EPOLL_DEL处理
		if (!socket->epoll) {
			errno = ENOENT;
			return -1;
		}

		socket->epoll = MTCP_EPOLLNONE;
	}

	return 0;
}
/*----------------------------------------------------------------------------*/
// 等待epoll事件触发
int 
mtcp_epoll_wait(mctx_t mctx, int epid, 
		struct mtcp_epoll_event *events, int maxevents, int timeout)
{
	mtcp_manager_t mtcp;
	struct mtcp_epoll *ep;
	struct event_queue *eq;
	struct event_queue *eq_shadow;
	socket_map_t event_socket;
	int validity;
	int i, cnt, ret;
	int num_events;

	mtcp = GetMTCPManager(mctx);
	if (!mtcp) {
		return -1;
	}

	if (epid < 0 || epid >= CONFIG.max_concurrency) {
		TRACE_API("Epoll id %d out of range.\n", epid);
		errno = EBADF;
		return -1;
	}

	if (mtcp->smap[epid].socktype == MTCP_SOCK_UNUSED) {
		errno = EBADF;
		return -1;
	}

	if (mtcp->smap[epid].socktype != MTCP_SOCK_EPOLL) {
		errno = EINVAL;
		return -1;
	}

	ep = mtcp->smap[epid].ep;
	if (!ep || !events || maxevents <= 0) {
		errno = EINVAL;
		return -1;
	}

	ep->stat.calls++;

#if SPIN_BEFORE_SLEEP
	int spin = 0;
	while (ep->num_events == 0 && spin < SPIN_THRESH) {
		spin++;
	}
#endif /* SPIN_BEFORE_SLEEP */

	if (pthread_mutex_lock(&ep->epoll_lock)) {
		if (errno == EDEADLK)
			perror("mtcp_epoll_wait: epoll_lock blocked\n");
		assert(0);
	}

wait:
	eq = ep->usr_queue;
	eq_shadow = ep->usr_shadow_queue;

	/* wait until event occurs */
	// 没有事件触发但达到超时时间后也退出
	while (eq->num_events == 0 && eq_shadow->num_events == 0 && timeout != 0) {

#if INTR_SLEEPING_MTCP
		/* signal to mtcp thread if it is sleeping */
		if (mtcp->wakeup_flag && mtcp->is_sleeping) {
			pthread_kill(mtcp->ctx->thread, SIGUSR1);
		}
#endif
		ep->stat.waits++;
		ep->waiting = TRUE;
		if (timeout > 0) {
		// 有设置超时时间
			struct timespec deadline;		// 用当前时间 + timeout

			// 获取精确到纳秒级的当前时间
			clock_gettime(CLOCK_REALTIME, &deadline);
			if (timeout > 1000) {
				int sec;
				sec = timeout / 1000;
				deadline.tv_sec += sec;
				timeout -= sec * 1000;
			}

			if (deadline.tv_nsec >= 1000000000) {
				deadline.tv_sec++;
				deadline.tv_nsec -= 1000000000;
			}

			//deadline.tv_sec = mtcp->cur_tv.tv_sec;
			//deadline.tv_nsec = (mtcp->cur_tv.tv_usec + timeout * 1000) * 1000;
			// 阻塞直到事件触发，并且需要设置超时时间
			ret = pthread_cond_timedwait(&ep->epoll_cond, 
					&ep->epoll_lock, &deadline);
			if (ret && ret != ETIMEDOUT) {
				/* errno set by pthread_cond_timedwait() */
				pthread_mutex_unlock(&ep->epoll_lock);
				TRACE_ERROR("pthread_cond_timedwait failed. ret: %d, error: %s\n", 
						ret, strerror(errno));
				return -1;
			}
			timeout = 0;
		} else if (timeout < 0) {
		// 没有设置超时时间，无限制等待事件触发
			ret = pthread_cond_wait(&ep->epoll_cond, &ep->epoll_lock);
			if (ret) {
				/* errno set by pthread_cond_wait() */
				pthread_mutex_unlock(&ep->epoll_lock);
				TRACE_ERROR("pthread_cond_wait failed. ret: %d, error: %s\n", 
						ret, strerror(errno));
				return -1;
			}
		}
		ep->waiting = FALSE;

		if (mtcp->ctx->done || mtcp->ctx->exit || mtcp->ctx->interrupt) {
			mtcp->ctx->interrupt = FALSE;
			//ret = pthread_cond_signal(&ep->epoll_cond);
			pthread_mutex_unlock(&ep->epoll_lock);
			errno = EINTR;
			return -1;
		}
	
	}
	
	/* fetch events from the user event queue */
	cnt = 0;
	num_events = eq->num_events;
	// 从user event队列中取出所有events
	for (i = 0; i < num_events && cnt < maxevents; i++) {
		event_socket = &mtcp->smap[eq->events[eq->start].sockid];
		validity = TRUE;
		if (event_socket->socktype == MTCP_SOCK_UNUSED)
			validity = FALSE;
		if (!(event_socket->epoll & eq->events[eq->start].ev.events))
			validity = FALSE;
		if (!(event_socket->events & eq->events[eq->start].ev.events))
			validity = FALSE;

		if (validity) {
			// 加入到用户传入的events数组中
			events[cnt++] = eq->events[eq->start].ev;
			assert(eq->events[eq->start].sockid >= 0);

			TRACE_EPOLL("Socket %d: Handled event. event: %s, "
					"start: %u, end: %u, num: %u\n", 
					event_socket->id, 
					EventToString(eq->events[eq->start].ev.events), 
					eq->start, eq->end, eq->num_events);
			ep->stat.handled++;
		} else {
			TRACE_EPOLL("Socket %d: event %s invalidated.\n", 
					eq->events[eq->start].sockid, 
					EventToString(eq->events[eq->start].ev.events));
			ep->stat.invalidated++;
		}
		// 把socket对应的事件清除掉
		event_socket->events &= (~eq->events[eq->start].ev.events);

		// 相当于将这个event从队列中去除
		eq->start++;
		eq->num_events--;
		// 循环使用数组
		if (eq->start >= eq->size) {
			eq->start = 0;
		}
	}

	/* fetch eventes from user shadow event queue */
	eq = ep->usr_shadow_queue;
	num_events = eq->num_events;
	// 从user shadow event队列中取出所有events
	for (i = 0; i < num_events && cnt < maxevents; i++) {
		event_socket = &mtcp->smap[eq->events[eq->start].sockid];
		validity = TRUE;
		if (event_socket->socktype == MTCP_SOCK_UNUSED)
			validity = FALSE;
		if (!(event_socket->epoll & eq->events[eq->start].ev.events))
			validity = FALSE;
		if (!(event_socket->events & eq->events[eq->start].ev.events))
			validity = FALSE;

		if (validity) {
			events[cnt++] = eq->events[eq->start].ev;
			assert(eq->events[eq->start].sockid >= 0);

			TRACE_EPOLL("Socket %d: Handled event. event: %s, "
					"start: %u, end: %u, num: %u\n", 
					event_socket->id, 
					EventToString(eq->events[eq->start].ev.events), 
					eq->start, eq->end, eq->num_events);
			ep->stat.handled++;
		} else {
			TRACE_EPOLL("Socket %d: event %s invalidated.\n", 
					eq->events[eq->start].sockid, 
					EventToString(eq->events[eq->start].ev.events));
			ep->stat.invalidated++;
		}
		event_socket->events &= (~eq->events[eq->start].ev.events);

		eq->start++;
		eq->num_events--;
		if (eq->start >= eq->size) {
			eq->start = 0;
		}
	}

	if (cnt == 0 && timeout != 0)
		goto wait;

	pthread_mutex_unlock(&ep->epoll_lock);

	return cnt;
}
/*----------------------------------------------------------------------------*/
// 将指定socket的事件增加到epoll队列中
inline int 
AddEpollEvent(struct mtcp_epoll *ep, 
		int queue_type, socket_map_t socket, uint32_t event)
{
	struct event_queue *eq;
	int index;

	if (!ep || !socket || !event)
		return -1;
	
	ep->stat.issued++;

	// 如果event事件类型没有变化，不做处理
	if (socket->events & event) {
		return 0;
	}

	if (queue_type == MTCP_EVENT_QUEUE) {
		eq = ep->mtcp_queue;
	} else if (queue_type == USR_EVENT_QUEUE) {
		eq = ep->usr_queue;
		pthread_mutex_lock(&ep->epoll_lock);
	} else if (queue_type == USR_SHADOW_EVENT_QUEUE) {
		eq = ep->usr_shadow_queue;
	} else {
		TRACE_ERROR("Non-existing event queue type!\n");
		return -1;
	}

	// 上限检测
	if (eq->num_events >= eq->size) {
		TRACE_ERROR("Exceeded epoll event queue! num_events: %d, size: %d\n", 
				eq->num_events, eq->size);
		if (queue_type == USR_EVENT_QUEUE)
			pthread_mutex_unlock(&ep->epoll_lock);
		return -1;
	}

	// 将这个epoll事件加入到队列中
	index = eq->end++;

	// 触发了什么事件记录到socket对象中
	socket->events |= event;
	eq->events[index].sockid = socket->id;
	eq->events[index].ev.events = event;
	eq->events[index].ev.data = socket->ep_data;

	if (eq->end >= eq->size) {
		eq->end = 0;
	}
	eq->num_events++;

#if 0
	TRACE_EPOLL("Socket %d New event: %s, start: %u, end: %u, num: %u\n",
			ep->events[index].sockid, 
			EventToString(ep->events[index].ev.events), 
			ep->start, ep->end, ep->num_events);
#endif

	if (queue_type == USR_EVENT_QUEUE)
		pthread_mutex_unlock(&ep->epoll_lock);

	ep->stat.registered++;

	return 0;
}
