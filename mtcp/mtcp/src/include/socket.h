#ifndef __SOCKET_H_
#define __SOCKET_H_

#include "mtcp_api.h"
#include "mtcp_epoll.h"

/*----------------------------------------------------------------------------*/
// socket选项
enum socket_opts
{
	MTCP_NONBLOCK		= 0x01,
	MTCP_ADDR_BIND		= 0x02, 
};
/*----------------------------------------------------------------------------*/
// socket对象
struct socket_map
{
	int id;						// 相当于fd
	int socktype;				// socket类型
	uint32_t opts;				// socket选项

	struct sockaddr_in saddr;	// socket绑定的本地地址

	union {
		struct tcp_stream *stream;
		struct tcp_listener *listener;
		struct mtcp_epoll *ep;
		struct pipe *pp;
	};		// 根据socktype不同，链接到不同的管理对象，例如epoll的话就是mtcp_eppoll对象

	uint32_t epoll;			/* registered events */		// 用户注册的epoll事件
	uint32_t events;		/* available events */		// 有效的事件
	mtcp_epoll_data_t ep_data;							// 对应的event_data

	TAILQ_ENTRY (socket_map) free_smap_link;

};
/*----------------------------------------------------------------------------*/
typedef struct socket_map * socket_map_t;
/*----------------------------------------------------------------------------*/
socket_map_t 
AllocateSocket(mctx_t mctx, int socktype, int need_lock);
/*----------------------------------------------------------------------------*/
void 
FreeSocket(mctx_t mctx, int sockid, int need_lock); 
/*----------------------------------------------------------------------------*/
socket_map_t 
GetSocket(mctx_t mctx, int sockid);
/*----------------------------------------------------------------------------*/
// tcp连接对象
struct tcp_listener
{
	int sockid;					// 监听套接字的fd
	socket_map_t socket;		// socket对象

	int backlog;				// backlog数
	stream_queue_t acceptq;		// accept队列
	
	pthread_mutex_t accept_lock;
	pthread_cond_t accept_cond;
};
/*----------------------------------------------------------------------------*/

#endif /* __SOCKET_H_ */
