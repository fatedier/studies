#include "mtcp.h"
#include "socket.h"
#include "debug.h"

/*---------------------------------------------------------------------------*/
// 创建一个新的socket对象
socket_map_t 
AllocateSocket(mctx_t mctx, int socktype, int need_lock)
{
	mtcp_manager_t mtcp = g_mtcp[mctx->cpu];
	socket_map_t socket = NULL;

	if (need_lock)
		pthread_mutex_lock(&mtcp->ctx->smap_lock);

	// 循环直到获取到一个新的socket对象
	while (socket == NULL) {
		// 从空间的socket对象队列中获取一个空的socket对象
		socket = TAILQ_FIRST(&mtcp->free_smap);
		if (!socket) {
			if (need_lock)
				pthread_mutex_unlock(&mtcp->ctx->smap_lock);

			TRACE_ERROR("The concurrent sockets are at maximum.\n");
			return NULL;
		}

		TAILQ_REMOVE(&mtcp->free_smap, socket, free_smap_link);

		/* if there is not invalidated events, insert the socket to the end */
		/* and find another socket in the free smap list */
		// 如果仍然有有效的事件，重新放入队列尾部，再去获取另外一个
		if (socket->events) {
			TRACE_INFO("There are still not invalidate events remaining.\n");
			TRACE_DBG("There are still not invalidate events remaining.\n");
			TAILQ_INSERT_TAIL(&mtcp->free_smap, socket, free_smap_link);
			socket = NULL;
		}
	}

	if (need_lock)
		pthread_mutex_unlock(&mtcp->ctx->smap_lock);
	
	socket->socktype = socktype;
	socket->opts = 0;
	socket->stream = NULL;
	socket->epoll = 0;
	socket->events = 0;

	//memset(&socket->saddr, 0, sizeof(struct sockaddr_in));
	memset(&socket->ep_data, 0, sizeof(mtcp_epoll_data_t));

	return socket;
}
/*---------------------------------------------------------------------------*/
// 释放socket对象，重新加入到free_list
void 
FreeSocket(mctx_t mctx, int sockid, int need_lock)
{
	mtcp_manager_t mtcp = g_mtcp[mctx->cpu];
	socket_map_t socket = &mtcp->smap[sockid];

	if (socket->socktype == MTCP_SOCK_UNUSED) {
		return;
	}
	
	socket->socktype = MTCP_SOCK_UNUSED;
	socket->epoll = MTCP_EPOLLNONE;

	if (need_lock)
		pthread_mutex_lock(&mtcp->ctx->smap_lock);

	/* insert into free stream map */
	mtcp->smap[sockid].stream = NULL;
	TAILQ_INSERT_TAIL(&mtcp->free_smap, socket, free_smap_link);

	if (need_lock)
		pthread_mutex_unlock(&mtcp->ctx->smap_lock);
}
/*---------------------------------------------------------------------------*/
socket_map_t 
GetSocket(mctx_t mctx, int sockid)
{
	if (sockid < 0 || sockid >= CONFIG.max_concurrency) {
		errno = EBADF;
		return NULL;
	}

	return &g_mtcp[mctx->cpu]->smap[sockid];
}
