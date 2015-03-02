/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include "shared_func.h"
#include "sched_thread.h"
#include "fdfs_global.h"
#include "logger.h"
#include "sockopt.h"
#include "fast_task_queue.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "tracker_mem.h"
#include "tracker_global.h"
#include "tracker_service.h"
#include "ioevent_loop.h"
#include "tracker_nio.h"

static void client_sock_read(int sock, short event, void *arg);
static void client_sock_write(int sock, short event, void *arg);

/* 任务结束后的清理函数 */
void task_finish_clean_up(struct fast_task_info *pTask)
{
	TrackerClientInfo *pClientInfo;

	pClientInfo = (TrackerClientInfo *)pTask->arg;

	/* 如果有注册回调函数，调用相应回调函数 */
	if (pTask->finish_callback != NULL)
	{
		pTask->finish_callback(pTask);
		pTask->finish_callback = NULL;
	}

	if (pClientInfo->pGroup != NULL)
	{
		if (pClientInfo->pStorage != NULL)
		{
			/* 将指定的storage设为offline状态 */
			tracker_mem_offline_store_server(pClientInfo->pGroup, \
						pClientInfo->pStorage);
		}
	}

	/* 将此任务从IO事件集合中删除 */
	ioevent_detach(&pTask->thread_data->ev_puller, pTask->event.fd);
	/* 关闭socket */
	close(pTask->event.fd);

	pTask->event.fd = -1;

	/* 如果设置了超时时间，从时间轮中删除此事件 */
	if (pTask->event.timer.expires > 0)
	{
		fast_timer_remove(&pTask->thread_data->timer,
			&pTask->event.timer);
		pTask->event.timer.expires = 0;
	}

	/* 初始化pTask->arg对象 */
	memset(pTask->arg, 0, sizeof(TrackerClientInfo));
	/* 将可重新使用的pTask对象加入到内存池的队列中 */
	free_queue_push(pTask);
}

/* 
 * 每个线程的管道pipe_fds[0]的READ事件的回调函数 
 * 每次读取一个int变量，是新建立连接的socket描述符，之后加入到IO事件集合
 * 按通道分发到相应工作线程中等待可读事件触发后调用client_sock_read函数进行处理
 */
void recv_notify_read(int sock, short event, void *arg)
{
	int bytes;
	int incomesock;
	struct nio_thread_data *pThreadData;
	struct fast_task_info *pTask;
	char szClientIp[IP_ADDRESS_SIZE];		/* ip地址的字符串形势 */
	in_addr_t client_addr;		/* ip地址 */

	while (1)
	{
		/* 获取新建立连接的socket描述符 */
		if ((bytes=read(sock, &incomesock, sizeof(incomesock))) < 0)
		{
			/* 在VxWorks和Windows上，EAGAIN的名字叫做EWOULDBLOCK，表示没有数据可读 */
			if (!(errno == EAGAIN || errno == EWOULDBLOCK))
			{
				logError("file: "__FILE__", line: %d, " \
					"call read failed, " \
					"errno: %d, error info: %s", \
					__LINE__, errno, STRERROR(errno));
			}

			break;
		}
		/* read返回0表示已经读完，无可读数据 */
		else if (bytes == 0)
		{
			break;
		}

		/* 传入的socket描述符小于0，忽略，直接返回 */
		if (incomesock < 0)
		{
			return;
		}

		/* 根据socket描述符获取ip地址 */
		client_addr = getPeerIpaddr(incomesock, \
				szClientIp, IP_ADDRESS_SIZE);
		if (g_allow_ip_count >= 0)
		{
			/* 
			 * 在g_allow_ip_addrs数组中查找此ip地址
			 * 如果没有找到，不允许连接，关闭socket描述符 
			 */
			if (bsearch(&client_addr, g_allow_ip_addrs, \
					g_allow_ip_count, sizeof(in_addr_t), \
					cmp_by_ip_addr_t) == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"ip addr %s is not allowed to access", \
					__LINE__, szClientIp);

				close(incomesock);
				continue;
			}
		}

		/* 设置socket为非阻塞模式 */
		if (tcpsetnonblockopt(incomesock) != 0)
		{
			close(incomesock);
			continue;
		}

		/* 从内存池的队列中获取一个fast_task_info对象空间 */
		pTask = free_queue_pop();
		if (pTask == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc task buff failed, you should " \
				"increase the parameter: max_connections", \
				__LINE__);
			close(incomesock);
			continue;
		}

		strcpy(pTask->client_ip, szClientIp);

		/* 根据socket描述符分通道分发到各个工作线程中去处理 */
		pThreadData = g_thread_data + incomesock % g_work_threads;
		/* 
		 * 将incomesock描述符加入到监听集合中，等待触发READ事件 
		 * incomesock处于可读状态后，会调用client_sock_read回调函数进行处理
		 */
		if (ioevent_set(pTask, pThreadData, incomesock, IOEVENT_READ,
			client_sock_read, g_fdfs_network_timeout) != 0)
		{
			/* 任务结束后的清理工作 */
			task_finish_clean_up(pTask);
			continue;
		}
	}
}

/* 将IO事件的触发条件改为socket可写 */
static int set_send_event(struct fast_task_info *pTask)
{
	int result;

	if (pTask->event.callback == client_sock_write)
	{
		return 0;
	}

	pTask->event.callback = client_sock_write;
	if (ioevent_modify(&pTask->thread_data->ev_puller,
		pTask->event.fd, IOEVENT_WRITE, pTask) != 0)
	{
		result = errno != 0 ? errno : ENOENT;
		task_finish_clean_up(pTask);

		logError("file: "__FILE__", line: %d, "\
			"ioevent_modify fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}
	return 0;
}

/* 将发送报文的事件加入到IO事件集合中 */
int send_add_event(struct fast_task_info *pTask)
{
	pTask->offset = 0;

	/* direct send */
	/* 这里直接往pTask->event.fd发送报文 */
	client_sock_write(pTask->event.fd, IOEVENT_WRITE, pTask);
	return 0;
}

/* 读取从客户端发送过来的消息 */
static void client_sock_read(int sock, short event, void *arg)
{
	int bytes;
	int recv_bytes;
	struct fast_task_info *pTask;

	pTask = (struct fast_task_info *)arg;

	/* 如果是需要设置事件的超时时间 */
	if (event & IOEVENT_TIMEOUT)
	{
		if (pTask->offset == 0 && pTask->req_count > 0)
		{
			/* 设置事件超时时间 */
			pTask->event.timer.expires = g_current_time +
				g_fdfs_network_timeout;
			/* 将此事件加入时间轮中 */
			fast_timer_add(&pTask->thread_data->timer,
				&pTask->event.timer);
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv timeout, " \
				"recv offset: %d, expect length: %d", \
				__LINE__, pTask->client_ip, \
				pTask->offset, pTask->length);

			task_finish_clean_up(pTask);
		}

		return;
	}

	/* 如果是出错事件 */
	if (event & IOEVENT_ERROR)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, recv error event: %d, "
			"close connection", __LINE__, pTask->client_ip, event);

		task_finish_clean_up(pTask);
		return;
	}

	while (1)
	{
		/* 更新超时时间 */
		fast_timer_modify(&pTask->thread_data->timer,
			&pTask->event.timer, g_current_time +
			g_fdfs_network_timeout);

		/* 读取报文头 */
		if (pTask->length == 0) //recv header
		{
			/* 之前有可能已经读取了部分内容，再读取的时候减去偏移量 */
			recv_bytes = sizeof(TrackerHeader) - pTask->offset;
		}
		/* 读取报文体 */
		else
		{
			recv_bytes = pTask->length - pTask->offset;
		}

		/* 将读取到的报文头和报文体放入pTask->data中 */
		bytes = recv(sock, pTask->data + pTask->offset, recv_bytes, 0);
		/* 没有读取到数据 */
		if (bytes < 0)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK)
			{
			}
			else
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, recv failed, " \
					"errno: %d, error info: %s", \
					__LINE__, pTask->client_ip, \
					errno, STRERROR(errno));

				task_finish_clean_up(pTask);
			}

			return;
		}
		/* 网络连接中断 */
		else if (bytes == 0)
		{
			logDebug("file: "__FILE__", line: %d, " \
				"client ip: %s, recv failed, " \
				"connection disconnected.", \
				__LINE__, pTask->client_ip);

			task_finish_clean_up(pTask);
			return;
		}

		/* 解析报文头 */
		if (pTask->length == 0) //header
		{
			/* 如果已读取的内容加上新读到的内容长度不足，直接返回 */
			if (pTask->offset + bytes < sizeof(TrackerHeader))
			{
				pTask->offset += bytes;
				return;
			}

			/* 获取报文体长度 */
			/* 将用于网络中传输的字符串还原为64位整型 */
			pTask->length = buff2long(((TrackerHeader *) \
						pTask->data)->pkg_len);
			/* 长度解析错误 */
			if (pTask->length < 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, pkg length: %d < 0", \
					__LINE__, pTask->client_ip, \
					pTask->length);

				task_finish_clean_up(pTask);
				return;
			}

			/* 总长度为报文头长度加上报文体长度 */
			pTask->length += sizeof(TrackerHeader);
			/* 如果总长度超过最大允许长度，结束任务处理 */
			if (pTask->length > TRACKER_MAX_PACKAGE_SIZE)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, pkg length: %d > " \
					"max pkg size: %d", __LINE__, \
					pTask->client_ip, pTask->length, \
					TRACKER_MAX_PACKAGE_SIZE);

				task_finish_clean_up(pTask);
				return;
			}
		}

		/* 增加偏移量 */
		pTask->offset += bytes;
		if (pTask->offset >= pTask->length) //recv done
		{
			/* 请求数增加 */
			pTask->req_count++;
			/* 具体的tracker任务处理 */
			tracker_deal_task(pTask);
			return;
		}
	}

	return;
}

/* 向sock发送报文 */
static void client_sock_write(int sock, short event, void *arg)
{
	int bytes;
	int result;
	struct fast_task_info *pTask;

	pTask = (struct fast_task_info *)arg;
	/* 如果事件超时，直接结束任务，清理 */
	if (event & IOEVENT_TIMEOUT)
	{
		logError("file: "__FILE__", line: %d, " \
			"send timeout", __LINE__);

		task_finish_clean_up(pTask);

		return;
	}

	/* 接收数据失败 */
	if (event & IOEVENT_ERROR)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, recv error event: %d, "
			"close connection", __LINE__, pTask->client_ip, event);

		task_finish_clean_up(pTask);
		return;
	}

	while (1)
	{
		/* 修改指定节点的过期时间 */
		fast_timer_modify(&pTask->thread_data->timer,
			&pTask->event.timer, g_current_time +
			g_fdfs_network_timeout);

		bytes = send(sock, pTask->data + pTask->offset, \
				pTask->length - pTask->offset,  0);
		//printf("%08X sended %d bytes\n", (int)pTask, bytes);
		/* 如果没有发送成功 */
		if (bytes < 0)
		{	
			/* 继续加入IO事件集合中，等待重新发送 */
			if (errno == EAGAIN || errno == EWOULDBLOCK)
			{
				/* 将IO事件的触发条件改为socket可写 */
				set_send_event(pTask);
			}
			else
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, recv failed, " \
					"errno: %d, error info: %s", \
					__LINE__, pTask->client_ip, \
					errno, STRERROR(errno));

				task_finish_clean_up(pTask);
			}

			return;
		}
		/* 连接断开 */
		else if (bytes == 0)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"send failed, connection disconnected.", \
				__LINE__);

			task_finish_clean_up(pTask);
			return;
		}

		/* 发送了部分字节，修改偏移量，下次继续从偏移量处开始发送 */
		pTask->offset += bytes;
		/* 如果发送完成 */
		if (pTask->offset >= pTask->length)
		{
			if (pTask->length == sizeof(TrackerHeader) && \
				((TrackerHeader *)pTask->data)->status == EINVAL)
			{
				logDebug("file: "__FILE__", line: %d, "\
					"close conn: #%d, client ip: %s", \
					__LINE__, pTask->event.fd,
					pTask->client_ip);
				task_finish_clean_up(pTask);
				return;
			}

			pTask->offset = 0;
			pTask->length  = 0;

			/* 将IO事件的触发条件改为socket可读 */
			pTask->event.callback = client_sock_read;
			if (ioevent_modify(&pTask->thread_data->ev_puller,
				pTask->event.fd, IOEVENT_READ, pTask) != 0)
			{
				result = errno != 0 ? errno : ENOENT;
				task_finish_clean_up(pTask);

				logError("file: "__FILE__", line: %d, "\
					"ioevent_modify fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, STRERROR(result));
				return;
			}

			return;
		}
	}
}

