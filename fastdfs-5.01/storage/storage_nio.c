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
#include "logger.h"
#include "sockopt.h"
#include "fast_task_queue.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "storage_global.h"
#include "storage_service.h"
#include "ioevent_loop.h"
#include "storage_dio.h"
#include "storage_nio.h"

static void client_sock_read(int sock, short event, void *arg);
static void client_sock_write(int sock, short event, void *arg);
static int storage_nio_init(struct fast_task_info *pTask);

/* 将pTask加入到deleted_list中待删除 */
void add_to_deleted_list(struct fast_task_info *pTask)
{
	((StorageClientInfo *)pTask->arg)->canceled = true;
	pTask->next = pTask->thread_data->deleted_list;
	pTask->thread_data->deleted_list = pTask;
}

/* storage 任务结束的清理函数 */
void task_finish_clean_up(struct fast_task_info *pTask)
{
	StorageClientInfo *pClientInfo;

	pClientInfo = (StorageClientInfo *)pTask->arg;
	if (pClientInfo->clean_func != NULL)
	{
		pClientInfo->clean_func(pTask);
	}

	/* 在events集合中删除指定event */
	ioevent_detach(&pTask->thread_data->ev_puller, pTask->event.fd);
	close(pTask->event.fd);
	pTask->event.fd = -1;

	if (pTask->event.timer.expires > 0)
	{
		/* 删除时间轮的指定节点 */
		fast_timer_remove(&pTask->thread_data->timer,
			&pTask->event.timer);
		pTask->event.timer.expires = 0;
	}

	/* 将空闲的资源添加到内存池队列中 */
	memset(pTask->arg, 0, sizeof(StorageClientInfo));
	free_queue_push(pTask);
}

/* 设置pTask的回调函数为client_sock_read */
static int set_recv_event(struct fast_task_info *pTask)
{
	int result;

	/* 如果IO事件的回调函数已经是client_sock_read，直接返回 */
	if (pTask->event.callback == client_sock_read)
	{
		return 0;
	}

	/* 否则设置回调函数，并添加到IO事件集合中 */
	pTask->event.callback = client_sock_read;
	if (ioevent_modify(&pTask->thread_data->ev_puller,
		pTask->event.fd, IOEVENT_READ, pTask) != 0)
	{
		result = errno != 0 ? errno : ENOENT;
		/* 将pTask加入到deleted_list中待删除 */
		add_to_deleted_list(pTask);

		logError("file: "__FILE__", line: %d, "\
			"ioevent_modify fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}
	return 0;
}

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
		add_to_deleted_list(pTask);

		logError("file: "__FILE__", line: %d, "\
			"ioevent_modify fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}
	return 0;
}

/* IO事件就绪后的处理函数 */
void storage_recv_notify_read(int sock, short event, void *arg)
{
	struct fast_task_info *pTask;
	StorageClientInfo *pClientInfo;
	long task_addr;
	int64_t remain_bytes;
	int bytes;
	int result;

	while (1)
	{
		/* 接收到一个指向fast_task_info结构的指针 */
		if ((bytes=read(sock, &task_addr, sizeof(task_addr))) < 0)
		{
			if (!(errno == EAGAIN || errno == EWOULDBLOCK))
			{
				logError("file: "__FILE__", line: %d, " \
					"call read failed, " \
					"errno: %d, error info: %s", \
					__LINE__, errno, STRERROR(errno));
			}

			break;
		}
		else if (bytes == 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call read failed, end of file", __LINE__);
			break;
		}

		pTask = (struct fast_task_info *)task_addr;
		pClientInfo = (StorageClientInfo *)pTask->arg;

		if (pTask->event.fd < 0)  //quit flag
		{
			return;
		}

		/* //logInfo("=====thread index: %d, pTask->event.fd=%d", \
			pClientInfo->nio_thread_index, pTask->event.fd);
		*/

		if (pClientInfo->stage & FDFS_STORAGE_STAGE_DIO_THREAD)
		{
			pClientInfo->stage &= ~FDFS_STORAGE_STAGE_DIO_THREAD;
		}
		switch (pClientInfo->stage)
		{
			/* 根据fast_task_info初始化，监听其socket的可读事件 */
			case FDFS_STORAGE_STAGE_NIO_INIT:
				result = storage_nio_init(pTask);
				break;
			/* 接收报文的操作 */
			case FDFS_STORAGE_STAGE_NIO_RECV:
				pTask->offset = 0;
				/* 获取要剩余要接收的报文长度 */
				remain_bytes = pClientInfo->total_length - \
					       pClientInfo->total_offset;
				if (remain_bytes > pTask->size)
				{
					pTask->length = pTask->size;
				}
				else
				{
					pTask->length = remain_bytes;
				}

				/* 设置pTask的回调函数为client_sock_read */
				if (set_recv_event(pTask) == 0)
				{
					client_sock_read(pTask->event.fd,
					IOEVENT_READ, pTask);
				}
				result = 0;
				break;
			/* 发送报文的操作 */
			case FDFS_STORAGE_STAGE_NIO_SEND:
				/* 
				 * 本来是将当前发送报文的任务加入到IO事件集合中
				 * 但是这里只是直接向socket端口发送报文
				 */
				result = storage_send_add_event(pTask);
				break;
			/* 结束socket的操作 */
			case FDFS_STORAGE_STAGE_NIO_CLOSE:
				result = EIO;   //close this socket
				break;
			default:
				logError("file: "__FILE__", line: %d, " \
					"invalid stage: %d", __LINE__, \
					pClientInfo->stage);
				result = EINVAL;
				break;
		}

		if (result != 0)
		{
			add_to_deleted_list(pTask);
		}
	}
}

/* 根据fast_task_info初始化，监听其socket的可读事件 */
static int storage_nio_init(struct fast_task_info *pTask)
{
	StorageClientInfo *pClientInfo;
	struct storage_nio_thread_data *pThreadData;

	pClientInfo = (StorageClientInfo *)pTask->arg;
	pThreadData = g_nio_thread_data + pClientInfo->nio_thread_index;

	pClientInfo->stage = FDFS_STORAGE_STAGE_NIO_RECV;
	return ioevent_set(pTask, &pThreadData->thread_data,
			pTask->event.fd, IOEVENT_READ, client_sock_read,
			g_fdfs_network_timeout);
}

/* 
 * 本来是将当前发送报文的任务加入到IO事件集合中
 * 但是这里只是直接向socket端口发送报文
 */
int storage_send_add_event(struct fast_task_info *pTask)
{
	pTask->offset = 0;

	/* direct send */
	/* 向sock发送报文 */
	client_sock_write(pTask->event.fd, IOEVENT_WRITE, pTask);

	return 0;
}

/* 读取从客户端发送过来的消息 */
static void client_sock_read(int sock, short event, void *arg)
{
	int bytes;
	int recv_bytes;
	struct fast_task_info *pTask;
        StorageClientInfo *pClientInfo;

	pTask = (struct fast_task_info *)arg;
        pClientInfo = (StorageClientInfo *)pTask->arg;
	if (pClientInfo->canceled)
	{
		return;
	}

	if (pClientInfo->stage != FDFS_STORAGE_STAGE_NIO_RECV)
	{
		/* 如果事件超时，增加超时时间 */
		if (event & IOEVENT_TIMEOUT) {
			pTask->event.timer.expires = g_current_time +
				g_fdfs_network_timeout;
			fast_timer_add(&pTask->thread_data->timer,
				&pTask->event.timer);
		}

		return;
	}

	/* 如果recv超时 */
	if (event & IOEVENT_TIMEOUT)
	{
		/* 还没有请求，增加超时时间 */
		if (pClientInfo->total_offset == 0 && pTask->req_count > 0)
		{
			pTask->event.timer.expires = g_current_time +
				g_fdfs_network_timeout;
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

	if (event & IOEVENT_ERROR)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, recv error event: %d, "
			"close connection", __LINE__, pTask->client_ip, event);

		task_finish_clean_up(pTask);
		return;
	}

	/* 设置新的超时时间 */
	fast_timer_modify(&pTask->thread_data->timer,
		&pTask->event.timer, g_current_time +
		g_fdfs_network_timeout);

	/* 循环接收数据，存放在pTask->data中 */
	while (1)
	{
		if (pClientInfo->total_length == 0)   /* recv header(先获取报文头) */
		{
			recv_bytes = sizeof(TrackerHeader) - pTask->offset;
		}
		else
		{
			recv_bytes = pTask->length - pTask->offset;
		}

		/*
		logInfo("total_length="INT64_PRINTF_FORMAT", recv_bytes=%d, "
			"pTask->length=%d, pTask->offset=%d",
			pClientInfo->total_length, recv_bytes, 
			pTask->length, pTask->offset);
		*/

		bytes = recv(sock, pTask->data + pTask->offset, recv_bytes, 0);
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
		else if (bytes == 0)
		{
			logDebug("file: "__FILE__", line: %d, " \
				"client ip: %s, recv failed, " \
				"connection disconnected.", \
				__LINE__, pTask->client_ip);

			task_finish_clean_up(pTask);
			return;
		}

		/* 如果是获取报文头，解析出报文体长度 */
		if (pClientInfo->total_length == 0)  //header
		{
			if (pTask->offset + bytes < sizeof(TrackerHeader))
			{
				pTask->offset += bytes;
				return;
			}

			pClientInfo->total_length=buff2long(((TrackerHeader *) \
						pTask->data)->pkg_len);
			if (pClientInfo->total_length < 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, pkg length: " \
					INT64_PRINTF_FORMAT" < 0", \
					__LINE__, pTask->client_ip, \
					pClientInfo->total_length);

				task_finish_clean_up(pTask);
				return;
			}

			/* total_length为整个报文长度 */
			pClientInfo->total_length += sizeof(TrackerHeader);
			if (pClientInfo->total_length > pTask->size)
			{
				pTask->length = pTask->size;
			}
			else
			{
				pTask->length = pClientInfo->total_length;
			}
		}

		pTask->offset += bytes;

		/* 全部数据读取完成后 */
		if (pTask->offset >= pTask->length) //recv current pkg done
		{
			if (pClientInfo->total_offset + pTask->length >= \
					pClientInfo->total_length)
			{
				/* current req recv done */
				/* 接收完成，将事件改为发送 */
				pClientInfo->stage = FDFS_STORAGE_STAGE_NIO_SEND;
				pTask->req_count++;
			}

			/* 之前这是第一次读到内容 */
			if (pClientInfo->total_offset == 0)
			{
				pClientInfo->total_offset = pTask->length;
				/* 处理请求报文 */
				storage_deal_task(pTask);
			}
			else
			{
				pClientInfo->total_offset += pTask->length;

				/* continue write to file */
				storage_dio_queue_push(pTask);
			}

			return;
		}
	}

	return;
}

/* 向sock发送报文 */
static void client_sock_write(int sock, short event, void *arg)
{
	int bytes;
	struct fast_task_info *pTask;
        StorageClientInfo *pClientInfo;

	pTask = (struct fast_task_info *)arg;
        pClientInfo = (StorageClientInfo *)pTask->arg;
	if (pClientInfo->canceled)
	{
		return;
	}

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
			/* 设置pTask的回调函数为client_sock_read */
			if (set_recv_event(pTask) != 0)
			{
				return;
			}

			pClientInfo->total_offset += pTask->length;
			if (pClientInfo->total_offset>=pClientInfo->total_length)
			{
				if (pClientInfo->total_length == sizeof(TrackerHeader)
					&& ((TrackerHeader *)pTask->data)->status == EINVAL)
				{
					logDebug("file: "__FILE__", line: %d, "\
						"close conn: #%d, client ip: %s", \
						__LINE__, pTask->event.fd,
						pTask->client_ip);
					task_finish_clean_up(pTask);
					return;
				}

				/*  reponse done, try to recv again */
				pClientInfo->total_length = 0;
				pClientInfo->total_offset = 0;
				pTask->offset = 0;
				pTask->length = 0;

				pClientInfo->stage = FDFS_STORAGE_STAGE_NIO_RECV;
			}
			else  //continue to send file content
			{
				pTask->length = 0;

				/* continue read from file */
				/* 
				 * 将此任务加入到任务队列中并通知对应的读写线程进行处理 
				 * 从磁盘获取文件内容，通过socket发出去
				 */
				storage_dio_queue_push(pTask);
			}

			return;
		}
	}
}

