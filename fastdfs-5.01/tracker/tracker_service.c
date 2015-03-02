/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_service.c

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include "fdfs_define.h"
#include "base64.h"
#include "logger.h"
#include "fdfs_global.h"
#include "sockopt.h"
#include "shared_func.h"
#include "pthread_func.h"
#include "sched_thread.h"
#include "tracker_types.h"
#include "tracker_global.h"
#include "tracker_mem.h"
#include "tracker_func.h"
#include "tracker_proto.h"
#include "tracker_nio.h"
#include "tracker_relationship.h"
#include "fdfs_shared_func.h"
#include "ioevent_loop.h"
#include "tracker_service.h"

#define PKG_LEN_PRINTF_FORMAT  "%d"

static pthread_mutex_t tracker_thread_lock;		/* 线程锁，主要是针对g_tracker_thread_count对象的操作 */
static pthread_mutex_t lb_thread_lock;

int g_tracker_thread_count = 0;				/* tracker工作线程数 */
struct nio_thread_data *g_thread_data = NULL;	/* 存放全局工作线程数据的数组 */

static int lock_by_client_count = 0;

static void *work_thread_entrance(void* arg);
static void wait_for_work_threads_exit();
static void tracker_find_max_free_space_group();

/* tracker服务的初始化 */
int tracker_service_init()
{
	int result;
	struct nio_thread_data *pThreadData;
	struct nio_thread_data *pDataEnd;
	pthread_t tid;
	pthread_attr_t thread_attr;

	/* 初始化互斥锁 */
	if ((result=init_pthread_lock(&tracker_thread_lock)) != 0)
	{
		return result;
	}

	if ((result=init_pthread_lock(&lb_thread_lock)) != 0)
	{
		return result;
	}

	/* 初始化线程参数选项，线程栈大小，是否是分离状态等 */
	if ((result=init_pthread_attr(&thread_attr, g_thread_stack_size)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_attr fail, program exit!", __LINE__);
		return result;
	}

	/* 内存池任务队列初始化 */
	if ((result=free_queue_init(g_max_connections, TRACKER_MAX_PACKAGE_SIZE,\
                TRACKER_MAX_PACKAGE_SIZE, sizeof(TrackerClientInfo))) != 0)
	{
		return result;
	}

	/* 为tracker工作线程数据分配空间(数组) */
	g_thread_data = (struct nio_thread_data *)malloc(sizeof( \
				struct nio_thread_data) * g_work_threads);
	if (g_thread_data == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, errno: %d, error info: %s", \
			__LINE__, (int)sizeof(struct nio_thread_data) * \
			g_work_threads, errno, STRERROR(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	g_tracker_thread_count = 0;
	pDataEnd = g_thread_data + g_work_threads;

	/* 初始化每一个工作线程数据 */
	for (pThreadData=g_thread_data; pThreadData<pDataEnd; pThreadData++)
	{
		/* 初始化IO事件调度对象 */
		if (ioevent_init(&pThreadData->ev_puller,
			g_max_connections + 2, 1000, 0) != 0)
		{
			result  = errno != 0 ? errno : ENOMEM;
			logError("file: "__FILE__", line: %d, " \
				"ioevent_init fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, STRERROR(result));
			return result;
		}

		/* 
		 * 初始化时间轮对象 
		 * slot数为2 * g_fdfs_network_timeout
		 */
		result = fast_timer_init(&pThreadData->timer,
				2 * g_fdfs_network_timeout, g_current_time);
		if (result != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"fast_timer_init fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, STRERROR(result));
			return result;
		}

		/* 创建管道，pipe_fds[0]为管道读取端，pipe_fds[1]为管道写入端 */
		if (pipe(pThreadData->pipe_fds) != 0)
		{
			result = errno != 0 ? errno : EPERM;
			logError("file: "__FILE__", line: %d, " \
				"call pipe fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, STRERROR(result));
			break;
		}

#if defined(OS_LINUX)
		/* 增加文件描述符为非阻塞并且读文件时不更新最后访问时间 */
		if ((result=fd_add_flags(pThreadData->pipe_fds[0], \
				O_NONBLOCK | O_NOATIME)) != 0)
		{
			break;
		}
#else
		if ((result=fd_add_flags(pThreadData->pipe_fds[0], \
				O_NONBLOCK)) != 0)
		{
			break;
		}
#endif

		/* 创建工作线程 */
		if ((result=pthread_create(&tid, &thread_attr, \
			work_thread_entrance, pThreadData)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"create thread failed, startup threads: %d, " \
				"errno: %d, error info: %s", \
				__LINE__, g_tracker_thread_count, \
				result, STRERROR(result));
			break;
		}
		else
		{
			if ((result=pthread_mutex_lock(&tracker_thread_lock)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_lock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, STRERROR(result));
			}
			/* 工作线程数加1 */
			g_tracker_thread_count++;
			if ((result=pthread_mutex_unlock(&tracker_thread_lock)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_lock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, STRERROR(result));
			}
		}
	}

	pthread_attr_destroy(&thread_attr);

	return 0;
}

/* 终止所有的工作线程，向每个线程的pipe[1]管道发送小于0的socket描述符 */
int tracker_terminate_threads()
{
        struct nio_thread_data *pThreadData;
        struct nio_thread_data *pDataEnd;
        int quit_sock;

        if (g_thread_data != NULL)
        {
                pDataEnd = g_thread_data + g_work_threads;
                quit_sock = 0;
                for (pThreadData=g_thread_data; pThreadData<pDataEnd; \
                        pThreadData++)
                {
                        quit_sock--;
                        if (write(pThreadData->pipe_fds[1], &quit_sock, \
                                        sizeof(quit_sock)) != sizeof(quit_sock))
			{
				logError("file: "__FILE__", line: %d, " \
					"write to pipe fail, " \
					"errno: %d, error info: %s", \
					__LINE__, errno, STRERROR(errno));
			}
                }
        }

        return 0;
}

/* 等待所有工作线程结束 */
static void wait_for_work_threads_exit()
{
	while (g_tracker_thread_count != 0)
	{
		sleep(1);
	}
}

/* 销毁tracker_service资源 */
int tracker_service_destroy()
{
	/* 等待所有工作线程结束 */
	wait_for_work_threads_exit();
	pthread_mutex_destroy(&tracker_thread_lock);
	pthread_mutex_destroy(&lb_thread_lock);

	return 0;
}

/* accept线程主函数 */
static void *accept_thread_entrance(void* arg)
{
	int server_sock;
	int incomesock;
	struct sockaddr_in inaddr;
	socklen_t sockaddr_len;
	struct nio_thread_data *pThreadData;

	server_sock = (long)arg;
	while (g_continue_flag)
	{
		sockaddr_len = sizeof(inaddr);
		incomesock = accept(server_sock, (struct sockaddr*)&inaddr, &sockaddr_len);
		if (incomesock < 0) //error
		{
			if (!(errno == EINTR || errno == EAGAIN))
			{
				logError("file: "__FILE__", line: %d, " \
					"accept failed, " \
					"errno: %d, error info: %s", \
					__LINE__, errno, STRERROR(errno));
			}

			continue;
		}

		/* 根据sockfd按通道选择相应的工作线程 */
		pThreadData = g_thread_data + incomesock % g_work_threads;
		/* 将建立连接的sockfd发送到相应工作线程的pipe[1]管道 */
		if (write(pThreadData->pipe_fds[1], &incomesock, \
			sizeof(incomesock)) != sizeof(incomesock))
		{
			close(incomesock);
			logError("file: "__FILE__", line: %d, " \
				"call write failed, " \
				"errno: %d, error info: %s", \
				__LINE__, errno, STRERROR(errno));
		}
	}

	return NULL;
}

/* 
 * 循环accept，主进程会阻塞在一个accept中
 * 如果g_accept_threads>1，会通过多线程来同时accept 
 */
void tracker_accept_loop(int server_sock)
{
	if (g_accept_threads > 1)
	{
		pthread_t tid;
		pthread_attr_t thread_attr;
		int result;
		int i;

		if ((result=init_pthread_attr(&thread_attr, g_thread_stack_size)) != 0)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"init_pthread_attr fail!", __LINE__);
		}
		else
		{
			/* 可以有多个线程同时accept */
			for (i=1; i<g_accept_threads; i++)
			{
				if ((result=pthread_create(&tid, &thread_attr, \
					accept_thread_entrance, \
					(void *)(long)server_sock)) != 0)
				{
					logError("file: "__FILE__", line: %d, " \
					"create thread failed, startup threads: %d, " \
					"errno: %d, error info: %s", \
					__LINE__, i, result, STRERROR(result));
					break;
				}
			}

			pthread_attr_destroy(&thread_attr);
		}
	}

	/* 主进程会阻塞在这个函数的accept上 */
	accept_thread_entrance((void *)(long)server_sock);
}

/* tracker工作线程入口函数 */
static void *work_thread_entrance(void* arg)
{
	int result;
	struct nio_thread_data *pThreadData;

	/* 全局的工作线程数据通过参数arg指针传入 */
	pThreadData = (struct nio_thread_data *)arg;

	/* 
	 * 循环等待IO事件触发，调用回调函数进行处理 
	 * pThreadData是线程自己的数据
	 * recv_notify_read是每个线程的管道pipe_fds[0]的READ事件的回调函数
	 * task_finish_clean_up是需要任务结束的IO事件的回调函数
	 * g_continue_flag 控制该循环是否结束
	 */
	ioevent_loop(pThreadData, recv_notify_read, task_finish_clean_up,
		&g_continue_flag);

	/* 循环结束后销毁IO事件调度对象相关资源，包括关闭文件描述符 */
	ioevent_destroy(&pThreadData->ev_puller);

	if ((result=pthread_mutex_lock(&tracker_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}
	/* 工作线程数减1 */
	g_tracker_thread_count--;
	if ((result=pthread_mutex_unlock(&tracker_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	return NULL;
}

/*
 * storage server list
 * 检查相应的改变状态，并将其同步
 */
static int tracker_check_and_sync(struct fast_task_info *pTask, \
			const int status)
{
	FDFSStorageDetail **ppServer;
	FDFSStorageDetail **ppEnd;
	FDFSStorageDetail *pServer;
	FDFSStorageBrief *pDestServer;
	TrackerClientInfo *pClientInfo;
	char *pFlags;
	char *p;

	pClientInfo = (TrackerClientInfo *)pTask->arg;
	if (status != 0 || pClientInfo->pGroup == NULL)
	{
		pTask->length = sizeof(TrackerHeader);
		return status;
	}
	
	p = pTask->data + sizeof(TrackerHeader);
	pFlags = p++;
	*pFlags = 0;
	/* 如果自己是tracker leader */
	if (g_if_leader_self)
	{
	if (pClientInfo->chg_count.tracker_leader != g_tracker_leader_chg_count)
	{
		int leader_index;

		*pFlags |= FDFS_CHANGE_FLAG_TRACKER_LEADER;

		pDestServer = (FDFSStorageBrief *)p;
		memset(p, 0, sizeof(FDFSStorageBrief));

		leader_index = g_tracker_servers.leader_index;
		if (leader_index >= 0)
		{
			ConnectionInfo *pTServer;
			pTServer = g_tracker_servers.servers + leader_index;
			snprintf(pDestServer->id, FDFS_STORAGE_ID_MAX_SIZE, \
				"%s", pTServer->ip_addr);
			memcpy(pDestServer->ip_addr, pTServer->ip_addr, \
				IP_ADDRESS_SIZE);
			int2buff(pTServer->port, pDestServer->port);
		}
		pDestServer++;

		pClientInfo->chg_count.tracker_leader = \
				g_tracker_leader_chg_count;
		p = (char *)pDestServer;
	}

	if (pClientInfo->pStorage->trunk_chg_count != \
		pClientInfo->pGroup->trunk_chg_count)
	{
		*pFlags |= FDFS_CHANGE_FLAG_TRUNK_SERVER;

		pDestServer = (FDFSStorageBrief *)p;
		memset(p, 0, sizeof(FDFSStorageBrief));

		pServer = pClientInfo->pGroup->pTrunkServer;
		if (pServer != NULL)
		{
			pDestServer->status = pServer->status;
			memcpy(pDestServer->id, pServer->id, \
				FDFS_STORAGE_ID_MAX_SIZE);
			memcpy(pDestServer->ip_addr, pServer->ip_addr, \
				IP_ADDRESS_SIZE);
			int2buff(pClientInfo->pGroup->storage_port, \
				pDestServer->port);
		}
		pDestServer++;

		pClientInfo->pStorage->trunk_chg_count = \
			pClientInfo->pGroup->trunk_chg_count;
		p = (char *)pDestServer;
	}
	}

	if (pClientInfo->pStorage->chg_count != pClientInfo->pGroup->chg_count)
	{
		*pFlags |= FDFS_CHANGE_FLAG_GROUP_SERVER;

		pDestServer = (FDFSStorageBrief *)p;
		ppEnd = pClientInfo->pGroup->sorted_servers + \
				pClientInfo->pGroup->count;
		for (ppServer=pClientInfo->pGroup->sorted_servers; \
			ppServer<ppEnd; ppServer++)
		{
			pDestServer->status = (*ppServer)->status;
			memcpy(pDestServer->id, (*ppServer)->id, \
				FDFS_STORAGE_ID_MAX_SIZE);
			memcpy(pDestServer->ip_addr, (*ppServer)->ip_addr, \
				IP_ADDRESS_SIZE);
			int2buff(pClientInfo->pGroup->storage_port, \
				pDestServer->port);
			pDestServer++;
		}

		pClientInfo->pStorage->chg_count = \
			pClientInfo->pGroup->chg_count;
		p = (char *)pDestServer;
	}

	pTask->length = p - pTask->data;
	return status;
}

/* 
 * 获取changelog文件中的内容，因为一个包可能传输不完，根据changelog_offset分多次传输 
 * 文件内容存放在pTask->data中
 */
static int tracker_changelog_response(struct fast_task_info *pTask, \
		FDFSStorageDetail *pStorage)
{
	char filename[MAX_PATH_SIZE];
	int64_t changelog_fsize;
	int read_bytes;
	int chg_len;
	int result;
	int fd;

	changelog_fsize = g_changelog_fsize;
	chg_len = changelog_fsize - pStorage->changelog_offset;
	if (chg_len < 0)
	{
		chg_len = 0;
	}

	if (chg_len == 0)
	{
		pTask->length = sizeof(TrackerHeader);
		return 0;
	}

	if (chg_len > sizeof(TrackerHeader) + TRACKER_MAX_PACKAGE_SIZE)
	{
		chg_len = TRACKER_MAX_PACKAGE_SIZE - sizeof(TrackerHeader);
	}

	snprintf(filename, sizeof(filename), "%s/data/%s", g_fdfs_base_path,\
		 STORAGE_SERVERS_CHANGELOG_FILENAME);
	fd = open(filename, O_RDONLY);
	if (fd < 0)
	{
		result = errno != 0 ? errno : EACCES;
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, open changelog file %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pTask->client_ip, \
			filename, result, STRERROR(result));
		pTask->length = sizeof(TrackerHeader);
		return result;
	}

	/* 定位到STORAGE_SERVERS_CHANGELOG_FILENAME文件的changelog_offset位置 */
	if (pStorage->changelog_offset > 0 && \
		lseek(fd, pStorage->changelog_offset, SEEK_SET) < 0)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, lseek changelog file %s fail, "\
			"errno: %d, error info: %s", \
			__LINE__, pTask->client_ip, \
			filename, result, STRERROR(result));
		close(fd);
		pTask->length = sizeof(TrackerHeader);
		return result;
	}

	/* 读取chg_len长度的字节 */
	read_bytes = read(fd, pTask->data + sizeof(TrackerHeader), chg_len);
	close(fd);

	if (read_bytes != chg_len)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, read changelog file %s fail, "\
			"errno: %d, error info: %s", \
			__LINE__, pTask->client_ip, \
			filename, result, STRERROR(result));

		close(fd);
		pTask->length = sizeof(TrackerHeader);
		return result;
	}

	pStorage->changelog_offset += chg_len;
	tracker_save_storages();

	pTask->length = sizeof(TrackerHeader) + chg_len;
	return 0;
}

/*
 * 获取changelog文件的内容
 * 发送报文:空或者group_name+storage_id
 * 返回报文:changelog文件中的一段，由pTask->storage的偏移量来决定
 */
static int tracker_deal_changelog_req(struct fast_task_info *pTask)
{
	int result;
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char *storage_id;
	FDFSGroupInfo *pGroup;
	FDFSStorageDetail *pStorage;
	TrackerClientInfo *pClientInfo;
	
	pClientInfo = (TrackerClientInfo *)pTask->arg;
	pStorage = NULL;

	do
	{
		/* 已经加入此storage */
		if (pClientInfo->pGroup != NULL && pClientInfo->pStorage != NULL)
		{  //already logined
			if (pTask->length - sizeof(TrackerHeader) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"cmd=%d, client ip: %s, package size " \
					PKG_LEN_PRINTF_FORMAT" is not correct, " \
					"expect length = %d", __LINE__, \
					TRACKER_PROTO_CMD_STORAGE_CHANGELOG_REQ, \
					pTask->client_ip, pTask->length - \
					(int)sizeof(TrackerHeader), 0);

				result = EINVAL;
				break;
			}

			pStorage = pClientInfo->pStorage;
			result = 0;
		}
		/* 此storage还没有join */
		else
		{
			if (pTask->length - sizeof(TrackerHeader) != \
				FDFS_GROUP_NAME_MAX_LEN + FDFS_STORAGE_ID_MAX_SIZE)
			{
				logError("file: "__FILE__", line: %d, " \
					"cmd=%d, client ip: %s, package size " \
					PKG_LEN_PRINTF_FORMAT" is not correct, " \
					"expect length = %d", __LINE__, \
					TRACKER_PROTO_CMD_STORAGE_CHANGELOG_REQ, \
					pTask->client_ip, pTask->length - \
					(int)sizeof(TrackerHeader), \
					FDFS_GROUP_NAME_MAX_LEN + \
					FDFS_STORAGE_ID_MAX_SIZE);

				result = EINVAL;
				break;
			}

			/* 解析出group_name */
			memcpy(group_name, pTask->data + sizeof(TrackerHeader), \
				FDFS_GROUP_NAME_MAX_LEN);
			*(group_name + FDFS_GROUP_NAME_MAX_LEN) = '\0';
			pGroup = tracker_mem_get_group(group_name);
			if (pGroup == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, invalid group_name: %s", \
					__LINE__, pTask->client_ip, group_name);
				result = ENOENT;
				break;
			}

			/* 解析出storage_id */
			storage_id = pTask->data + sizeof(TrackerHeader) + \
					FDFS_GROUP_NAME_MAX_LEN;
			*(storage_id + FDFS_STORAGE_ID_MAX_SIZE - 1) = '\0';
			pStorage = tracker_mem_get_storage(pGroup, storage_id);
			if (pStorage == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, group_name: %s, " \
					"storage server: %s not exist", \
					__LINE__, pTask->client_ip, \
					group_name, storage_id);
				result = ENOENT;
				break;
			}
			
			result = 0;
		}
	} while (0);

	if (result != 0)
	{
		pTask->length = sizeof(TrackerHeader);
		return result;
	}

	 /* 
	  * 获取changelog文件中的内容
	  * 因为一个包可能传输不完，根据changelog_offset分多次传输 
	  */
	return tracker_changelog_response(pTask, pStorage);
}

/*
 * 获取当前group的trunk_file_id
 * 发送报文:无
 * 返回报文:int型的trunk_file_id
 */
static int tracker_deal_get_trunk_fid(struct fast_task_info *pTask)
{
	TrackerClientInfo *pClientInfo;
	
	pClientInfo = (TrackerClientInfo *)pTask->arg;
	if (pTask->length - sizeof(TrackerHeader) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length = %d", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_FETCH_TRUNK_FID, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader), 0);

		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->length = sizeof(TrackerHeader) + sizeof(int);
	int2buff(pClientInfo->pGroup->current_trunk_file_id, \
		pTask->data + sizeof(TrackerHeader));

	return 0;
}

/*
 * 获取一些配置的参数
 * 发送报文:无
 * 返回报文:每一行表示一条配置信息
 */
static int tracker_deal_parameter_req(struct fast_task_info *pTask)
{
	char reserved_space_str[32];

	if (pTask->length - sizeof(TrackerHeader) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length = %d", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_PARAMETER_REQ, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader), 0);

		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}
	
	pTask->length = sizeof(TrackerHeader) + \
	sprintf(pTask->data + sizeof(TrackerHeader), \
		"use_storage_id=%d\n" \
		"id_type_in_filename=%s\n" \
		"storage_ip_changed_auto_adjust=%d\n" \
		"storage_sync_file_max_delay=%d\n" \
		"store_path=%d\n" \
		"reserved_storage_space=%s\n" \
		"use_trunk_file=%d\n" \
		"slot_min_size=%d\n" \
		"slot_max_size=%d\n" \
		"trunk_file_size=%d\n" \
		"trunk_create_file_advance=%d\n" \
		"trunk_create_file_time_base=%02d:%02d\n" \
		"trunk_create_file_interval=%d\n" \
		"trunk_create_file_space_threshold="INT64_PRINTF_FORMAT"\n" \
		"trunk_init_check_occupying=%d\n"     \
		"trunk_init_reload_from_binlog=%d\n"  \
		"trunk_compress_binlog_min_interval=%d\n"  \
		"store_slave_file_use_link=%d\n",    \
		g_use_storage_id, g_id_type_in_filename == \
    FDFS_ID_TYPE_SERVER_ID ? "id" : "ip", \
    g_storage_ip_changed_auto_adjust, \
		g_storage_sync_file_max_delay, g_groups.store_path, \
		fdfs_storage_reserved_space_to_string( \
			&g_storage_reserved_space, reserved_space_str), \
		g_if_use_trunk_file, \
		g_slot_min_size, g_slot_max_size, \
		g_trunk_file_size, g_trunk_create_file_advance, \
		g_trunk_create_file_time_base.hour, \
		g_trunk_create_file_time_base.minute, \
		g_trunk_create_file_interval, \
		g_trunk_create_file_space_threshold, \
		g_trunk_init_check_occupying, \
		g_trunk_init_reload_from_binlog, \
		g_trunk_compress_binlog_min_interval, \
		g_store_slave_file_use_link);

	return 0;
}

/*
 * 更新多个storage_server的状态信息
 * 报文体:多个FDFSStorageBrief对象
 */
static int tracker_deal_storage_replica_chg(struct fast_task_info *pTask)
{
	int server_count;
	FDFSStorageBrief *briefServers;
	int nPkgLen;

	nPkgLen = pTask->length - sizeof(TrackerHeader);
	if ((nPkgLen <= 0) || (nPkgLen % sizeof(FDFSStorageBrief) != 0))
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip addr: %s, " \
			"package size %d is not correct", \
			__LINE__, TRACKER_PROTO_CMD_STORAGE_REPLICA_CHG, \
			pTask->client_ip, nPkgLen);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	server_count = nPkgLen / sizeof(FDFSStorageBrief);
	if (server_count > FDFS_MAX_SERVERS_EACH_GROUP)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip addr: %s, return storage count: %d" \
			" exceed max: %d", __LINE__, \
			pTask->client_ip, server_count, \
			FDFS_MAX_SERVERS_EACH_GROUP);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->length = sizeof(TrackerHeader);
	briefServers = (FDFSStorageBrief *)(pTask->data + sizeof(TrackerHeader));
	/* 同步storage server的状态信息 */
	return tracker_mem_sync_storages(((TrackerClientInfo *)pTask->arg)->pGroup, \
				briefServers, server_count);
}

/*
 * 汇报当前group的trunk_file_id
 * 发送报文:int型的trunk_file_id
 * 返回报文:无
 */
static int tracker_deal_report_trunk_fid(struct fast_task_info *pTask)
{
	int current_trunk_fid;
	TrackerClientInfo *pClientInfo;
	
	pClientInfo = (TrackerClientInfo *)pTask->arg;

	if (pTask->length - sizeof(TrackerHeader) != sizeof(int))
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip addr: %s, " \
			"package size "PKG_LEN_PRINTF_FORMAT" " \
			"is not correct", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_REPORT_TRUNK_FID, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader));
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->length = sizeof(TrackerHeader);
	/* 解析出trunk_file_id */
	current_trunk_fid = buff2int(pTask->data + sizeof(TrackerHeader));
	if (current_trunk_fid < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid current trunk file id: %d", \
			__LINE__, pTask->client_ip, current_trunk_fid);
		return EINVAL;
	}

	/* 如果当前连接的storage不是trunk_server ，报错 */
	if (pClientInfo->pStorage != pClientInfo->pGroup->pTrunkServer)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, i am not the trunk server", \
			__LINE__, pTask->client_ip);
		return EINVAL;
	}

	/* 更新当前group的trunk_file_id */
	if (pClientInfo->pGroup->current_trunk_file_id < current_trunk_fid)
	{
		pClientInfo->pGroup->current_trunk_file_id = current_trunk_fid;
		return tracker_save_groups();
	}
	else
	{
		return 0;
	}
}

/*
 * 汇报当前group的trunk_server的空闲空间
 * 发送报文:8个字节的trunk的空闲空间
 * 返回报文:无
 */
static int tracker_deal_report_trunk_free_space(struct fast_task_info *pTask)
{
	int64_t trunk_free_space;
	TrackerClientInfo *pClientInfo;
	
	pClientInfo = (TrackerClientInfo *)pTask->arg;
	if (pTask->length - sizeof(TrackerHeader) != 8)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip addr: %s, " \
			"package size "PKG_LEN_PRINTF_FORMAT" " \
			"is not correct", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_REPORT_TRUNK_FREE, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader));
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->length = sizeof(TrackerHeader);
	trunk_free_space = buff2long(pTask->data + sizeof(TrackerHeader));
	if (trunk_free_space < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid trunk free space: " \
			INT64_PRINTF_FORMAT, __LINE__, pTask->client_ip, \
			trunk_free_space);
		return EINVAL;
	}

	if (pClientInfo->pStorage != pClientInfo->pGroup->pTrunkServer)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, i am not the trunk server", \
			__LINE__, pTask->client_ip);
		return EINVAL;
	}

	pClientInfo->pGroup->trunk_free_mb = trunk_free_space;
	/* trunk_server空闲空间变更，重新选择上传文件的group */
	tracker_find_max_free_space_group();
	return 0;
}

/*
 * 指定下一个leader_server
 * 发送报文:ip地址+端口号
 * 返回报文:无
 */
static int tracker_deal_notify_next_leader(struct fast_task_info *pTask)
{
	char *pIpAndPort;
	char *ipAndPort[2];
	ConnectionInfo leader;
	int server_index;
	
	if (pTask->length - sizeof(TrackerHeader) != FDFS_PROTO_IP_PORT_SIZE)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip addr: %s, " \
			"package size "PKG_LEN_PRINTF_FORMAT" " \
			"is not correct, expect length: %d", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_REPORT_TRUNK_FID, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader), FDFS_PROTO_IP_PORT_SIZE);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	*(pTask->data + pTask->length) = '\0';
	pIpAndPort = pTask->data + sizeof(TrackerHeader);
	if (splitEx(pIpAndPort, ':', ipAndPort, 2) != 2)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid ip and port: %s", \
			__LINE__, pTask->client_ip, pIpAndPort);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->length = sizeof(TrackerHeader);
	/* 解析出ip地址 */
	strcpy(leader.ip_addr, ipAndPort[0]);
	/* 解析出端口号 */
	leader.port = atoi(ipAndPort[1]);

	/* 在所有的tracker server中查找指定ip:port的tracker的index */
	server_index = fdfs_get_tracker_leader_index_ex(&g_tracker_servers, \
					leader.ip_addr, leader.port);
	if (server_index < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, leader %s:%d not exist", \
			__LINE__, pTask->client_ip, \
			leader.ip_addr, leader.port);
		return ENOENT;
	}

	if (g_if_leader_self && (leader.port != g_server_port || \
		!is_local_host_ip(leader.ip_addr)))
	{
		g_if_leader_self = false;
		g_tracker_servers.leader_index = -1;
		g_tracker_leader_chg_count++;

		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, two leader occur, " \
			"new leader is %s:%d", \
			__LINE__, pTask->client_ip, \
			leader.ip_addr, leader.port);
		return EINVAL;
	}

	/* 设置下一个leader_server的索引 */
	g_next_leader_index = server_index;
	return 0;
}

/*
 * 设置接收到报文中的ip:port对应的主机为tracker_leader_server
 * 发送报文:ip地址+端口号
 * 返回报文:无
 */
static int tracker_deal_commit_next_leader(struct fast_task_info *pTask)
{
	char *pIpAndPort;
	char *ipAndPort[2];
	ConnectionInfo leader;
	int server_index;
	
	if (pTask->length - sizeof(TrackerHeader) != FDFS_PROTO_IP_PORT_SIZE)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip addr: %s, " \
			"package size "PKG_LEN_PRINTF_FORMAT" " \
			"is not correct, expect length: %d", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_REPORT_TRUNK_FID, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader), FDFS_PROTO_IP_PORT_SIZE);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	*(pTask->data + pTask->length) = '\0';
	pIpAndPort = pTask->data + sizeof(TrackerHeader);
	if (splitEx(pIpAndPort, ':', ipAndPort, 2) != 2)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid ip and port: %s", \
			__LINE__, pTask->client_ip, pIpAndPort);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	/* 解析出ip地址及端口号 */
	pTask->length = sizeof(TrackerHeader);
	strcpy(leader.ip_addr, ipAndPort[0]);
	leader.port = atoi(ipAndPort[1]);

	/* 在所有的tracker server中查找指定ip:port的tracker的index */
	server_index = fdfs_get_tracker_leader_index_ex(&g_tracker_servers, \
					leader.ip_addr, leader.port);
	if (server_index < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, leader %s:%d not exist", \
			__LINE__, pTask->client_ip, \
			leader.ip_addr, leader.port);
		return ENOENT;
	}
	if (server_index != g_next_leader_index)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, can't commit leader %s:%d", \
			__LINE__, pTask->client_ip, \
			leader.ip_addr, leader.port);
		return EINVAL;
	}

	/* 设置当前的tracker_leader_server */
	g_tracker_servers.leader_index = server_index;
	
	if (leader.port == g_server_port && is_local_host_ip(leader.ip_addr))
	{
		g_if_leader_self = true;
		g_tracker_leader_chg_count++;
	}
	else
	{
		logInfo("file: "__FILE__", line: %d, " \
			"the tracker leader is %s:%d", __LINE__, \
			leader.ip_addr, leader.port);
	}

	return 0;
}

/*
 * 获取指定的group_name中的storage的状态信息
 * 报文体:group_name，ip地址(可有可无，如果没传，从当前连接的客户端取ip)
 * 返回报文体:要查询的storage的FDFSStorageBrief对象信息
 */
static int tracker_deal_server_get_storage_status(struct fast_task_info *pTask)
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char ip_addr[IP_ADDRESS_SIZE];
	FDFSGroupInfo *pGroup;
	FDFSStorageDetail *pStorage;
	FDFSStorageBrief *pDest;
	int nPkgLen;

	/* 检查报文长度 */
	nPkgLen = pTask->length - sizeof(TrackerHeader);
	if (nPkgLen < FDFS_GROUP_NAME_MAX_LEN)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip addr: %s, " \
			"package size %d is not correct", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_GET_STATUS, \
			pTask->client_ip, nPkgLen);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	/* 获取group_name */
	memcpy(group_name, pTask->data + sizeof(TrackerHeader), \
			FDFS_GROUP_NAME_MAX_LEN);
	*(group_name + FDFS_GROUP_NAME_MAX_LEN) = '\0';
	
	/* 根据group_name获取group信息，如果不存在，返回NULL */
	pGroup = tracker_mem_get_group(group_name);
	if (pGroup == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_name: %s", \
			__LINE__, pTask->client_ip, group_name);
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	/* 如果只传了一个group_name过来，取连接中的客户端的ip地址 */
	if (nPkgLen == FDFS_GROUP_NAME_MAX_LEN)
	{
		strcpy(ip_addr, pTask->client_ip);
	}
	/* 否则从报文中获取ip地址 */
	else
	{
		int ip_len;

		ip_len = nPkgLen - FDFS_GROUP_NAME_MAX_LEN;
		if (ip_len >= IP_ADDRESS_SIZE)
		{
			ip_len = IP_ADDRESS_SIZE - 1;
		}
		memcpy(ip_addr, pTask->data + sizeof(TrackerHeader) + \
			FDFS_GROUP_NAME_MAX_LEN, ip_len);
		*(ip_addr + ip_len) = '\0';
	}

	/* 根据ip地址，获取storage对象 */
	pStorage = tracker_mem_get_storage_by_ip(pGroup, ip_addr);
	if (pStorage == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, group_name: %s, ip_addr: %s, " \
			"storage server not exist", __LINE__, \
			pTask->client_ip, group_name, ip_addr);
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	/* 
	 * 拼返回报文 
	 * 报文体为要查询的storage的FDFSStorageBrief对象信息
	 */
	pTask->length = sizeof(TrackerHeader) + sizeof(FDFSStorageBrief);
	pDest = (FDFSStorageBrief *)(pTask->data + sizeof(TrackerHeader));
	memset(pDest, 0, sizeof(FDFSStorageBrief));
	strcpy(pDest->id, pStorage->id);
	strcpy(pDest->ip_addr, pStorage->ip_addr);
	pDest->status = pStorage->status;
	int2buff(pGroup->storage_port, pDest->port);

	return 0;
}

/*
 * 获取指定的group_name中的storage的id
 * 报文体:group_name，ip地址(可有可无，如果没传，从当前连接的客户端取ip)
 * 返回报文体:对应的id值
 */
static int tracker_deal_get_storage_id(struct fast_task_info *pTask)
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char ip_addr[IP_ADDRESS_SIZE];
	FDFSStorageIdInfo *pFDFSStorageIdInfo;
	char *storage_id;
	int nPkgLen;
	int id_len;

	nPkgLen = pTask->length - sizeof(TrackerHeader);
	if (nPkgLen < FDFS_GROUP_NAME_MAX_LEN)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip addr: %s, " \
			"package size %d is not correct", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_GET_SERVER_ID, \
			pTask->client_ip, nPkgLen);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	memcpy(group_name, pTask->data + sizeof(TrackerHeader), \
			FDFS_GROUP_NAME_MAX_LEN);
	*(group_name + FDFS_GROUP_NAME_MAX_LEN) = '\0';

	if (nPkgLen == FDFS_GROUP_NAME_MAX_LEN)
	{
		strcpy(ip_addr, pTask->client_ip);
	}
	else
	{
		int ip_len;

		ip_len = nPkgLen - FDFS_GROUP_NAME_MAX_LEN;
		if (ip_len >= IP_ADDRESS_SIZE)
		{
			ip_len = IP_ADDRESS_SIZE - 1;
		}
		memcpy(ip_addr, pTask->data + sizeof(TrackerHeader) + \
			FDFS_GROUP_NAME_MAX_LEN, ip_len);
		*(ip_addr + ip_len) = '\0';
	}

	if (g_use_storage_id)
	{
		pFDFSStorageIdInfo = fdfs_get_storage_id_by_ip(group_name, \
						ip_addr);
		if (pFDFSStorageIdInfo == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip addr: %s, " \
				"group_name: %s, storage ip: %s not exist", \
				__LINE__, TRACKER_PROTO_CMD_STORAGE_GET_SERVER_ID, \
				pTask->client_ip, group_name, ip_addr);
			pTask->length = sizeof(TrackerHeader);
			return ENOENT;
		}

		storage_id = pFDFSStorageIdInfo->id;
	}
	else
	{
		storage_id = ip_addr;
	}

	id_len = strlen(storage_id);
	pTask->length = sizeof(TrackerHeader) + id_len; 
	memcpy(pTask->data + sizeof(TrackerHeader), storage_id, id_len);

	return 0;
}

/*
 * 获取从start_index开始的所有storage的ids信息
 * 报文体:int型的start_index
 * 返回报文体:int型的storage ids总数+查询到的ids总数+ %s %s %s\n 格式的id,group_name,ip信息
 */
static int tracker_deal_fetch_storage_ids(struct fast_task_info *pTask)
{
	FDFSStorageIdInfo *pIdsStart;
	FDFSStorageIdInfo *pIdsEnd;
	FDFSStorageIdInfo *pIdInfo;
	char *p;
	int *pCurrentCount;
	int nPkgLen;
	int start_index;

	if (!g_use_storage_id)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip addr: %s, operation not supported", \
			__LINE__, pTask->client_ip);
		pTask->length = sizeof(TrackerHeader);
		return EOPNOTSUPP;
	}

	nPkgLen = pTask->length - sizeof(TrackerHeader);
	if (nPkgLen != sizeof(int))
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip addr: %s, " \
			"package size %d is not correct, " \
			"expect %d bytes", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_FETCH_STORAGE_IDS, \
			pTask->client_ip, nPkgLen, (int)sizeof(int));
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	start_index = buff2int(pTask->data + sizeof(TrackerHeader));
	if (start_index < 0 || start_index >= g_storage_id_count)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip addr: %s, invalid offset: %d", \
			__LINE__, pTask->client_ip, start_index);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	p = pTask->data + sizeof(TrackerHeader);
	int2buff(g_storage_id_count, p);
	p += sizeof(int);
	pCurrentCount = (int *)p;
	p += sizeof(int);

	pIdsStart = g_storage_ids_by_ip + start_index;
	pIdsEnd = g_storage_ids_by_ip + g_storage_id_count;
	for (pIdInfo = pIdsStart; pIdInfo < pIdsEnd; pIdInfo++)
	{
		if ((int)(p - pTask->data) > pTask->size - 64)
		{
			break;
		}

		p += sprintf(p, "%s %s %s\n", pIdInfo->id, \
			pIdInfo->group_name, pIdInfo->ip_addr);
	}

	int2buff((int)(pIdInfo - pIdsStart), (char *)pCurrentCount);
	pTask->length = p - pTask->data;

	return 0;
}

/* 
 * storage汇报状态信息的报文处理 
 * 报文体:group_name，之后是一个FDFSStorageBrief结构的报文
 * 返回报文:无
 */
static int tracker_deal_storage_report_status(struct fast_task_info *pTask)
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	FDFSGroupInfo *pGroup;
	FDFSStorageBrief *briefServers;

	/* 检查报文长度 */
	if (pTask->length - sizeof(TrackerHeader) != FDFS_GROUP_NAME_MAX_LEN + \
			sizeof(FDFSStorageBrief))
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip addr: %s, " \
			"package size "PKG_LEN_PRINTF_FORMAT" " \
			"is not correct", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_REPORT_STATUS, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader));
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	/* 获取group_name */
	memcpy(group_name, pTask->data + sizeof(TrackerHeader), \
			FDFS_GROUP_NAME_MAX_LEN);
	*(group_name + FDFS_GROUP_NAME_MAX_LEN) = '\0';
	
	/* 根据group_name获取group信息，如果不存在，返回NULL */
	pGroup = tracker_mem_get_group(group_name);
	if (pGroup == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_name: %s", \
			__LINE__, pTask->client_ip, group_name);
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	pTask->length = sizeof(TrackerHeader);
	/* 获取发送报文过来的storage的简要信息 */
	briefServers = (FDFSStorageBrief *)(pTask->data + \
			sizeof(TrackerHeader) + FDFS_GROUP_NAME_MAX_LEN);

	/* 同步storage server的状态信息 */
	return tracker_mem_sync_storages(pGroup, briefServers, 1);
}

/* 处理storage向tracker发送的join请求报文 */
static int tracker_deal_storage_join(struct fast_task_info *pTask)
{
	TrackerStorageJoinBodyResp *pJoinBodyResp;
	TrackerStorageJoinBody *pBody;
	ConnectionInfo *pTrackerServer;
	ConnectionInfo *pTrackerEnd;
	char *p;
	char *pSeperator;
	FDFSStorageJoinBody joinBody;
	int result;
	TrackerClientInfo *pClientInfo;
	char tracker_ip[IP_ADDRESS_SIZE];

	pClientInfo = (TrackerClientInfo *)pTask->arg;

	/* 检查报文体长度，如果不满足一个TrackerStorageJoinBody结构的长度，显然错误 */
	if (pTask->length - sizeof(TrackerHeader) < \
			sizeof(TrackerStorageJoinBody))
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd: %d, client ip: %s, " \
			"package size "PKG_LEN_PRINTF_FORMAT" " \
			"is not correct, expect length >= %d.", \
			__LINE__, TRACKER_PROTO_CMD_STORAGE_JOIN, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader),
			(int)sizeof(TrackerStorageJoinBody));
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pBody = (TrackerStorageJoinBody *)(pTask->data + sizeof(TrackerHeader));

	/* 获取tracker server总数，并检查是否超出限制或者数量错误 */
	joinBody.tracker_count = buff2long(pBody->tracker_count);
	if (joinBody.tracker_count <= 0 || \
		joinBody.tracker_count > FDFS_MAX_TRACKERS)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd: %d, client ip: %s, " \
			"tracker_count: %d is invalid, it <= 0 or > %d", \
			__LINE__, TRACKER_PROTO_CMD_STORAGE_JOIN, \
			pTask->client_ip, joinBody.tracker_count, \
			FDFS_MAX_TRACKERS);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	/* 确定tracker_count之后再次检查报文长度 */
	if (pTask->length - sizeof(TrackerHeader) != \
		sizeof(TrackerStorageJoinBody) + joinBody.tracker_count *\
		FDFS_PROTO_IP_PORT_SIZE)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd: %d, client ip: %s, " \
			"package size "PKG_LEN_PRINTF_FORMAT" " \
			"is not correct, expect length %d.", \
			__LINE__, TRACKER_PROTO_CMD_STORAGE_JOIN, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader),
			(int)sizeof(TrackerStorageJoinBody) + \
			joinBody.tracker_count * FDFS_PROTO_IP_PORT_SIZE);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	/* 获取storage所在group的name */
	memcpy(joinBody.group_name, pBody->group_name, FDFS_GROUP_NAME_MAX_LEN);
	joinBody.group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
	/* 检查组名是否符合要求，需为数字或英文字母 */
	if ((result=fdfs_validate_group_name(joinBody.group_name)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_name: %s", \
			__LINE__, pTask->client_ip, \
			joinBody.group_name);
		pTask->length = sizeof(TrackerHeader);
		return result;
	}

	/* 获取storage用于通信的port */
	joinBody.storage_port = (int)buff2long(pBody->storage_port);
	if (joinBody.storage_port <= 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid port: %d", \
			__LINE__, pTask->client_ip, \
			joinBody.storage_port);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	/* 获取http_port */
	joinBody.storage_http_port = (int)buff2long(pBody->storage_http_port);
	if (joinBody.storage_http_port < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid http port: %d", \
			__LINE__, pTask->client_ip, \
			joinBody.storage_http_port);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	/* 获取store_path_count */
	joinBody.store_path_count = (int)buff2long(pBody->store_path_count);
	if (joinBody.store_path_count <= 0 || joinBody.store_path_count > 256)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid store_path_count: %d", \
			__LINE__, pTask->client_ip, \
			joinBody.store_path_count);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	/* 获取二级目录的个数 */
	joinBody.subdir_count_per_path = (int)buff2long( \
					pBody->subdir_count_per_path);
	if (joinBody.subdir_count_per_path <= 0 || \
	    joinBody.subdir_count_per_path > 256)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid subdir_count_per_path: %d", \
			__LINE__, pTask->client_ip, \
			joinBody.subdir_count_per_path);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	p = pTask->data+sizeof(TrackerHeader)+sizeof(TrackerStorageJoinBody);
	pTrackerEnd = joinBody.tracker_servers + \
		      joinBody.tracker_count;
	/* 获取该storage所配置的所有tracker server的ip和port */
	for (pTrackerServer=joinBody.tracker_servers; \
		pTrackerServer<pTrackerEnd; pTrackerServer++)
	{
		* (p + FDFS_PROTO_IP_PORT_SIZE - 1) = '\0';
		if ((pSeperator=strchr(p, ':')) == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, invalid tracker server ip " \
				"and port: %s", __LINE__, pTask->client_ip, p);
			pTask->length = sizeof(TrackerHeader);
			return EINVAL;
		}

		*pSeperator = '\0';
		snprintf(pTrackerServer->ip_addr, \
			sizeof(pTrackerServer->ip_addr), "%s", p);
		pTrackerServer->port = atoi(pSeperator + 1);
		pTrackerServer->sock = -1;

		p += FDFS_PROTO_IP_PORT_SIZE;
	}

	/* 获取上传优先级 */
	joinBody.upload_priority = (int)buff2long(pBody->upload_priority);
	/* 获取storage的加入时间(创建时间) */
	joinBody.join_time = (time_t)buff2long(pBody->join_time);
	/* 获取服务启动时间 */
	joinBody.up_time = (time_t)buff2long(pBody->up_time);

	*(pBody->version + (sizeof(pBody->version) - 1)) = '\0';
	*(pBody->domain_name + (sizeof(pBody->domain_name) - 1)) = '\0';
	/* 获取storage 的版本号 */
	strcpy(joinBody.version, pBody->version);
	/* 获取storage的域名 */
	strcpy(joinBody.domain_name, pBody->domain_name);
	joinBody.init_flag = pBody->init_flag;
	joinBody.status = pBody->status;

	/* 获取自身的ip地址 */
	getSockIpaddr(pTask->event.fd, \
		tracker_ip, IP_ADDRESS_SIZE);

	/* 将指定ip地址加入到本地ip地址字符串中 */
	insert_into_local_host_ip(tracker_ip);


	/* 
	 * 将新加入的storage_server所在的group以及storage信息加入到内存的全局变量中
	 * 并写入文件 
	 */
	result = tracker_mem_add_group_and_storage(pClientInfo, \
			pTask->client_ip, &joinBody, true);
	if (result != 0)
	{
		pTask->length = sizeof(TrackerHeader);
		return result;
	}

	/* 填写应答报文体 */
	pJoinBodyResp = (TrackerStorageJoinBodyResp *)(pTask->data + \
				sizeof(TrackerHeader));
	memset(pJoinBodyResp, 0, sizeof(TrackerStorageJoinBodyResp));

	/* 设置源storage_server_id */
	if (pClientInfo->pStorage->psync_src_server != NULL)
	{
		strcpy(pJoinBodyResp->src_id, \
			pClientInfo->pStorage->psync_src_server->id);
	}

	pTask->length = sizeof(TrackerHeader) + \
			sizeof(TrackerStorageJoinBodyResp);
	return 0;
}

/*
 * 移除指定group中的一个storage
 * 发送报文:group_name,storage_id为选
 * 返回报文:无
 */
static int tracker_deal_server_delete_storage(struct fast_task_info *pTask)
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char *pStorageId;
	FDFSGroupInfo *pGroup;
	int nPkgLen;

	nPkgLen = pTask->length - sizeof(TrackerHeader);
	if (nPkgLen <= FDFS_GROUP_NAME_MAX_LEN)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length > %d", __LINE__, \
			TRACKER_PROTO_CMD_SERVER_DELETE_STORAGE, \
			pTask->client_ip, nPkgLen, FDFS_GROUP_NAME_MAX_LEN);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}
	if (nPkgLen >= FDFS_GROUP_NAME_MAX_LEN + FDFS_STORAGE_ID_MAX_SIZE)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length < %d", __LINE__, \
			TRACKER_PROTO_CMD_SERVER_DELETE_STORAGE, \
			pTask->client_ip, nPkgLen, \
			FDFS_GROUP_NAME_MAX_LEN + FDFS_STORAGE_ID_MAX_SIZE);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->data[pTask->length] = '\0';

	/* 解析出group_name */
	memcpy(group_name, pTask->data + sizeof(TrackerHeader), \
			FDFS_GROUP_NAME_MAX_LEN);
	group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
	/* 解析出storage_id */
	pStorageId = pTask->data + sizeof(TrackerHeader) + \
			FDFS_GROUP_NAME_MAX_LEN;
	*(pStorageId + FDFS_STORAGE_ID_MAX_SIZE - 1) = '\0';
	
	pGroup = tracker_mem_get_group(group_name);
	if (pGroup == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_name: %s", \
			__LINE__, pTask->client_ip, group_name);
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	pTask->length = sizeof(TrackerHeader);
	/* 在pGroup中删除指定storage_id的storage */
	return tracker_mem_delete_storage(pGroup, pStorageId);
}


/*
 * 设置trunk_server
 * 发送报文:group_name,storage_id
 * 返回报文:成功则返回trunk_server的id,失败则为空
 */
static int tracker_deal_server_set_trunk_server(struct fast_task_info *pTask)
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char *pStorageId;
	FDFSGroupInfo *pGroup;
	const FDFSStorageDetail *pTrunkServer;
	int nPkgLen;
	int result;

	nPkgLen = pTask->length - sizeof(TrackerHeader);
	if (nPkgLen < FDFS_GROUP_NAME_MAX_LEN)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length >= %d", __LINE__, \
			TRACKER_PROTO_CMD_SERVER_SET_TRUNK_SERVER, \
			pTask->client_ip, nPkgLen, FDFS_GROUP_NAME_MAX_LEN);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}
	if (nPkgLen >= FDFS_GROUP_NAME_MAX_LEN + FDFS_STORAGE_ID_MAX_SIZE)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length < %d", __LINE__, \
			TRACKER_PROTO_CMD_SERVER_SET_TRUNK_SERVER, \
			pTask->client_ip, nPkgLen, \
			FDFS_GROUP_NAME_MAX_LEN + FDFS_STORAGE_ID_MAX_SIZE);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->data[pTask->length] = '\0';

	/* 解析出group_name */
	memcpy(group_name, pTask->data + sizeof(TrackerHeader), \
			FDFS_GROUP_NAME_MAX_LEN);
	group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
	/* 解析出storage_id */
	pStorageId = pTask->data + sizeof(TrackerHeader) + \
			FDFS_GROUP_NAME_MAX_LEN;
	*(pStorageId + FDFS_STORAGE_ID_MAX_SIZE - 1) = '\0';
	pGroup = tracker_mem_get_group(group_name);
	if (pGroup == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_name: %s", \
			__LINE__, pTask->client_ip, group_name);
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	/* 设置pGroup中的pStroageId为trunk_server,如果pStorageId为空，则选取trunk_binlog_size最大的storage */
	pTrunkServer = tracker_mem_set_trunk_server(pGroup, \
				pStorageId, &result);
	if (result == 0 && pTrunkServer != NULL)
	{
		int nIdLen;
		nIdLen = strlen(pTrunkServer->id) + 1;
		pTask->length = sizeof(TrackerHeader) + nIdLen;
		memcpy(pTask->data + sizeof(TrackerHeader), \
			pTrunkServer->id, nIdLen);
	}
	else
	{
		if (result == 0)
		{
			result = ENOENT;
		}

		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, set trunk server %s:%s fail, " \
			"errno: %d, error info: %s", __LINE__, \
			pTask->client_ip, group_name, pStorageId, \
			result, STRERROR(result));
		pTask->length = sizeof(TrackerHeader);
	}
	return result;
}

/*
 * 测试主机是否处于正常通信状态
 */
static int tracker_deal_active_test(struct fast_task_info *pTask)
{
	if (pTask->length - sizeof(TrackerHeader) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length 0", __LINE__, \
			FDFS_PROTO_CMD_ACTIVE_TEST, pTask->client_ip, \
			pTask->length - (int)sizeof(TrackerHeader));
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->length = sizeof(TrackerHeader);
	return 0;
}

/*
 * 发送报文给tracker leader获取所有group的trunk_server_id
 * 发送报文:无
 * 返回报文:每一个group的group_name以及trunk_server_id
 */
static int tracker_deal_ping_leader(struct fast_task_info *pTask)
{
	FDFSGroupInfo **ppGroup;
	FDFSGroupInfo **ppEnd;
	int body_len;
	char *p;
	TrackerClientInfo *pClientInfo;
	
	pClientInfo = (TrackerClientInfo *)pTask->arg;
	if (pTask->length - sizeof(TrackerHeader) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length 0", __LINE__, \
			TRACKER_PROTO_CMD_TRACKER_PING_LEADER, \
			pTask->client_ip, \
			pTask->length - (int)sizeof(TrackerHeader));
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	/* 如果自己不是leader，不处理 */
	if (!g_if_leader_self)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, i am not the leader!", \
			__LINE__, TRACKER_PROTO_CMD_TRACKER_PING_LEADER, \
			pTask->client_ip);
		pTask->length = sizeof(TrackerHeader);
		return EOPNOTSUPP;
	}

	/* 如果trunk_server变更次数一样，说明trunk_server没有变更 */
	if (pClientInfo->chg_count.trunk_server == g_trunk_server_chg_count)
	{
		pTask->length = sizeof(TrackerHeader);
		return 0;
	}

	/* 报文体为每一个group的group_name以及trunk_server_id */
	body_len = (FDFS_GROUP_NAME_MAX_LEN + FDFS_STORAGE_ID_MAX_SIZE) * \
			g_groups.count;
	if (body_len + sizeof(TrackerHeader) > pTask->size)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, " \
			"exceeds max package size: %d!", \
			__LINE__, TRACKER_PROTO_CMD_TRACKER_PING_LEADER, \
			pTask->client_ip, pTask->size);
		pTask->length = sizeof(TrackerHeader);
		return ENOSPC;
	}

	p = pTask->data + sizeof(TrackerHeader);
	memset(p, 0, body_len);

	ppEnd = g_groups.sorted_groups + g_groups.count;
	for (ppGroup=g_groups.sorted_groups; ppGroup<ppEnd; ppGroup++)
	{
		memcpy(p, (*ppGroup)->group_name, FDFS_GROUP_NAME_MAX_LEN);
		p += FDFS_GROUP_NAME_MAX_LEN;

		if ((*ppGroup)->pTrunkServer != NULL)
		{
			memcpy(p, (*ppGroup)->pTrunkServer->id, \
				FDFS_STORAGE_ID_MAX_SIZE);
		}
		p += FDFS_STORAGE_ID_MAX_SIZE;
	}

	pTask->length = p - pTask->data;
	pClientInfo->chg_count.trunk_server = g_trunk_server_chg_count;

	return 0;
}

/* 客户端加锁后的解锁操作 */
static int tracker_unlock_by_client(struct fast_task_info *pTask)
{
	if (lock_by_client_count <= 0 || pTask->finish_callback == NULL)
	{  //already unlocked
		return 0;
	}

	pTask->finish_callback = NULL;
	lock_by_client_count--;

	tracker_mem_file_unlock();

	logInfo("file: "__FILE__", line: %d, " \
		"unlock by client: %s, locked count: %d", \
		__LINE__, pTask->client_ip, lock_by_client_count);

	return 0;
}

/* 接收到客户端报文后上锁 */
static int tracker_lock_by_client(struct fast_task_info *pTask)
{
	if (lock_by_client_count > 0)
	{
		return EBUSY;
	}

	tracker_mem_file_lock();  //avoid to read dirty data

	/* 任务结束后确保会解锁 */
	pTask->finish_callback = tracker_unlock_by_client; //make sure to release lock
	lock_by_client_count++;

	logInfo("file: "__FILE__", line: %d, " \
		"lock by client: %s, locked count: %d", \
		__LINE__, pTask->client_ip, lock_by_client_count);

	return 0;
}

/*
 * tracker获取其他tracker server的状态信息
 * 发送报文:无
 * 返回报文: 1个字节的if_leader + running time + startup interval
 */
static int tracker_deal_get_tracker_status(struct fast_task_info *pTask)
{
	char *p;
	TrackerRunningStatus runningStatus;

	if (pTask->length - sizeof(TrackerHeader) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length %d", __LINE__, \
			TRACKER_PROTO_CMD_TRACKER_GET_STATUS, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader), 0);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	if (g_groups.count <= 0)
	{
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	p = pTask->data + sizeof(TrackerHeader);

	/* 计算进程运行时间及重启的间隔时间 */
	tracker_calc_running_times(&runningStatus);

	*p++= g_if_leader_self;  //if leader

	long2buff(runningStatus.running_time, p);
	p += FDFS_PROTO_PKG_LEN_SIZE;

	long2buff(runningStatus.restart_interval, p);
	p += FDFS_PROTO_PKG_LEN_SIZE;

	pTask->length = p - pTask->data;
	return 0;
}

/*
 * 开始获取系统数据文件信息，同步数据到文件中，加锁
 * 发送报文:无
 * 返回报文: 无
 */
static int tracker_deal_get_sys_files_start(struct fast_task_info *pTask)
{
	int result;

	if (pTask->length - sizeof(TrackerHeader) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length %d", __LINE__, \
			TRACKER_PROTO_CMD_TRACKER_GET_SYS_FILES_START, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader), 0);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->length = sizeof(TrackerHeader);
	if (g_groups.count == 0)
	{
		return ENOENT;
	}

	/* 将系统数据文件在内存中对应的信息写入文件中 */
	if ((result=tracker_save_sys_files()) != 0)
	{
		return result == ENOENT ? EACCES: result;
	}

	/* 接收到客户端报文后上锁 */
	return tracker_lock_by_client(pTask);
}

/* 结束获取系统数据文件信息，解锁 */
static int tracker_deal_get_sys_files_end(struct fast_task_info *pTask)
{
	if (pTask->length - sizeof(TrackerHeader) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length %d", __LINE__, \
			TRACKER_PROTO_CMD_TRACKER_GET_SYS_FILES_END, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader), 0);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->length = sizeof(TrackerHeader);
	/* 客户端加锁后的解锁操作 */
	return tracker_unlock_by_client(pTask);
}

/*
 * 获取一个系统数据文件，指定文件名索引，及偏移量，多次传输
 * 发送报文:1个字节的文件名索引+ 8个字节的偏移量
 * 返回报文:总的文件大小 + 从偏移量开始取到的文件内容
 */
static int tracker_deal_get_one_sys_file(struct fast_task_info *pTask)
{
#define TRACKER_READ_BYTES_ONCE  (TRACKER_MAX_PACKAGE_SIZE - \
			FDFS_PROTO_PKG_LEN_SIZE - sizeof(TrackerHeader) - 1)
	int result;
	int index;
	struct stat file_stat;
	int64_t offset;
	int64_t read_bytes;
	int64_t bytes;
	char full_filename[MAX_PATH_SIZE];
	char *p;

	if (pTask->length - sizeof(TrackerHeader) != 1+FDFS_PROTO_PKG_LEN_SIZE)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length %d", __LINE__, \
			TRACKER_PROTO_CMD_TRACKER_GET_ONE_SYS_FILE, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader), 1+FDFS_PROTO_PKG_LEN_SIZE);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	p = pTask->data + sizeof(TrackerHeader);
	index = *p++;
	offset = buff2long(p);

	if (index < 0 || index >= TRACKER_SYS_FILE_COUNT)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid index: %d", \
			__LINE__, pTask->client_ip, index);

		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	snprintf(full_filename, sizeof(full_filename), "%s/data/%s", \
		g_fdfs_base_path, g_tracker_sys_filenames[index]);
	if (stat(full_filename, &file_stat) != 0)
	{
		result = errno != 0 ? errno : ENOENT;
		logError("file: "__FILE__", line: %d, " \
			"client ip:%s, call stat file %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pTask->client_ip, full_filename,
			result, STRERROR(result));
		return result;
	}

	if (offset < 0 || offset > file_stat.st_size)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid offset: "INT64_PRINTF_FORMAT
			" < 0 or > "OFF_PRINTF_FORMAT,  \
			__LINE__, pTask->client_ip, offset, \
			file_stat.st_size);

		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	read_bytes = file_stat.st_size - offset;
	if (read_bytes > TRACKER_READ_BYTES_ONCE)
	{
		read_bytes = TRACKER_READ_BYTES_ONCE;
	}

	p = pTask->data + sizeof(TrackerHeader);
	long2buff(file_stat.st_size, p);
	p += FDFS_PROTO_PKG_LEN_SIZE;

	bytes = read_bytes;
	if (read_bytes > 0 && (result=getFileContentEx(full_filename, \
					p, offset, &bytes)) != 0)
	{
		pTask->length = sizeof(TrackerHeader);
		return result;
	}

	if (bytes != read_bytes)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, read bytes: "INT64_PRINTF_FORMAT
			" != expect bytes: "INT64_PRINTF_FORMAT,  \
			__LINE__, pTask->client_ip, bytes, read_bytes);

		pTask->length = sizeof(TrackerHeader);
		return EIO;
	}

	p += read_bytes;
	pTask->length = p - pTask->data;
	return 0;
}

/*
 * storage IP变更报文
 * 发送报文:group_name + old_ip + new_ip
 * 返回报文:无
 */
static int tracker_deal_storage_report_ip_changed(struct fast_task_info *pTask)
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	FDFSGroupInfo *pGroup;
	char *pOldIpAddr;
	char *pNewIpAddr;
	
	if (g_use_storage_id)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, do NOT support ip changed adjust " \
			"because cluster use server ID instead of " \
			"IP address", __LINE__, pTask->client_ip);
		return EOPNOTSUPP;
	}

	if (pTask->length - sizeof(TrackerHeader) != \
			FDFS_GROUP_NAME_MAX_LEN + 2 * IP_ADDRESS_SIZE)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length = %d", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_REPORT_IP_CHANGED, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader),\
			FDFS_GROUP_NAME_MAX_LEN + 2 * IP_ADDRESS_SIZE);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	memcpy(group_name, pTask->data + sizeof(TrackerHeader), \
			FDFS_GROUP_NAME_MAX_LEN);
	*(group_name + FDFS_GROUP_NAME_MAX_LEN) = '\0';

	pOldIpAddr = pTask->data + sizeof(TrackerHeader) + \
			FDFS_GROUP_NAME_MAX_LEN;
	*(pOldIpAddr + (IP_ADDRESS_SIZE - 1)) = '\0';

	pNewIpAddr = pOldIpAddr + IP_ADDRESS_SIZE;
	*(pNewIpAddr + (IP_ADDRESS_SIZE - 1)) = '\0';

	pGroup = tracker_mem_get_group(group_name);
	if (pGroup == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_name: %s", \
			__LINE__, pTask->client_ip, group_name);
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	if (strcmp(pNewIpAddr, pTask->client_ip) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, group_name: %s, " \
			"new ip address %s != client ip address %s", \
			__LINE__, pTask->client_ip, group_name, \
			pNewIpAddr, pTask->client_ip);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->length = sizeof(TrackerHeader);
	/* 在pGroup中修改指定storage_id的ip */
	return tracker_mem_storage_ip_changed(pGroup, \
			pOldIpAddr, pNewIpAddr);
}

/*
 * 同步完成通知报文
 * 发送报文体:TrackerStorageSyncReqBody结构的同步信息
 * 返回报文:无
 */
static int tracker_deal_storage_sync_notify(struct fast_task_info *pTask)
{
	TrackerStorageSyncReqBody *pBody;
	char sync_src_id[FDFS_STORAGE_ID_MAX_SIZE];
	bool bSaveStorages;
	TrackerClientInfo *pClientInfo;
	
	pClientInfo = (TrackerClientInfo *)pTask->arg;

	if (pTask->length  - sizeof(TrackerHeader) != \
			sizeof(TrackerStorageSyncReqBody))
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd: %d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length: %d", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_SYNC_NOTIFY, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader),
			(int)sizeof(TrackerStorageSyncReqBody));
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pBody=(TrackerStorageSyncReqBody *)(pTask->data+sizeof(TrackerHeader));
	/* 如果src_id为空 */
	if (*(pBody->src_id) == '\0')
	{
		if (pClientInfo->pStorage->status == FDFS_STORAGE_STATUS_INIT || \
		    pClientInfo->pStorage->status == FDFS_STORAGE_STATUS_WAIT_SYNC || \
		    pClientInfo->pStorage->status == FDFS_STORAGE_STATUS_SYNCING)
		{
			pClientInfo->pStorage->status = FDFS_STORAGE_STATUS_ONLINE;
			pClientInfo->pGroup->chg_count++;
			tracker_save_storages();
		}

		pTask->length = sizeof(TrackerHeader);
		return 0;
	}

	/* 如果src_id不为空 */
	bSaveStorages = false;
	/* 如果状态为init,修改为wait_sync */
	if (pClientInfo->pStorage->status == FDFS_STORAGE_STATUS_INIT)
	{
		pClientInfo->pStorage->status = FDFS_STORAGE_STATUS_WAIT_SYNC;
		pClientInfo->pGroup->chg_count++;
		bSaveStorages = true;
	}

	/* 如果没有设置同步源storage，设置报文中的src_id对应的storage为同步源storage */
	if (pClientInfo->pStorage->psync_src_server == NULL)
	{
		memcpy(sync_src_id, pBody->src_id, \
				FDFS_STORAGE_ID_MAX_SIZE);
		sync_src_id[FDFS_STORAGE_ID_MAX_SIZE - 1] = '\0';

		pClientInfo->pStorage->psync_src_server = \
			tracker_mem_get_storage(pClientInfo->pGroup, \
				sync_src_id);
		if (pClientInfo->pStorage->psync_src_server == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, " \
				"sync src server: %s not exists", \
				__LINE__, pTask->client_ip, \
				sync_src_id);
			pTask->length = sizeof(TrackerHeader);
			return ENOENT;
		}

		if (pClientInfo->pStorage->psync_src_server->status == \
			FDFS_STORAGE_STATUS_DELETED)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, " \
				"sync src server: %s already be deleted", \
				__LINE__, pTask->client_ip, \
				sync_src_id);
			pTask->length = sizeof(TrackerHeader);
			return ENOENT;
		}

		if (pClientInfo->pStorage->psync_src_server->status == \
			FDFS_STORAGE_STATUS_IP_CHANGED)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, the ip address of " \
				"the sync src server: %s changed", \
				__LINE__, pTask->client_ip, \
				sync_src_id);
			pTask->length = sizeof(TrackerHeader);
			return ENOENT;
		}

		/* 修改同步时间戳为报文中的同步时间戳 */
		pClientInfo->pStorage->sync_until_timestamp = \
				(int)buff2long(pBody->until_timestamp);
		bSaveStorages = true;
	}

	if (bSaveStorages)
	{
		tracker_save_storages();
	}

	pTask->length = sizeof(TrackerHeader);
	return 0;
}

/*
 * 获取指定group中所有storage的状态信息
 * pkg format:
 * Header
 * FDFS_GROUP_NAME_MAX_LEN bytes: group_name
 * 发送报文体:group_name，storage_id可能存在
 * 返回报文:查询的group中所有storage的TrackerStorageStat对象信息
 */
static int tracker_deal_server_list_group_storages(struct fast_task_info *pTask)
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char storage_id[FDFS_STORAGE_ID_MAX_SIZE];
	char *pStorageId;
	FDFSGroupInfo *pGroup;
	FDFSStorageDetail **ppServer;
	FDFSStorageDetail **ppEnd;
	FDFSStorageStat *pStorageStat;
	TrackerStorageStat *pStart;
	TrackerStorageStat *pDest;
	FDFSStorageStatBuff *pStatBuff;
	int nPkgLen;
	int id_len;

	nPkgLen = pTask->length - sizeof(TrackerHeader);
	if (nPkgLen < FDFS_GROUP_NAME_MAX_LEN || \
		nPkgLen >= FDFS_GROUP_NAME_MAX_LEN + FDFS_STORAGE_ID_MAX_SIZE)
	{
		logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				PKG_LEN_PRINTF_FORMAT" is not correct, " \
				"expect length >= %d && <= %d", __LINE__, \
				TRACKER_PROTO_CMD_SERVER_LIST_STORAGE, \
				pTask->client_ip,  \
				nPkgLen, FDFS_GROUP_NAME_MAX_LEN, \
				FDFS_GROUP_NAME_MAX_LEN + FDFS_STORAGE_ID_MAX_SIZE);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	/* 解析出group_name */
	memcpy(group_name, pTask->data + sizeof(TrackerHeader), \
			FDFS_GROUP_NAME_MAX_LEN);
	*(group_name + FDFS_GROUP_NAME_MAX_LEN) = '\0';
	pGroup = tracker_mem_get_group(group_name);
	if (pGroup == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_name: %s", \
			__LINE__, pTask->client_ip, group_name);
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	/* 如果指定了storage_id */
	if (nPkgLen > FDFS_GROUP_NAME_MAX_LEN)
	{
		id_len = nPkgLen - FDFS_GROUP_NAME_MAX_LEN;
		if (id_len >= sizeof(storage_id))
		{
			id_len = sizeof(storage_id) - 1;
		}
		pStorageId = storage_id;
		memcpy(pStorageId, pTask->data + sizeof(TrackerHeader) + \
			FDFS_GROUP_NAME_MAX_LEN, id_len);
		*(pStorageId + id_len) = '\0';
	}
	else
	{
		pStorageId = NULL;
	}

	memset(pTask->data + sizeof(TrackerHeader), 0, \
			pTask->size - sizeof(TrackerHeader));
	pDest = pStart = (TrackerStorageStat *)(pTask->data + \
					sizeof(TrackerHeader));
	ppEnd = pGroup->sorted_servers + pGroup->count;
	/* 遍历此group中所有storage，获取状态信息 */
	for (ppServer=pGroup->sorted_servers; ppServer<ppEnd; \
			ppServer++)
	{
		/* 如果和指定的storage_id不同，跳过 */
		if (pStorageId != NULL && strcmp(pStorageId, \
					(*ppServer)->id) != 0)
		{
			continue;
		}

		pStatBuff = &(pDest->stat_buff);
		pStorageStat = &((*ppServer)->stat);
		pDest->status = (*ppServer)->status;
		strcpy(pDest->id, (*ppServer)->id);
		strcpy(pDest->ip_addr, (*ppServer)->ip_addr);
		if ((*ppServer)->psync_src_server != NULL)
		{
			strcpy(pDest->src_id, \
				(*ppServer)->psync_src_server->id);
		}

		strcpy(pDest->domain_name, (*ppServer)->domain_name);
		strcpy(pDest->version, (*ppServer)->version);
		long2buff((*ppServer)->join_time, pDest->sz_join_time);
		long2buff((*ppServer)->up_time, pDest->sz_up_time);
		long2buff((*ppServer)->total_mb, pDest->sz_total_mb);
		long2buff((*ppServer)->free_mb, pDest->sz_free_mb);
		long2buff((*ppServer)->upload_priority, \
				pDest->sz_upload_priority);
		long2buff((*ppServer)->storage_port, \
				pDest->sz_storage_port);
		long2buff((*ppServer)->storage_http_port, \
				pDest->sz_storage_http_port);
		long2buff((*ppServer)->store_path_count, \
				pDest->sz_store_path_count);
		long2buff((*ppServer)->subdir_count_per_path, \
				pDest->sz_subdir_count_per_path);
		long2buff((*ppServer)->current_write_path, \
				pDest->sz_current_write_path);

		long2buff(pStorageStat->total_upload_count, \
				pStatBuff->sz_total_upload_count);
		long2buff(pStorageStat->success_upload_count, \
				pStatBuff->sz_success_upload_count);
		long2buff(pStorageStat->total_append_count, \
				pStatBuff->sz_total_append_count);
		long2buff(pStorageStat->success_append_count, \
				pStatBuff->sz_success_append_count);
		long2buff(pStorageStat->total_modify_count, \
				pStatBuff->sz_total_modify_count);
		long2buff(pStorageStat->success_modify_count, \
				pStatBuff->sz_success_modify_count);
		long2buff(pStorageStat->total_truncate_count, \
				pStatBuff->sz_total_truncate_count);
		long2buff(pStorageStat->success_truncate_count, \
				pStatBuff->sz_success_truncate_count);
		long2buff(pStorageStat->total_set_meta_count, \
				pStatBuff->sz_total_set_meta_count);
		long2buff(pStorageStat->success_set_meta_count, \
				pStatBuff->sz_success_set_meta_count);
		long2buff(pStorageStat->total_delete_count, \
				pStatBuff->sz_total_delete_count);
		long2buff(pStorageStat->success_delete_count, \
				pStatBuff->sz_success_delete_count);
		long2buff(pStorageStat->total_download_count, \
				pStatBuff->sz_total_download_count);
		long2buff(pStorageStat->success_download_count, \
				pStatBuff->sz_success_download_count);
		long2buff(pStorageStat->total_get_meta_count, \
				pStatBuff->sz_total_get_meta_count);
		long2buff(pStorageStat->success_get_meta_count, \
				pStatBuff->sz_success_get_meta_count);
		long2buff(pStorageStat->last_source_update, \
				pStatBuff->sz_last_source_update);
		long2buff(pStorageStat->last_sync_update, \
				pStatBuff->sz_last_sync_update);
		long2buff(pStorageStat->last_synced_timestamp, \
				pStatBuff->sz_last_synced_timestamp);
		long2buff(pStorageStat->total_create_link_count, \
				pStatBuff->sz_total_create_link_count);
		long2buff(pStorageStat->success_create_link_count, \
				pStatBuff->sz_success_create_link_count);
		long2buff(pStorageStat->total_delete_link_count, \
				pStatBuff->sz_total_delete_link_count);
		long2buff(pStorageStat->success_delete_link_count, \
				pStatBuff->sz_success_delete_link_count);
		long2buff(pStorageStat->total_upload_bytes, \
				pStatBuff->sz_total_upload_bytes);
		long2buff(pStorageStat->success_upload_bytes, \
				pStatBuff->sz_success_upload_bytes);
		long2buff(pStorageStat->total_append_bytes, \
				pStatBuff->sz_total_append_bytes);
		long2buff(pStorageStat->success_append_bytes, \
				pStatBuff->sz_success_append_bytes);
		long2buff(pStorageStat->total_modify_bytes, \
				pStatBuff->sz_total_modify_bytes);
		long2buff(pStorageStat->success_modify_bytes, \
				pStatBuff->sz_success_modify_bytes);
		long2buff(pStorageStat->total_download_bytes, \
				pStatBuff->sz_total_download_bytes);
		long2buff(pStorageStat->success_download_bytes, \
				pStatBuff->sz_success_download_bytes);
		long2buff(pStorageStat->total_sync_in_bytes, \
				pStatBuff->sz_total_sync_in_bytes);
		long2buff(pStorageStat->success_sync_in_bytes, \
				pStatBuff->sz_success_sync_in_bytes);
		long2buff(pStorageStat->total_sync_out_bytes, \
				pStatBuff->sz_total_sync_out_bytes);
		long2buff(pStorageStat->success_sync_out_bytes, \
				pStatBuff->sz_success_sync_out_bytes);
		long2buff(pStorageStat->total_file_open_count, \
				pStatBuff->sz_total_file_open_count);
		long2buff(pStorageStat->success_file_open_count, \
				pStatBuff->sz_success_file_open_count);
		long2buff(pStorageStat->total_file_read_count, \
				pStatBuff->sz_total_file_read_count);
		long2buff(pStorageStat->success_file_read_count, \
				pStatBuff->sz_success_file_read_count);
		long2buff(pStorageStat->total_file_write_count, \
				pStatBuff->sz_total_file_write_count);
		long2buff(pStorageStat->success_file_write_count, \
				pStatBuff->sz_success_file_write_count);
		long2buff(pStorageStat->last_heart_beat_time, \
				pStatBuff->sz_last_heart_beat_time);
		pDest->if_trunk_server = (pGroup->pTrunkServer == *ppServer);

		pDest++;
	}

	if (pStorageId != NULL && pDest - pStart == 0)
	{
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	pTask->length = sizeof(TrackerHeader) + (pDest - pStart) * \
				sizeof(TrackerStorageStat);
	return 0;
}

/*
 * 获取下载或更新文件需要连接的storage
 * pkg format:
 * Header
 * FDFS_GROUP_NAME_MAX_LEN bytes: group_name
 * remain bytes: filename 22-127字节
 *
 * 返回报文:group_name,ip,port 如果是获取多个文件的storage，后面加上对应的所有ip地址
 */
static int tracker_deal_service_query_fetch_update( \
		struct fast_task_info *pTask, const byte cmd)
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char *filename;
	char *p;
	FDFSGroupInfo *pGroup;
	FDFSStorageDetail **ppServer;
	FDFSStorageDetail **ppServerEnd;
	FDFSStorageDetail *ppStoreServers[FDFS_MAX_SERVERS_EACH_GROUP];
	int filename_len;
	int server_count;
	int result;
	int nPkgLen;

	nPkgLen = pTask->length - sizeof(TrackerHeader);
	if (nPkgLen < FDFS_GROUP_NAME_MAX_LEN+22)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length > %d", __LINE__, cmd, \
			pTask->client_ip, nPkgLen, \
			FDFS_GROUP_NAME_MAX_LEN+22);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}
	if (nPkgLen >= FDFS_GROUP_NAME_MAX_LEN + 128)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is too long, exceeds %d", \
			__LINE__, cmd, pTask->client_ip, nPkgLen, \
			FDFS_GROUP_NAME_MAX_LEN + 128);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->data[pTask->length] = '\0';

	memcpy(group_name, pTask->data + sizeof(TrackerHeader), \
			FDFS_GROUP_NAME_MAX_LEN);
	group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
	filename = pTask->data + sizeof(TrackerHeader)+FDFS_GROUP_NAME_MAX_LEN;
	filename_len = pTask->length - sizeof(TrackerHeader) - \
			FDFS_GROUP_NAME_MAX_LEN;

	/* 根据file_name获取应该去哪台storage server下载 */
	result = tracker_mem_get_storage_by_filename(cmd, \
			FDFS_DOWNLOAD_TYPE_CALL \
			group_name, filename, filename_len, &pGroup, \
			ppStoreServers, &server_count);

	if (result != 0)
	{
		pTask->length = sizeof(TrackerHeader);
		return result;
	}


	pTask->length = sizeof(TrackerHeader) + \
			TRACKER_QUERY_STORAGE_FETCH_BODY_LEN + \
			(server_count - 1) * (IP_ADDRESS_SIZE - 1);

	/* 写入group_name */
	p  = pTask->data + sizeof(TrackerHeader);
	memcpy(p, pGroup->group_name, FDFS_GROUP_NAME_MAX_LEN);
	p += FDFS_GROUP_NAME_MAX_LEN;
	/* 写入ip地址 */
	memcpy(p, ppStoreServers[0]->ip_addr, IP_ADDRESS_SIZE-1);
	p += IP_ADDRESS_SIZE - 1;
	/* 写入port */
	long2buff(pGroup->storage_port, p);
	p += FDFS_PROTO_PKG_LEN_SIZE;

	/* 如果有多个storage的信息，依次把ip地址写在后面 */
	if (server_count > 1)
	{
		ppServerEnd = ppStoreServers + server_count;
		for (ppServer=ppStoreServers+1; ppServer<ppServerEnd; \
				ppServer++)
		{
			memcpy(p, (*ppServer)->ip_addr, \
					IP_ADDRESS_SIZE - 1);
			p += IP_ADDRESS_SIZE - 1;
		}
	}

	return 0;
}

/* 检查pGroup的空闲空间是否符合保留空间的限制 */
#define tracker_check_reserved_space(pGroup) \
	fdfs_check_reserved_space(pGroup, &g_storage_reserved_space)

/* 检查pGroup的trunk空间空间是否符合保留空间的限制 */
#define tracker_check_reserved_space_trunk(pGroup) \
	fdfs_check_reserved_space_trunk(pGroup, &g_storage_reserved_space)

/* 检查pGroup的space_path是否符合保留空间的限制 */
#define tracker_check_reserved_space_path(total_mb, free_mb, avg_mb) \
	fdfs_check_reserved_space_path(total_mb, free_mb, avg_mb, \
				&g_storage_reserved_space)

/*
 * 选取一个storage来上传文件，可以指定group，可以获取一个或全部
 * 报文体:group_name或者为空
 * 返回报文:group_name,一个或多个ip地址+端口号,base_store_path路径索引值
 */
static int tracker_deal_service_query_storage( \
		struct fast_task_info *pTask, char cmd)
{
	int expect_pkg_len;
	FDFSGroupInfo *pStoreGroup;
	FDFSGroupInfo **ppFoundGroup;
	FDFSGroupInfo **ppGroup;
	FDFSStorageDetail *pStorageServer;
	char *group_name;
	char *p;
	bool bHaveActiveServer;
	int write_path_index;
	int avg_reserved_mb;

	if (cmd == TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE
	 || cmd == TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ALL)
	{
		expect_pkg_len = FDFS_GROUP_NAME_MAX_LEN;
	}
	else
	{
		expect_pkg_len = 0;
	}

	if (pTask->length - sizeof(TrackerHeader) != expect_pkg_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT"is not correct, " \
			"expect length: %d", __LINE__, \
			cmd, pTask->client_ip, \
			pTask->length - (int)sizeof(TrackerHeader), \
			expect_pkg_len);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	if (g_groups.count == 0)
	{
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	if (cmd == TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE
	 || cmd == TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ALL)
	{
		/* 解析出group_name */
		group_name = pTask->data + sizeof(TrackerHeader);
		group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';

		pStoreGroup = tracker_mem_get_group(group_name);
		if (pStoreGroup == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, invalid group name: %s", \
				__LINE__, pTask->client_ip, group_name);
			pTask->length = sizeof(TrackerHeader);
			return ENOENT;
		}

		if (pStoreGroup->active_count == 0)
		{
			pTask->length = sizeof(TrackerHeader);
			return ENOENT;
		}

		/* 检查pGroup的空闲空间如果不符合保留空间的限制并且使用了trunk_file也不够 */
		if (!tracker_check_reserved_space(pStoreGroup))
		{
			if (!(g_if_use_trunk_file && \
				tracker_check_reserved_space_trunk(pStoreGroup)))
			{
				/* 出错 */
				pTask->length = sizeof(TrackerHeader);
				return ENOSPC;
			}
		}
	}
	/* 没有指定group，并且采用循环或均衡选择的方式上传文件 */
	else if (g_groups.store_lookup == FDFS_STORE_LOOKUP_ROUND_ROBIN
		||g_groups.store_lookup==FDFS_STORE_LOOKUP_LOAD_BALANCE)
	{
		int write_group_index;

		bHaveActiveServer = false;
		/* 选取上传文件的group */
		write_group_index = g_groups.current_write_group;
		if (write_group_index >= g_groups.count)
		{
			write_group_index = 0;
		}

		pStoreGroup = NULL;
		ppFoundGroup = g_groups.sorted_groups + write_group_index;
		if ((*ppFoundGroup)->active_count > 0)
		{
			bHaveActiveServer = true;
			if (tracker_check_reserved_space(*ppFoundGroup))
			{
				pStoreGroup = *ppFoundGroup;
			}
			else if (g_if_use_trunk_file && \
				g_groups.store_lookup == \
				FDFS_STORE_LOOKUP_LOAD_BALANCE && \
				tracker_check_reserved_space_trunk( \
					*ppFoundGroup))
			{
				pStoreGroup = *ppFoundGroup;
			}
		}

		if (pStoreGroup == NULL)
		{
			FDFSGroupInfo **ppGroupEnd;
			ppGroupEnd = g_groups.sorted_groups + \
				     g_groups.count;
			for (ppGroup=ppFoundGroup+1; \
					ppGroup<ppGroupEnd; ppGroup++)
			{
				if ((*ppGroup)->active_count == 0)
				{
					continue;
				}

				bHaveActiveServer = true;
				if (tracker_check_reserved_space(*ppGroup))
				{
					pStoreGroup = *ppGroup;
					g_groups.current_write_group = \
					       ppGroup-g_groups.sorted_groups;
					break;
				}
			}

			if (pStoreGroup == NULL)
			{
				for (ppGroup=g_groups.sorted_groups; \
						ppGroup<ppFoundGroup; ppGroup++)
				{
					if ((*ppGroup)->active_count == 0)
					{
						continue;
					}

					bHaveActiveServer = true;
					if (tracker_check_reserved_space(*ppGroup))
					{
						pStoreGroup = *ppGroup;
						g_groups.current_write_group = \
						 ppGroup-g_groups.sorted_groups;
						break;
					}
				}
			}

			if (pStoreGroup == NULL)
			{
				if (!bHaveActiveServer)
				{
					pTask->length = sizeof(TrackerHeader);
					return ENOENT;
				}

				if (!g_if_use_trunk_file)
				{
					pTask->length = sizeof(TrackerHeader);
					return ENOSPC;
				}

				for (ppGroup=g_groups.sorted_groups; \
						ppGroup<ppGroupEnd; ppGroup++)
				{
					if ((*ppGroup)->active_count == 0)
					{
						continue;
					}
					if (tracker_check_reserved_space_trunk(*ppGroup))
					{
						pStoreGroup = *ppGroup;
						g_groups.current_write_group = \
						 ppGroup-g_groups.sorted_groups;
						break;
					}
				}

				if (pStoreGroup == NULL)
				{
					pTask->length = sizeof(TrackerHeader);
					return ENOSPC;
				}
			}
		}

		if (g_groups.store_lookup == FDFS_STORE_LOOKUP_ROUND_ROBIN)
		{
			g_groups.current_write_group++;
			if (g_groups.current_write_group >= g_groups.count)
			{
				g_groups.current_write_group = 0;
			}
		}
	}
	/* 如果没有指定group并且采用特定组的方式，即pStoreGroup，按照优先级来排 */
	else if (g_groups.store_lookup == FDFS_STORE_LOOKUP_SPEC_GROUP)
	{
		if (g_groups.pStoreGroup == NULL || \
				g_groups.pStoreGroup->active_count == 0)
		{
			pTask->length = sizeof(TrackerHeader);
			return ENOENT;
		}

		if (!tracker_check_reserved_space(g_groups.pStoreGroup))
		{
			if (!(g_if_use_trunk_file && \
				tracker_check_reserved_space_trunk( \
						g_groups.pStoreGroup)))
			{
				pTask->length = sizeof(TrackerHeader);
				return ENOSPC;
			}
		}

		pStoreGroup = g_groups.pStoreGroup;
	}
	else
	{
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	if (pStoreGroup->store_path_count <= 0)
	{
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	if (cmd == TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE
	  || cmd == TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE)
	{
		/* 在指定group中获取一个可上传文件的storage */
		pStorageServer = tracker_get_writable_storage(pStoreGroup);
		if (pStorageServer == NULL)
		{
			pTask->length = sizeof(TrackerHeader);
			return ENOENT;
		}
	}
	/* 获取所有可用的storage server */
	else  //query store server list, use the first to check
	{
		pStorageServer = *(pStoreGroup->active_servers);
	}

	write_path_index = pStorageServer->current_write_path;
	if (write_path_index >= pStoreGroup->store_path_count)
	{
		write_path_index = 0;
	}

	/* 平均每个路径需保留的空间大小 */
	avg_reserved_mb = g_storage_reserved_space.rs.mb / \
			  pStoreGroup->store_path_count;
	/* 判断每个store_path的空间是否足够 */
	if (!tracker_check_reserved_space_path(pStorageServer-> \
		path_total_mbs[write_path_index], pStorageServer-> \
		path_free_mbs[write_path_index], avg_reserved_mb))
	{
		int i;
		for (i=0; i<pStoreGroup->store_path_count; i++)
		{
			if (tracker_check_reserved_space_path( \
				pStorageServer->path_total_mbs[i], \
				pStorageServer->path_free_mbs[i], \
				avg_reserved_mb))
			{
				pStorageServer->current_write_path = i;
				write_path_index = i;
				break;
			}
		}

		if (i == pStoreGroup->store_path_count)
		{
			if (!g_if_use_trunk_file)
			{
				pTask->length = sizeof(TrackerHeader);
				return ENOSPC;
			}

			for (i=write_path_index; i<pStoreGroup-> \
				store_path_count; i++)
			{
				if (tracker_check_reserved_space_path( \
				  pStorageServer->path_total_mbs[i], \
				  pStorageServer->path_free_mbs[i] + \
				  pStoreGroup->trunk_free_mb, \
				  avg_reserved_mb))
				{
					pStorageServer->current_write_path = i;
					write_path_index = i;
					break;
				}
			}
			if ( i == pStoreGroup->store_path_count)
			{
				for (i=0; i<write_path_index; i++)
				{
				if (tracker_check_reserved_space_path( \
				  pStorageServer->path_total_mbs[i], \
				  pStorageServer->path_free_mbs[i] + \
				  pStoreGroup->trunk_free_mb, \
				  avg_reserved_mb))
				{
					pStorageServer->current_write_path = i;
					write_path_index = i;
					break;
				}
				}

				if (i == write_path_index)
				{
					pTask->length = sizeof(TrackerHeader);
					return ENOSPC;
				}
			}
		}
	}

	/* 如果设置为循环的方式上传文件，直接把current_write_path指向下一个storage */
	if (g_groups.store_path == FDFS_STORE_PATH_ROUND_ROBIN)
	{
		pStorageServer->current_write_path++;
		if (pStorageServer->current_write_path >= \
				pStoreGroup->store_path_count)
		{
			pStorageServer->current_write_path = 0;
		}
	}

	/*
	//printf("pStoreGroup->current_write_server: %d, " \
	"pStoreGroup->active_count=%d\n", \
	pStoreGroup->current_write_server, \
	pStoreGroup->active_count);
	*/

	/* 拼返回报文 */
	p = pTask->data + sizeof(TrackerHeader);
	/* 写入group_name */
	memcpy(p, pStoreGroup->group_name, FDFS_GROUP_NAME_MAX_LEN);
	p += FDFS_GROUP_NAME_MAX_LEN;

	/* 写入多个ip地址+端口号 */
	if (cmd == TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ALL
	 || cmd == TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ALL)
	{
		int active_count;
		FDFSStorageDetail **ppServer;
		FDFSStorageDetail **ppEnd;

		active_count = pStoreGroup->active_count;
		if (active_count == 0)
		{
			pTask->length = sizeof(TrackerHeader);
			return ENOENT;
		}

		ppEnd = pStoreGroup->active_servers + active_count;
		for (ppServer=pStoreGroup->active_servers; ppServer<ppEnd; \
			ppServer++)
		{
			memcpy(p, (*ppServer)->ip_addr, IP_ADDRESS_SIZE - 1);
			p += IP_ADDRESS_SIZE - 1;

			long2buff(pStoreGroup->storage_port, p);
			p += FDFS_PROTO_PKG_LEN_SIZE;
		}
	}
	/* 只需要写一个ip地址+端口号 */
	else
	{
		memcpy(p, pStorageServer->ip_addr, IP_ADDRESS_SIZE - 1);
		p += IP_ADDRESS_SIZE - 1;

		long2buff(pStoreGroup->storage_port, p);
		p += FDFS_PROTO_PKG_LEN_SIZE;
	}

	/* 写入上传文件的storage的base_store_path索引 */
	*p++ = (char)write_path_index;

	pTask->length = p - pTask->data;

	return 0;
}

/*
 * 获取一个group的状态信息
 * 发送报文:group_name
 * 返回报文:TrackerGroupStat对象
 */
static int tracker_deal_server_list_one_group(struct fast_task_info *pTask)
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	FDFSGroupInfo *pGroup;
	TrackerGroupStat *pDest;

	if (pTask->length - sizeof(TrackerHeader) != FDFS_GROUP_NAME_MAX_LEN)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length: %d", __LINE__, \
			TRACKER_PROTO_CMD_SERVER_LIST_ONE_GROUP, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader), FDFS_GROUP_NAME_MAX_LEN);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	memcpy(group_name, pTask->data + sizeof(TrackerHeader), \
		FDFS_GROUP_NAME_MAX_LEN);
	*(group_name + FDFS_GROUP_NAME_MAX_LEN) = '\0';

	pGroup = tracker_mem_get_group(group_name);
	if (pGroup == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, group name: %s not exist", \
			__LINE__, pTask->client_ip, group_name);
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	pDest = (TrackerGroupStat *)(pTask->data + sizeof(TrackerHeader));
	memcpy(pDest->group_name, pGroup->group_name, \
			FDFS_GROUP_NAME_MAX_LEN + 1);
	long2buff(pGroup->total_mb, pDest->sz_total_mb);
	long2buff(pGroup->free_mb, pDest->sz_free_mb);
	long2buff(pGroup->trunk_free_mb, pDest->sz_trunk_free_mb);
	long2buff(pGroup->count, pDest->sz_count);
	long2buff(pGroup->storage_port, pDest->sz_storage_port);
	long2buff(pGroup->storage_http_port, pDest->sz_storage_http_port);
	long2buff(pGroup->active_count, pDest->sz_active_count);
	long2buff(pGroup->current_write_server, 
			pDest->sz_current_write_server);
	long2buff(pGroup->store_path_count, pDest->sz_store_path_count);
	long2buff(pGroup->subdir_count_per_path, \
			pDest->sz_subdir_count_per_path);
	long2buff(pGroup->current_trunk_file_id, \
			pDest->sz_current_trunk_file_id);
	pTask->length = sizeof(TrackerHeader) + sizeof(TrackerGroupStat);

	return 0;
}

/*
 * 获取所有group的状态信息
 * 发送报文:为空
 * 返回报文:多个TrackerGroupStat对象
 */
static int tracker_deal_server_list_all_groups(struct fast_task_info *pTask)
{
	FDFSGroupInfo **ppGroup;
	FDFSGroupInfo **ppEnd;
	TrackerGroupStat *groupStats;
	TrackerGroupStat *pDest;

	if (pTask->length - sizeof(TrackerHeader) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length: 0", __LINE__, \
			TRACKER_PROTO_CMD_SERVER_LIST_ALL_GROUPS, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader));
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	groupStats = (TrackerGroupStat *)(pTask->data + sizeof(TrackerHeader));
	pDest = groupStats;
	ppEnd = g_groups.sorted_groups + g_groups.count;
	for (ppGroup=g_groups.sorted_groups; ppGroup<ppEnd; ppGroup++)
	{
		memcpy(pDest->group_name, (*ppGroup)->group_name, \
				FDFS_GROUP_NAME_MAX_LEN + 1);
		long2buff((*ppGroup)->total_mb, pDest->sz_total_mb);
		long2buff((*ppGroup)->free_mb, pDest->sz_free_mb);
		long2buff((*ppGroup)->trunk_free_mb, pDest->sz_trunk_free_mb);
		long2buff((*ppGroup)->count, pDest->sz_count);
		long2buff((*ppGroup)->storage_port, \
				pDest->sz_storage_port);
		long2buff((*ppGroup)->storage_http_port, \
				pDest->sz_storage_http_port);
		long2buff((*ppGroup)->active_count, \
				pDest->sz_active_count);
		long2buff((*ppGroup)->current_write_server, \
				pDest->sz_current_write_server);
		long2buff((*ppGroup)->store_path_count, \
				pDest->sz_store_path_count);
		long2buff((*ppGroup)->subdir_count_per_path, \
				pDest->sz_subdir_count_per_path);
		long2buff((*ppGroup)->current_trunk_file_id, \
				pDest->sz_current_trunk_file_id);
		pDest++;
	}

	pTask->length = sizeof(TrackerHeader) + (pDest - groupStats) * \
			sizeof(TrackerGroupStat);

	return 0;
}

/*
 * 获取指定storage的源storage和同步时间戳
 * 发送报文:group_name+storage_id
 * 返回报文:指定的storage的TrackerStorageSyncReqBody(包括源storage和同步时间戳)
 */
static int tracker_deal_storage_sync_src_req(struct fast_task_info *pTask)
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	FDFSGroupInfo *pGroup;
	char *dest_storage_id;
	FDFSStorageDetail *pDestStorage;

	if (pTask->length - sizeof(TrackerHeader) != \
			FDFS_GROUP_NAME_MAX_LEN + FDFS_STORAGE_ID_MAX_SIZE)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length: %d", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_SYNC_SRC_REQ, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader), \
			FDFS_GROUP_NAME_MAX_LEN + FDFS_STORAGE_ID_MAX_SIZE);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	/* 解析出group_name */
	memcpy(group_name, pTask->data + sizeof(TrackerHeader), \
			FDFS_GROUP_NAME_MAX_LEN);
	*(group_name + FDFS_GROUP_NAME_MAX_LEN) = '\0';
	pGroup = tracker_mem_get_group(group_name);
	if (pGroup == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_name: %s", \
			__LINE__, pTask->client_ip, group_name);
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	/* 解析出storage_id */
	dest_storage_id = pTask->data + sizeof(TrackerHeader) + \
			FDFS_GROUP_NAME_MAX_LEN;
	dest_storage_id[FDFS_STORAGE_ID_MAX_SIZE - 1] = '\0';

	/* 根据id在pGroup中找到对应的storage_server */
	pDestStorage = tracker_mem_get_storage(pGroup, dest_storage_id);
	if (pDestStorage == NULL)
	{
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	if (pDestStorage->status == FDFS_STORAGE_STATUS_INIT || \
		pDestStorage->status == FDFS_STORAGE_STATUS_DELETED || \
		pDestStorage->status == FDFS_STORAGE_STATUS_IP_CHANGED)
	{
		pTask->length = sizeof(TrackerHeader);
		return ENOENT;
	}

	pTask->length = sizeof(TrackerHeader);
	if (pDestStorage->psync_src_server != NULL)
	{
		if (pDestStorage->psync_src_server->status == \
				FDFS_STORAGE_STATUS_OFFLINE \
			|| pDestStorage->psync_src_server->status == \
				FDFS_STORAGE_STATUS_ONLINE \
			|| pDestStorage->psync_src_server->status == \
				FDFS_STORAGE_STATUS_ACTIVE \
			|| pDestStorage->psync_src_server->status == \
				FDFS_STORAGE_STATUS_RECOVERY)
		{
			/* 拼返回报文 */
			TrackerStorageSyncReqBody *pBody;
			pBody = (TrackerStorageSyncReqBody *)(pTask->data + \
						sizeof(TrackerHeader));
			/* 写入此storage的源storage，从源storage同步文件到目标storage */
			strcpy(pBody->src_id, \
					pDestStorage->psync_src_server->id);
			/* 写入同步时间戳 */
			long2buff(pDestStorage->sync_until_timestamp, \
					pBody->until_timestamp);
			pTask->length += sizeof(TrackerStorageSyncReqBody);
		}
		else
		{
			pDestStorage->psync_src_server = NULL;
			/* 将storage的状态信息写入新的storage配置文件中保存 */
			tracker_save_storages();
		}
	}

	return 0;
}

/*
 * 获取一个用于同步的源storage server
 * 发送报文:无
 * 返回报文:当前的storage的TrackerStorageSyncReqBody(包括源storage和同步时间戳)
 */
static int tracker_deal_storage_sync_dest_req(struct fast_task_info *pTask)
{
	TrackerStorageSyncReqBody *pBody;
	FDFSStorageDetail *pSrcStorage;
	FDFSStorageDetail **ppServer;
	FDFSStorageDetail **ppServerEnd;
	int sync_until_timestamp;
	int source_count;
	TrackerClientInfo *pClientInfo;
	
	pClientInfo = (TrackerClientInfo *)pTask->arg;

	pSrcStorage = NULL;
	sync_until_timestamp = (int)g_current_time;

	do
	{
		if (pTask->length - sizeof(TrackerHeader) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				PKG_LEN_PRINTF_FORMAT" is not correct, " \
				"expect length: 0", \
				__LINE__, TRACKER_PROTO_CMD_STORAGE_SYNC_DEST_REQ, \
				pTask->client_ip, pTask->length - \
				(int)sizeof(TrackerHeader));
			pTask->length = sizeof(TrackerHeader);
			return EINVAL;
		}

		if (pClientInfo->pGroup->count <= 1)
		{
			break;
		}

		source_count = 0;
		ppServerEnd = pClientInfo->pGroup->all_servers + \
			      pClientInfo->pGroup->count;
		/* 遍历同group中所有其他的storage ，记下个数 */
		for (ppServer=pClientInfo->pGroup->all_servers; \
				ppServer<ppServerEnd; ppServer++)
		{
			if (strcmp((*ppServer)->id, \
					pClientInfo->pStorage->id) == 0)
			{
				continue;
			}

			if ((*ppServer)->status ==FDFS_STORAGE_STATUS_OFFLINE 
				|| (*ppServer)->status == FDFS_STORAGE_STATUS_ONLINE
				|| (*ppServer)->status == FDFS_STORAGE_STATUS_ACTIVE)
			{
				source_count++;
			}
		}
		if (source_count == 0)
		{
			break;
		}

		/* 找到同group中的一台storage，除了自己 */
		pSrcStorage = tracker_get_group_sync_src_server( \
				pClientInfo->pGroup, pClientInfo->pStorage);
		if (pSrcStorage == NULL)
		{
			pTask->length = sizeof(TrackerHeader);
			return ENOENT;
		}

		pBody=(TrackerStorageSyncReqBody *)(pTask->data+sizeof(TrackerHeader));
		strcpy(pBody->src_id, pSrcStorage->id);
		long2buff(sync_until_timestamp, pBody->until_timestamp);
	} while (0);

	/* 如果没有其他的storage了，直接修改状态为online */
	if (pSrcStorage == NULL)
	{
		pClientInfo->pStorage->status = \
				FDFS_STORAGE_STATUS_ONLINE;
		pClientInfo->pGroup->chg_count++;
		tracker_save_storages();

		pTask->length = sizeof(TrackerHeader);
		return 0;
	}

	/* 修改该storage的同步信息，状态改为同步中 */
	pClientInfo->pStorage->psync_src_server = pSrcStorage;
	pClientInfo->pStorage->sync_until_timestamp = sync_until_timestamp;
	pClientInfo->pStorage->status = FDFS_STORAGE_STATUS_WAIT_SYNC;
	pClientInfo->pGroup->chg_count++;

	tracker_save_storages();

	pTask->length = sizeof(TrackerHeader)+sizeof(TrackerStorageSyncReqBody);
	return 0;
}

/*
 * 获取当前storage的源storage_id及同步时间戳
 * 发送报文:无
 * 返回报文:当前storage的TrackerStorageSyncReqBody对象(包括源storage_id及同步时间戳)
 */
static int tracker_deal_storage_sync_dest_query(struct fast_task_info *pTask)
{
	FDFSStorageDetail *pSrcStorage;
	TrackerClientInfo *pClientInfo;
	
	pClientInfo = (TrackerClientInfo *)pTask->arg;

	if (pTask->length - sizeof(TrackerHeader) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length: 0", \
			__LINE__, TRACKER_PROTO_CMD_STORAGE_SYNC_DEST_QUERY, \
			pTask->client_ip, pTask->length - \
			(int)sizeof(TrackerHeader));
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	pTask->length = sizeof(TrackerHeader);

	/* 获取当前连接的storage的源storage server */
	pSrcStorage = pClientInfo->pStorage->psync_src_server;
	if (pSrcStorage != NULL)
	{
		TrackerStorageSyncReqBody *pBody;
		pBody = (TrackerStorageSyncReqBody *)(pTask->data + \
					sizeof(TrackerHeader));
		strcpy(pBody->src_id, pSrcStorage->id);

		long2buff(pClientInfo->pStorage->sync_until_timestamp, \
				pBody->until_timestamp);
		pTask->length += sizeof(TrackerStorageSyncReqBody);
	}

	return 0;
}

/* 
 * 如果是均衡选择的方式存储
 * 设置free_mb或者启用了trunk_file的话就是trunk_free_mb最大的group为当前文件上传对应的group 
 * 否则什么都不做
 */
static void tracker_find_max_free_space_group()
{
	FDFSGroupInfo **ppGroup;
	FDFSGroupInfo **ppGroupEnd;
	FDFSGroupInfo **ppMaxGroup;
	int result;

	/* 如果不以均衡选择的方式存储，直接返回 */
	if (g_groups.store_lookup != FDFS_STORE_LOOKUP_LOAD_BALANCE)
	{
		return;
	}

	if ((result=pthread_mutex_lock(&lb_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	/* 遍历所有group，找到空闲空间最大的group */
	ppMaxGroup = NULL;
	ppGroupEnd = g_groups.sorted_groups + g_groups.count;
	for (ppGroup=g_groups.sorted_groups; \
		ppGroup<ppGroupEnd; ppGroup++)
	{
		if ((*ppGroup)->active_count > 0)
		{
			if (ppMaxGroup == NULL)
			{
				ppMaxGroup = ppGroup;
			}
			else if ((*ppGroup)->free_mb > (*ppMaxGroup)->free_mb)
			{
				ppMaxGroup = ppGroup;
			}
		}
	}

	if (ppMaxGroup == NULL)
	{
		pthread_mutex_unlock(&lb_thread_lock);
		return;
	}

	/* 检查pGroup的空闲空间是否符合保留空间的限制 */
	if (tracker_check_reserved_space(*ppMaxGroup) \
		|| !g_if_use_trunk_file)
	{
		/* 
		 * 如果符合保留空间的限制或者不需要启用trunk_file
		 * 设置空闲空间最大的group 为当前上传文件的group
		 */
		g_groups.current_write_group = \
			ppMaxGroup - g_groups.sorted_groups;
		pthread_mutex_unlock(&lb_thread_lock);
		return;
	}

	/* 使用trunk_file的流程，选用trunk_file空闲值最大的group */
	ppMaxGroup = NULL;
	for (ppGroup=g_groups.sorted_groups; \
		ppGroup<ppGroupEnd; ppGroup++)
	{
		if ((*ppGroup)->active_count > 0)
		{
			if (ppMaxGroup == NULL)
			{
				ppMaxGroup = ppGroup;
			}
			else if ((*ppGroup)->trunk_free_mb > \
				(*ppMaxGroup)->trunk_free_mb)
			{
				ppMaxGroup = ppGroup;
			}
		}
	}

	if (ppMaxGroup == NULL)
	{
		pthread_mutex_unlock(&lb_thread_lock);
		return;
	}

	g_groups.current_write_group = \
			ppMaxGroup - g_groups.sorted_groups;
	pthread_mutex_unlock(&lb_thread_lock);
}

/* 设置pGroup->free_mb和total_mb为当前group中空闲空间最小的那台storage的值 */
static void tracker_find_min_free_space(FDFSGroupInfo *pGroup)
{
	FDFSStorageDetail **ppServerEnd;
	FDFSStorageDetail **ppServer;

	if (pGroup->active_count == 0)
	{
		return;
	}

	pGroup->total_mb = (*(pGroup->active_servers))->total_mb;
	pGroup->free_mb = (*(pGroup->active_servers))->free_mb;
	ppServerEnd = pGroup->active_servers + pGroup->active_count;
	for (ppServer=pGroup->active_servers+1; \
		ppServer<ppServerEnd; ppServer++)
	{
		if ((*ppServer)->free_mb < pGroup->free_mb)
		{
			pGroup->total_mb = (*ppServer)->total_mb;
			pGroup->free_mb = (*ppServer)->free_mb;
		}
	}
}

/* 
 * 报告和源storage server的最后一次同步时间的处理 
 * 报文体为同组的所有storage_server_id+4字节的同步时间戳
 */
static int tracker_deal_storage_sync_report(struct fast_task_info *pTask)
{
	char *p;
	char *pEnd;
	char *src_id;
	int status;
	int sync_timestamp;
	int src_index;
	int dest_index;
	int nPkgLen;
	FDFSStorageDetail *pSrcStorage;
	TrackerClientInfo *pClientInfo;
	
	pClientInfo = (TrackerClientInfo *)pTask->arg;

	nPkgLen = pTask->length - sizeof(TrackerHeader);
	/* 检查报文长度 */
	if (nPkgLen <= 0 || nPkgLen % (FDFS_STORAGE_ID_MAX_SIZE + 4) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct", \
			__LINE__, TRACKER_PROTO_CMD_STORAGE_SYNC_REPORT, \
			pTask->client_ip, nPkgLen);

		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	do
	{
	/* 
	 * 返回pStorage在pGroup->all_servers中的位置 
	 * 即当前连接所在组的索引值
	 */
	dest_index = tracker_mem_get_storage_index(pClientInfo->pGroup,
			pClientInfo->pStorage);
	if (dest_index < 0 || dest_index >= pClientInfo->pGroup->count)
	{
		status = 0;
		break;
	}

	/* 如果采用循环选择storage的上传方式 */
	if (g_groups.store_server == FDFS_STORE_SERVER_ROUND_ROBIN)
	{
		int min_synced_timestamp;

		min_synced_timestamp = 0;
		pEnd = pTask->data + pTask->length;
		for (p=pTask->data + sizeof(TrackerHeader); p<pEnd; \
			p += (FDFS_STORAGE_ID_MAX_SIZE + 4))
		{
			/* 先保存同步时间戳 */
			sync_timestamp = buff2int(p + FDFS_STORAGE_ID_MAX_SIZE);
			if (sync_timestamp <= 0)
			{
				continue;
			}

			src_id = p;
			*(src_id + (FDFS_STORAGE_ID_MAX_SIZE - 1)) = '\0';
			/* 获取同步时间戳对应的storage server */
			pSrcStorage = tracker_mem_get_storage( \
					pClientInfo->pGroup, src_id);
			if (pSrcStorage == NULL)
			{
				continue;
			}
			if (pSrcStorage->status != FDFS_STORAGE_STATUS_ACTIVE)
			{
				continue;
			}

			/* 获取此storage所在组的索引值 */
			src_index = tracker_mem_get_storage_index( \
					pClientInfo->pGroup, pSrcStorage);
			if (src_index == dest_index || src_index < 0 || \
					src_index >= pClientInfo->pGroup->count)
			{
				continue;
			}

			/* 更新该组的同步时间戳数据 */
			pClientInfo->pGroup->last_sync_timestamps \
				[src_index][dest_index] = sync_timestamp;

			/* 记录下最晚的一次同步时间 */
			if (min_synced_timestamp == 0)
			{
				min_synced_timestamp = sync_timestamp;
			}
			else if (sync_timestamp < min_synced_timestamp)
			{
				min_synced_timestamp = sync_timestamp;
			}
		}

		/* 更新 从其他storage server同步文件过来的最近的一次完整同步的时间戳 */
		if (min_synced_timestamp > 0)
		{
			pClientInfo->pStorage->stat.last_synced_timestamp = \
						   min_synced_timestamp;
		}
	}
	/* 非循环选择的上传方式 */
	else
	{
		int max_synced_timestamp;

		max_synced_timestamp = pClientInfo->pStorage->stat.\
				       last_synced_timestamp;
		pEnd = pTask->data + pTask->length;
		for (p=pTask->data + sizeof(TrackerHeader); p<pEnd; \
			p += (FDFS_STORAGE_ID_MAX_SIZE + 4))
		{
			/* 先保存同步时间戳 */
			sync_timestamp = buff2int(p + FDFS_STORAGE_ID_MAX_SIZE);
			if (sync_timestamp <= 0)
			{
				continue;
			}

			src_id = p;
			*(src_id + (FDFS_STORAGE_ID_MAX_SIZE - 1)) = '\0';
			/* 获取同步时间戳对应的storage server */
			pSrcStorage = tracker_mem_get_storage( \
					pClientInfo->pGroup, src_id);
			if (pSrcStorage == NULL)
			{
				continue;
			}
			if (pSrcStorage->status != FDFS_STORAGE_STATUS_ACTIVE)
			{
				continue;
			}

			/* 获取此storage所在组的索引值 */
			src_index = tracker_mem_get_storage_index( \
					pClientInfo->pGroup, pSrcStorage);
			if (src_index == dest_index || src_index < 0 || \
					src_index >= pClientInfo->pGroup->count)
			{
				continue;
			}

			pClientInfo->pGroup->last_sync_timestamps \
				[src_index][dest_index] = sync_timestamp;

			if (sync_timestamp > max_synced_timestamp)
			{
				max_synced_timestamp = sync_timestamp;
			}
		}

		pClientInfo->pStorage->stat.last_synced_timestamp = \
						    max_synced_timestamp;
	}

	/* 同步时间戳改变达到一定次数，相关数据写入文件 */
	if (++g_storage_sync_time_chg_count % \
			TRACKER_SYNC_TO_FILE_FREQ == 0)
	{
		status = tracker_save_sync_timestamps();
	}
	else
	{
		status = 0;
	}
	} while (0);

	/* 检查相应的改变状态，并将其同步 */
	return tracker_check_and_sync(pTask, status);
}

/*
 * 处理storage server的disk使用情况汇报报文
 * 报文格式为，所在group的每个storage存储路径个数的TrackerStatReportReqBody信息 */
 */
static int tracker_deal_storage_df_report(struct fast_task_info *pTask)
{
	int nPkgLen;
	int i;
	TrackerStatReportReqBody *pStatBuff;
	int64_t *path_total_mbs;
	int64_t *path_free_mbs;
	int64_t old_free_mb;
	TrackerClientInfo *pClientInfo;
	
	pClientInfo = (TrackerClientInfo *)pTask->arg;
	/* 如果该服务是不合法的 */
	if (pClientInfo->pGroup == NULL || pClientInfo->pStorage == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, not join in!", \
			__LINE__, TRACKER_PROTO_CMD_STORAGE_REPORT_DISK_USAGE, \
			pTask->client_ip);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	nPkgLen = pTask->length - sizeof(TrackerHeader);
	/* 检测报文长度 */
	if (nPkgLen != sizeof(TrackerStatReportReqBody) * \
			pClientInfo->pGroup->store_path_count)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			PKG_LEN_PRINTF_FORMAT" is not correct, " \
			"expect length: %d", __LINE__, \
			TRACKER_PROTO_CMD_STORAGE_REPORT_DISK_USAGE, \
			pTask->client_ip, nPkgLen, \
			(int)sizeof(TrackerStatReportReqBody) * \
			pClientInfo->pGroup->store_path_count);
		pTask->length = sizeof(TrackerHeader);
		return EINVAL;
	}

	old_free_mb = pClientInfo->pStorage->free_mb;
	path_total_mbs = pClientInfo->pStorage->path_total_mbs;
	path_free_mbs = pClientInfo->pStorage->path_free_mbs;
	pClientInfo->pStorage->total_mb = 0;
	pClientInfo->pStorage->free_mb = 0;

	pStatBuff = (TrackerStatReportReqBody *)(pTask->data + sizeof(TrackerHeader));
	/* 遍历报文中所有的base_path对应的信息 */
	for (i=0; i<pClientInfo->pGroup->store_path_count; i++)
	{
		/* 获取每个存储路径的disk总计和空闲空间大小 */
		path_total_mbs[i] = buff2long(pStatBuff->sz_total_mb);
		path_free_mbs[i] = buff2long(pStatBuff->sz_free_mb);

		/* 累计所有的总空间和空闲空间的大小 */
		pClientInfo->pStorage->total_mb += path_total_mbs[i];
		pClientInfo->pStorage->free_mb += path_free_mbs[i];

		/* 如果采用均衡存储方式，并且发现有路径比当前使用路径的空闲空间更大 */
		if (g_groups.store_path == FDFS_STORE_PATH_LOAD_BALANCE
				&& path_free_mbs[i] > path_free_mbs[ \
				pClientInfo->pStorage->current_write_path])
		{
			pClientInfo->pStorage->current_write_path = i;
		}

		pStatBuff++;
	}

	/* 
	 * 因为每个storage互为备份
	 * 所以如果一台storage可用空间是最少的，group的free_mb变更为此storage的值
	 */
	if ((pClientInfo->pGroup->free_mb == 0) ||
		(pClientInfo->pStorage->free_mb < pClientInfo->pGroup->free_mb))
	{
		pClientInfo->pGroup->total_mb = pClientInfo->pStorage->total_mb;
		pClientInfo->pGroup->free_mb = pClientInfo->pStorage->free_mb;
	}
	else if (pClientInfo->pStorage->free_mb > old_free_mb)
	{
		/* 设置pGroup->free_mb和total_mb为当前group中空闲空间最小的那台storage的值 */
		tracker_find_min_free_space(pClientInfo->pGroup);
	}

	/* 
	 * 如果是均衡选择的方式存储
	 * 设置free_mb或者启用了trunk_file的话就是trunk_free_mb最大的group为当前文件上传对应的group 
	 * 否则什么都不做
	 */
	tracker_find_max_free_space_group();

	/*
	//logInfo("storage: %s:%d, total_mb=%dMB, free_mb=%dMB\n", \
		pClientInfo->pStorage->ip_addr, \
		pClientInfo->pGroup->storage_port, \
		pClientInfo->pStorage->total_mb, \
		pClientInfo->pStorage->free_mb);
	*/

	/* 接收到发送过来的报文，激活该storage */
	tracker_mem_active_store_server(pClientInfo->pGroup, \
				pClientInfo->pStorage);

	/* 检查相应的改变状态，并将其同步 */
	return tracker_check_and_sync(pTask, 0);
}

/* 
 * 心跳协议处理 
 * storage server会发送自己的所有信息，包括总计上传下载次数等数据给tracker server
 */
static int tracker_deal_storage_beat(struct fast_task_info *pTask)
{
	int nPkgLen;
	int status;
	FDFSStorageStatBuff *pStatBuff;	/* storage server状态信息对应的字符串 */
	FDFSStorageStat *pStat;			/* storage server状态信息 */
	TrackerClientInfo *pClientInfo;
	
	pClientInfo = (TrackerClientInfo *)pTask->arg;

	do 
	{
		/* 获取报文体长度 */
		nPkgLen = pTask->length - sizeof(TrackerHeader);
		if (nPkgLen == 0)
		{
			status = 0;
			break;
		}

		/* 检查报文体长度是否符合storage server状态结构体的大小 */
		if (nPkgLen != sizeof(FDFSStorageStatBuff))
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				PKG_LEN_PRINTF_FORMAT" is not correct, " \
				"expect length: 0 or %d", __LINE__, \
				TRACKER_PROTO_CMD_STORAGE_BEAT, \
				pTask->client_ip, nPkgLen, 
				(int)sizeof(FDFSStorageStatBuff));
			status = EINVAL;
			break;
		}

		/* 报文体就是一个FDFSStorageStatBuff结构体，内部都是相关状态的字符串 */
		pStatBuff = (FDFSStorageStatBuff *)(pTask->data + \
					sizeof(TrackerHeader));
		/* 
		 * 解析报文，将FDFSStorageStatBuff结构体转换为FDFSStorageStat
		 * 放入pTask->arg的TrackerClientInfo对象中
		 */
		pStat = &(pClientInfo->pStorage->stat);

		pStat->total_upload_count = \
			buff2long(pStatBuff->sz_total_upload_count);
		pStat->success_upload_count = \
			buff2long(pStatBuff->sz_success_upload_count);
		pStat->total_append_count = \
			buff2long(pStatBuff->sz_total_append_count);
		pStat->success_append_count = \
			buff2long(pStatBuff->sz_success_append_count);
		pStat->total_modify_count = \
			buff2long(pStatBuff->sz_total_modify_count);
		pStat->success_modify_count = \
			buff2long(pStatBuff->sz_success_modify_count);
		pStat->total_truncate_count = \
			buff2long(pStatBuff->sz_total_truncate_count);
		pStat->success_truncate_count = \
			buff2long(pStatBuff->sz_success_truncate_count);
		pStat->total_download_count = \
			buff2long(pStatBuff->sz_total_download_count);
		pStat->success_download_count = \
			buff2long(pStatBuff->sz_success_download_count);
		pStat->total_set_meta_count = \
			buff2long(pStatBuff->sz_total_set_meta_count);
		pStat->success_set_meta_count = \
			buff2long(pStatBuff->sz_success_set_meta_count);
		pStat->total_delete_count = \
			buff2long(pStatBuff->sz_total_delete_count);
		pStat->success_delete_count = \
			buff2long(pStatBuff->sz_success_delete_count);
		pStat->total_get_meta_count = \
			buff2long(pStatBuff->sz_total_get_meta_count);
		pStat->success_get_meta_count = \
			buff2long(pStatBuff->sz_success_get_meta_count);
		pStat->last_source_update = \
			buff2long(pStatBuff->sz_last_source_update);
		pStat->last_sync_update = \
			buff2long(pStatBuff->sz_last_sync_update);
		pStat->total_create_link_count = \
			buff2long(pStatBuff->sz_total_create_link_count);
		pStat->success_create_link_count = \
			buff2long(pStatBuff->sz_success_create_link_count);
		pStat->total_delete_link_count = \
			buff2long(pStatBuff->sz_total_delete_link_count);
		pStat->success_delete_link_count = \
			buff2long(pStatBuff->sz_success_delete_link_count);
		pStat->total_upload_bytes = \
			buff2long(pStatBuff->sz_total_upload_bytes);
		pStat->success_upload_bytes = \
			buff2long(pStatBuff->sz_success_upload_bytes);
		pStat->total_append_bytes = \
			buff2long(pStatBuff->sz_total_append_bytes);
		pStat->success_append_bytes = \
			buff2long(pStatBuff->sz_success_append_bytes);
		pStat->total_modify_bytes = \
			buff2long(pStatBuff->sz_total_modify_bytes);
		pStat->success_modify_bytes = \
			buff2long(pStatBuff->sz_success_modify_bytes);
		pStat->total_download_bytes = \
			buff2long(pStatBuff->sz_total_download_bytes);
		pStat->success_download_bytes = \
			buff2long(pStatBuff->sz_success_download_bytes);
		pStat->total_sync_in_bytes = \
			buff2long(pStatBuff->sz_total_sync_in_bytes);
		pStat->success_sync_in_bytes = \
			buff2long(pStatBuff->sz_success_sync_in_bytes);
		pStat->total_sync_out_bytes = \
			buff2long(pStatBuff->sz_total_sync_out_bytes);
		pStat->success_sync_out_bytes = \
			buff2long(pStatBuff->sz_success_sync_out_bytes);
		pStat->total_file_open_count = \
			buff2long(pStatBuff->sz_total_file_open_count);
		pStat->success_file_open_count = \
			buff2long(pStatBuff->sz_success_file_open_count);
		pStat->total_file_read_count = \
			buff2long(pStatBuff->sz_total_file_read_count);
		pStat->success_file_read_count = \
			buff2long(pStatBuff->sz_success_file_read_count);
		pStat->total_file_write_count = \
			buff2long(pStatBuff->sz_total_file_write_count);
		pStat->success_file_write_count = \
			buff2long(pStatBuff->sz_success_file_write_count);

		/* 每隔一定次数，就将数据同步到文件中保存 */
		if (++g_storage_stat_chg_count % TRACKER_SYNC_TO_FILE_FREQ == 0)
		{
			/* 将所有storage的状态信息写入文件中保存，成功之后返回0 */
			status = tracker_save_storages();
		}
		else
		{
			status = 0;
		}

		//printf("g_storage_stat_chg_count=%d\n", g_storage_stat_chg_count);

	} while (0);

	if (status == 0)
	{
		/* 发送心跳包过来的storage server如果没有，就加入到相应group中 */
		tracker_mem_active_store_server(pClientInfo->pGroup, \
				pClientInfo->pStorage);
		/* 更新最近一次收到心跳包的时间 */
		pClientInfo->pStorage->stat.last_heart_beat_time = g_current_time;

	}

	//printf("deal heart beat, status=%d\n", status);
	/* 检查相应的改变状态，并将其同步 */
	return tracker_check_and_sync(pTask, status);
}

#define TRACKER_CHECK_LOGINED(pTask) \
	if (((TrackerClientInfo *)pTask->arg)->pGroup == NULL || \
		((TrackerClientInfo *)pTask->arg)->pStorage == NULL) \
	{ \
		pTask->length = sizeof(TrackerHeader); \
		result = EACCES; \
		break; \
	} \

/* tracker任务处理 */
int tracker_deal_task(struct fast_task_info *pTask)
{
	TrackerHeader *pHeader;
	int result;

	/* 解析出报文头 */
	pHeader = (TrackerHeader *)pTask->data;
	switch(pHeader->cmd)
	{
		/* 心跳包报文处理 */
		case TRACKER_PROTO_CMD_STORAGE_BEAT:
			TRACKER_CHECK_LOGINED(pTask)
			result = tracker_deal_storage_beat(pTask);
			break;
		/* 报告和源storage server的最后一次同步时间戳的处理 */
		case TRACKER_PROTO_CMD_STORAGE_SYNC_REPORT:
			TRACKER_CHECK_LOGINED(pTask)
			result = tracker_deal_storage_sync_report(pTask);
			break;
		/* 报告storage上disk使用情况的处理 */
		case TRACKER_PROTO_CMD_STORAGE_REPORT_DISK_USAGE:
			TRACKER_CHECK_LOGINED(pTask)
			result = tracker_deal_storage_df_report(pTask);
			break;
		/* 处理storage向tracker发送的join请求报文 */
		case TRACKER_PROTO_CMD_STORAGE_JOIN:
			result = tracker_deal_storage_join(pTask);
			break;
		/* storage汇报状态信息的报文处理  */
		case TRACKER_PROTO_CMD_STORAGE_REPORT_STATUS:
			result = tracker_deal_storage_report_status(pTask);
			break;
		/* 获取指定的group_name中的storage的状态信息 */
		case TRACKER_PROTO_CMD_STORAGE_GET_STATUS:
			result = tracker_deal_server_get_storage_status(pTask);
			break;
		/* 获取指定的group_name中的storage的id */
		case TRACKER_PROTO_CMD_STORAGE_GET_SERVER_ID:
			result = tracker_deal_get_storage_id(pTask);
			break;
		/* 获取从start_index开始的所有storage的ids信息 */
		case TRACKER_PROTO_CMD_STORAGE_FETCH_STORAGE_IDS:
			result = tracker_deal_fetch_storage_ids(pTask);
			break;
		/* 更新多个storage_server的状态信息 */
		case TRACKER_PROTO_CMD_STORAGE_REPLICA_CHG:
			TRACKER_CHECK_LOGINED(pTask)
			result = tracker_deal_storage_replica_chg(pTask);
			break;
		/* 获取要下载该文件需访问的storage */
		case TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE:
			result = tracker_deal_service_query_fetch_update( \
					pTask, pHeader->cmd);
			break;
		/* 获取更新文件需访问的storage */
		case TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE:
			result = tracker_deal_service_query_fetch_update( \
					pTask, pHeader->cmd);
			break;
		/* 遍历所有storage根据时间戳获取能下载该文件的storage的全部列表 */
		case TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ALL:
			result = tracker_deal_service_query_fetch_update( \
					pTask, pHeader->cmd);
			break;
		/* 获取一个storage来上传文件，不指定group */
		case TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE:
			result = tracker_deal_service_query_storage( \
					pTask, pHeader->cmd);
			break;
		/* 获取一个storage来上传文件，指定group */
		case TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE:
			result = tracker_deal_service_query_storage( \
					pTask, pHeader->cmd);
			break;
		/* 获取能上传文件的所有的storage，不指定group */
		case TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ALL:
			result = tracker_deal_service_query_storage( \
					pTask, pHeader->cmd);
			break;
		/* 获取能上传文件的所有的storage，指定group */
		case TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ALL:
			result = tracker_deal_service_query_storage( \
					pTask, pHeader->cmd);
			break;
		/* 获取一个group的状态信息 */
		case TRACKER_PROTO_CMD_SERVER_LIST_ONE_GROUP:
			result = tracker_deal_server_list_one_group(pTask);
			break;
		/* 获取所有group的状态信息 */
		case TRACKER_PROTO_CMD_SERVER_LIST_ALL_GROUPS:
			result = tracker_deal_server_list_all_groups(pTask);
			break;
		/* 获取指定group中所有storage的状态信息 */
		case TRACKER_PROTO_CMD_SERVER_LIST_STORAGE:
			result = tracker_deal_server_list_group_storages(pTask);
			break;
		/* 获取指定storage的源storage和同步时间戳 */
		case TRACKER_PROTO_CMD_STORAGE_SYNC_SRC_REQ:
			result = tracker_deal_storage_sync_src_req(pTask);
			break;
		/* 获取一个用于同步的源storage server */
		case TRACKER_PROTO_CMD_STORAGE_SYNC_DEST_REQ:
			TRACKER_CHECK_LOGINED(pTask)
			result = tracker_deal_storage_sync_dest_req(pTask);
			break;
		/* 同步完成通知报文 */
		case TRACKER_PROTO_CMD_STORAGE_SYNC_NOTIFY:
			result = tracker_deal_storage_sync_notify(pTask);
			break;
		/*  获取当前storage的源storage_id及同步时间戳 */
		case TRACKER_PROTO_CMD_STORAGE_SYNC_DEST_QUERY:
			result = tracker_deal_storage_sync_dest_query(pTask);
			break;
		/* 移除指定group中的一个storage */
		case TRACKER_PROTO_CMD_SERVER_DELETE_STORAGE:
			result = tracker_deal_server_delete_storage(pTask);
			break;
		/* 设置trunk_server */
		case TRACKER_PROTO_CMD_SERVER_SET_TRUNK_SERVER:
			result = tracker_deal_server_set_trunk_server(pTask);
			break;
		/* storage IP变更报文 */
		case TRACKER_PROTO_CMD_STORAGE_REPORT_IP_CHANGED:
			result = tracker_deal_storage_report_ip_changed(pTask);
			break;
		/* 获取changelog文件的内容 */
		case TRACKER_PROTO_CMD_STORAGE_CHANGELOG_REQ:
			result = tracker_deal_changelog_req(pTask);
			break;
		/* 获取一些配置的参数 */
		case TRACKER_PROTO_CMD_STORAGE_PARAMETER_REQ:
			result = tracker_deal_parameter_req(pTask);
			break;
		/* 主机下线，结束该任务 */
		case FDFS_PROTO_CMD_QUIT:
			task_finish_clean_up(pTask);
			return 0;
		/* 测试主机是否处于正常通信状态 */
		case FDFS_PROTO_CMD_ACTIVE_TEST:
			result = tracker_deal_active_test(pTask);
			break;
		/* tracker获取其他tracker server的状态信息 */
		case TRACKER_PROTO_CMD_TRACKER_GET_STATUS:
			result = tracker_deal_get_tracker_status(pTask);
			break;
		/* 开始获取系统数据文件信息，同步数据到文件中，加锁 */
		case TRACKER_PROTO_CMD_TRACKER_GET_SYS_FILES_START:
			result = tracker_deal_get_sys_files_start(pTask);
			break;
		/* 获取一个系统数据文件，指定文件名索引，及偏移量，多次传输 */
		case TRACKER_PROTO_CMD_TRACKER_GET_ONE_SYS_FILE:
			result = tracker_deal_get_one_sys_file(pTask);
			break;
		/* 结束获取系统数据文件信息，解锁 */
		case TRACKER_PROTO_CMD_TRACKER_GET_SYS_FILES_END:
			result = tracker_deal_get_sys_files_end(pTask);
			break;
		/* 汇报当前group的trunk_file_id */
		case TRACKER_PROTO_CMD_STORAGE_REPORT_TRUNK_FID:
			TRACKER_CHECK_LOGINED(pTask)
			result = tracker_deal_report_trunk_fid(pTask);
			break;
		/* 获取当前group的trunk_file_id */
		case TRACKER_PROTO_CMD_STORAGE_FETCH_TRUNK_FID:
			TRACKER_CHECK_LOGINED(pTask)
			result = tracker_deal_get_trunk_fid(pTask);
			break;
		/* 汇报当前group的trunk_server的空闲空间 */
		case TRACKER_PROTO_CMD_STORAGE_REPORT_TRUNK_FREE:
			TRACKER_CHECK_LOGINED(pTask)
			result = tracker_deal_report_trunk_free_space(pTask);
			break;
		/* 发送报文给tracker leader获取所有group的trunk_server_id */
		case TRACKER_PROTO_CMD_TRACKER_PING_LEADER:
			result = tracker_deal_ping_leader(pTask);
			break;
		/* 指定下一个leader_server */
		case TRACKER_PROTO_CMD_TRACKER_NOTIFY_NEXT_LEADER:
			result = tracker_deal_notify_next_leader(pTask);
			break;
		/* 设置接收到报文中的ip:port对应的主机为tracker_leader_server */
		case TRACKER_PROTO_CMD_TRACKER_COMMIT_NEXT_LEADER:
			result = tracker_deal_commit_next_leader(pTask);
			break;
		default:
			logError("file: "__FILE__", line: %d, "  \
				"client ip: %s, unkown cmd: %d", \
				__LINE__, pTask->client_ip, \
				pHeader->cmd);
			pTask->length = sizeof(TrackerHeader);
			result = EINVAL;
			break;
	}
	
	/* 在前面处理的逻辑过程中会拼好报文体，然后发送应答报文给对方 */
	pHeader = (TrackerHeader *)pTask->data;
	/* 设置应答状态 */
	pHeader->status = result;
	pHeader->cmd = TRACKER_PROTO_CMD_RESP;
	/* 计算报文体的长度 */
	long2buff(pTask->length - sizeof(TrackerHeader), pHeader->pkg_len);

	/* 将发送报文的事件加入到IO事件集合中 */
	send_add_event(pTask);

	return 0;
}

