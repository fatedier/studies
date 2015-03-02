/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/


#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/statvfs.h>
#include <sys/param.h>
#include "fdfs_define.h"
#include "logger.h"
#include "fdfs_global.h"
#include "sockopt.h"
#include "shared_func.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "storage_global.h"
#include "storage_func.h"
#include "storage_ip_changed_dealer.h"

/* 
 * 向指定tracker发送报文获取changelog文件的内容 
 * 发送报文:空或者group_name+storage_id 
 * 返回报文:changelog文件中的一段，由pTask->storage的偏移量来决定 
 * 解析出同组的storage的变更记录，修改或重命名相关的用于sync或trunk的mark_file文件
 */
static int storage_do_changelog_req(ConnectionInfo *pTrackerServer)
{
	char out_buff[sizeof(TrackerHeader) + FDFS_GROUP_NAME_MAX_LEN + \
			FDFS_STORAGE_ID_MAX_SIZE];
	TrackerHeader *pHeader;
	int result;

	memset(out_buff, 0, sizeof(out_buff));
	pHeader = (TrackerHeader *)out_buff;

	long2buff(FDFS_GROUP_NAME_MAX_LEN + FDFS_STORAGE_ID_MAX_SIZE, \
		pHeader->pkg_len);
	pHeader->cmd = TRACKER_PROTO_CMD_STORAGE_CHANGELOG_REQ;
	strcpy(out_buff + sizeof(TrackerHeader), g_group_name);
	strcpy(out_buff + sizeof(TrackerHeader) + FDFS_GROUP_NAME_MAX_LEN,
		g_my_server_id_str);
	if((result=tcpsenddata_nb(pTrackerServer->sock, out_buff, \
		sizeof(out_buff), g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, send data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, STRERROR(result));
		return result;
	}

	/* 
	 * 解析返回报文
	 * 返回报文:changelog文件中的一段，由pTask->storage的偏移量来决定 
	 * 解析出同组的storage的变更记录，修改或重命名相关的用于sync或trunk的mark_file文件
	 */
	return tracker_deal_changelog_response(pTrackerServer);
}

/* 向指定tracker发送ip变更的报文 */
static int storage_report_ip_changed(ConnectionInfo *pTrackerServer)
{
	char out_buff[sizeof(TrackerHeader) + FDFS_GROUP_NAME_MAX_LEN + \
		2 * IP_ADDRESS_SIZE];
	char in_buff[1];
	char *pInBuff;
	TrackerHeader *pHeader;
	int result;
	int64_t in_bytes;

	memset(out_buff, 0, sizeof(out_buff));
	pHeader = (TrackerHeader *)out_buff;

	long2buff(FDFS_GROUP_NAME_MAX_LEN+2*IP_ADDRESS_SIZE, pHeader->pkg_len);
	pHeader->cmd = TRACKER_PROTO_CMD_STORAGE_REPORT_IP_CHANGED;
	strcpy(out_buff + sizeof(TrackerHeader), g_group_name);
	strcpy(out_buff + sizeof(TrackerHeader) + FDFS_GROUP_NAME_MAX_LEN, \
		g_last_storage_ip);
	strcpy(out_buff + sizeof(TrackerHeader) + FDFS_GROUP_NAME_MAX_LEN + \
		IP_ADDRESS_SIZE, g_tracker_client_ip);

	if((result=tcpsenddata_nb(pTrackerServer->sock, out_buff, \
		sizeof(out_buff), g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, result, STRERROR(result));
		return result;
	}

	pInBuff = in_buff;
	result = fdfs_recv_response(pTrackerServer, \
                &pInBuff, 0, &in_bytes);

	if (result == 0 || result == EALREADY || result == ENOENT)
	{
		return 0;
	}
	else
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, recv data fail or " \
			"response status != 0, " \
			"errno: %d, error info: %s", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, result, STRERROR(result));
		return result == EBUSY ? 0 : result;
	}
}

/* 获取与tracker建立连接的自身的ip地址 */
int storage_get_my_tracker_client_ip()
{
	ConnectionInfo *pGlobalServer;
	ConnectionInfo *pTServer;
	ConnectionInfo *pTServerEnd;
	ConnectionInfo trackerServer;
	char tracker_client_ip[IP_ADDRESS_SIZE];
	int success_count;
	int result;
	int i;

	result = 0;
	success_count = 0;
	pTServer = &trackerServer;
	pTServerEnd = g_tracker_group.servers + g_tracker_group.server_count;

	/* 循环遍历连接所有设置的tracker的ip:port，成功的记下连接用的自身的ip地址 */
	while (success_count == 0 && g_continue_flag)
	{
		for (pGlobalServer=g_tracker_group.servers; pGlobalServer<pTServerEnd; \
				pGlobalServer++)
		{
			memcpy(pTServer, pGlobalServer, sizeof(ConnectionInfo));
			/* 每个tracker尝试连接3次 */
			for (i=0; i < 3; i++)
			{
				pTServer->sock = socket(AF_INET, SOCK_STREAM, 0);
				if(pTServer->sock < 0)
				{
					result = errno != 0 ? errno : EPERM;
					logError("file: "__FILE__", line: %d, " \
						"socket create failed, errno: %d, " \
						"error info: %s.", \
						__LINE__, result, STRERROR(result));
					sleep(5);
					break;
				}

				if (g_client_bind_addr && *g_bind_addr != '\0')
				{
					/* 绑定指定ip地址和端口，如果不指定addr，则使用INADDR_ANY，通常为0.0.0.0 */
					socketBind(pTServer->sock, g_bind_addr, 0);
				}

				/* 设置socket为非阻塞模式 */
				if (tcpsetnonblockopt(pTServer->sock) != 0)
				{
					close(pTServer->sock);
					pTServer->sock = -1;
					sleep(5);
					continue;
				}

				/* 建立与server_ip:server_port之间的非阻塞连接，超时时间为timeout */
				if ((result=connectserverbyip_nb(pTServer->sock, \
					pTServer->ip_addr, pTServer->port, \
					g_fdfs_connect_timeout)) == 0)
				{
					break;
				}

				close(pTServer->sock);
				pTServer->sock = -1;
				sleep(5);
			}

			if (pTServer->sock < 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"connect to tracker server %s:%d fail, " \
					"errno: %d, error info: %s", \
					__LINE__, pTServer->ip_addr, pTServer->port, \
					result, STRERROR(result));

				continue;
			}

			/* 根据socket描述符获取自身的ip地址 */
			getSockIpaddr(pTServer->sock,tracker_client_ip,IP_ADDRESS_SIZE);
			if (*g_tracker_client_ip == '\0')
			{
				strcpy(g_tracker_client_ip, tracker_client_ip);
			}
			else if (strcmp(tracker_client_ip, g_tracker_client_ip) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"as a client of tracker server %s:%d, " \
					"my ip: %s != client ip: %s of other " \
					"tracker client", __LINE__, \
					pTServer->ip_addr, pTServer->port, \
					tracker_client_ip, g_tracker_client_ip);

				close(pTServer->sock);
				return EINVAL;
			}

			/* 向tracker_server发送退出报文 */
			fdfs_quit(pTServer);
			close(pTServer->sock);
			success_count++;
		}
	}

	if (!g_continue_flag)
	{
		return EINTR;
	}

	return 0;
}

/*  向所有的tracker server发送ip变更的报文 */
static int storage_report_storage_ip_addr()
{
	ConnectionInfo *pGlobalServer;
	ConnectionInfo *pTServer;
	ConnectionInfo *pTServerEnd;
	ConnectionInfo trackerServer;
	int success_count;
	int result;
	int i;

	result = 0;
	success_count = 0;
	pTServer = &trackerServer;
	pTServerEnd = g_tracker_group.servers + g_tracker_group.server_count;

	logDebug("file: "__FILE__", line: %d, " \
		"last my ip is %s, current my ip is %s", \
		__LINE__, g_last_storage_ip, g_tracker_client_ip);

	if (*g_last_storage_ip == '\0')
	{
		return storage_write_to_sync_ini_file();
	}
	else if (strcmp(g_tracker_client_ip, g_last_storage_ip) == 0)
	{
		return 0;
	}

	success_count = 0;
	/* 向每一个tracker server发送ip变更的报文 */
	while (success_count == 0 && g_continue_flag)
	{
		for (pGlobalServer=g_tracker_group.servers; pGlobalServer<pTServerEnd; \
				pGlobalServer++)
		{
			memcpy(pTServer, pGlobalServer, sizeof(ConnectionInfo));
			for (i=0; i < 3; i++)
			{
				pTServer->sock = socket(AF_INET, SOCK_STREAM, 0);
				if(pTServer->sock < 0)
				{
					result = errno != 0 ? errno : EPERM;
					logError("file: "__FILE__", line: %d, " \
						"socket create failed, errno: %d, " \
						"error info: %s.", \
						__LINE__, result, STRERROR(result));
					sleep(5);
					break;
				}

				if (g_client_bind_addr && *g_bind_addr != '\0')
				{
					socketBind(pTServer->sock, g_bind_addr, 0);
				}

				if (tcpsetnonblockopt(pTServer->sock) != 0)
				{
					close(pTServer->sock);
					pTServer->sock = -1;
					sleep(1);
					continue;
				}

				if ((result=connectserverbyip_nb(pTServer->sock, \
					pTServer->ip_addr, pTServer->port, \
					g_fdfs_connect_timeout)) == 0)
				{
					break;
				}

				close(pTServer->sock);
				pTServer->sock = -1;
				sleep(1);
			}

			if (pTServer->sock < 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"connect to tracker server %s:%d fail, " \
					"errno: %d, error info: %s", \
					__LINE__, pTServer->ip_addr, pTServer->port, \
					result, STRERROR(result));

				continue;
			}

			/* 向指定tracker发送ip变更的报文 */
			if ((result=storage_report_ip_changed(pTServer)) == 0)
			{
				success_count++;
			}
			else
			{
				sleep(1);
			}

			fdfs_quit(pTServer);
			close(pTServer->sock);
		}
	}

	if (!g_continue_flag)
	{
		return EINTR;
	}

	/* storage将同步信息写入磁盘保存，供storage重启时使用 */
	return storage_write_to_sync_ini_file();
}

/* 
 * 向所有tracker发送报文获取changelog文件的内容 ，直到有一台成功返回
 * 发送报文:空或者group_name+storage_id 
 * 返回报文:changelog文件中的一段，由pTask->storage的偏移量来决定 
 * 解析出同组的storage的变更记录，修改或重命名相关的用于sync或trunk的mark_file文件
 */
int storage_changelog_req()
{
	ConnectionInfo *pGlobalServer;
	ConnectionInfo *pTServer;
	ConnectionInfo *pTServerEnd;
	ConnectionInfo trackerServer;
	int success_count;
	int result;
	int i;

	result = 0;
	success_count = 0;
	pTServer = &trackerServer;
	pTServerEnd = g_tracker_group.servers + g_tracker_group.server_count;

	while (success_count == 0 && g_continue_flag)
	{
		for (pGlobalServer=g_tracker_group.servers; pGlobalServer<pTServerEnd; \
				pGlobalServer++)
		{
			memcpy(pTServer, pGlobalServer, sizeof(ConnectionInfo));
			for (i=0; i < 3; i++)
			{
				pTServer->sock = socket(AF_INET, SOCK_STREAM, 0);
				if(pTServer->sock < 0)
				{
					result = errno != 0 ? errno : EPERM;
					logError("file: "__FILE__", line: %d, " \
						"socket create failed, errno: %d, " \
						"error info: %s.", \
						__LINE__, result, STRERROR(result));
					sleep(5);
					break;
				}

				if (g_client_bind_addr && *g_bind_addr != '\0')
				{
					socketBind(pTServer->sock, g_bind_addr, 0);
				}

				if (tcpsetnonblockopt(pTServer->sock) != 0)
				{
					close(pTServer->sock);
					pTServer->sock = -1;
					sleep(1);
					continue;
				}

				if ((result=connectserverbyip_nb(pTServer->sock, \
					pTServer->ip_addr, pTServer->port, \
					g_fdfs_connect_timeout)) == 0)
				{
					break;
				}

				close(pTServer->sock);
				pTServer->sock = -1;
				sleep(1);
			}

			if (pTServer->sock < 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"connect to tracker server %s:%d fail, " \
					"errno: %d, error info: %s", \
					__LINE__, pTServer->ip_addr, pTServer->port, \
					result, STRERROR(result));

				continue;
			}

			/* 
			 * 向指定tracker发送报文获取changelog文件的内容 
			 * 发送报文:空或者group_name+storage_id 
			 * 返回报文:changelog文件中的一段，由pTask->storage的偏移量来决定 
			 * 解析出同组的storage的变更记录，修改或重命名相关的用于sync或trunk的mark_file文件
			 */
			result = storage_do_changelog_req(pTServer);
			if (result == 0 || result == ENOENT)
			{
				success_count++;
			}
			else
			{
				sleep(1);
			}

			fdfs_quit(pTServer);
			close(pTServer->sock);
		}
	}

	if (!g_continue_flag)
	{
		return EINTR;
	}

	return 0;
}

/* 
 * 如果ip改变后需要集群自动调整，向tracker发送报文通知
 * 并获取changelog文件更新 有变更的storage相对应的mark_file文件
 */
int storage_check_ip_changed()
{
	int result;

	if ((!g_storage_ip_changed_auto_adjust) || g_use_storage_id)
	{
		return 0;
	}

	/* 
	 * 当storage server ip改变时集群需要自动调整 
	 * 向所有的tracker server发送ip变更的报文
	 */
	if ((result=storage_report_storage_ip_addr()) != 0)
	{
		return result;
	}

	if (*g_last_storage_ip == '\0') //first run
	{
		return 0;
	}

	/* 
	 * 向所有tracker发送报文获取changelog文件的内容 ，直到有一台成功返回
	 * 发送报文:空或者group_name+storage_id 
	 * 返回报文:changelog文件中的一段，由pTask->storage的偏移量来决定 
	 * 解析出同组的storage的变更记录，修改或重命名相关的用于sync或trunk的mark_file文件
	 */
	return storage_changelog_req();
}

