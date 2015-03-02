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
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fdfs_client.h"
#include "logger.h"

static void usage(char *argv[])
{
	printf("Usage: %s <config_file> <local_filename> " \
		"[storage_ip:port] [store_path_index]\n", argv[0]);
}

int main(int argc, char *argv[])
{
	char *conf_filename;		//配置文件名
	char *local_filename;		//要上传的文件名
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];	//Group Name
	ConnectionInfo *pTrackerServer;
	int result;
	int store_path_index;
	ConnectionInfo storageServer;
	char file_id[128];
	
	if (argc < 3)	//如果参数少于3个，提示错误信息
	{
		usage(argv);
		return 1;
	}

	log_init();
	g_log_context.log_level = LOG_ERR;
	ignore_signal_pipe();

	conf_filename = argv[1];
	if ((result=fdfs_client_init(conf_filename)) != 0)	//根据参数中配置文件进行客户端初始化
	{
		return result;
	}

	pTrackerServer = tracker_get_connection();	//获取跟TrackerServer的连接
	if (pTrackerServer == NULL)		//如果没有找到TrackerServer，则关闭客户端
	{
		fdfs_client_destroy();
		return errno != 0 ? errno : ECONNREFUSED;
	}

	local_filename = argv[2];	//从参数中获取本地上传文件名
	*group_name = '\0';
	if (argc >= 4)		//如果有四个参数或以上，用户指定了要连接的Storage Server，否则主动查询可用的Storage Server
	{
		const char *pPort;
		const char *pIpAndPort;

		pIpAndPort = argv[3];	//第四个参数应该输入要指定的Storage Server的IP地址及端口号
		pPort = strchr(pIpAndPort, ':');
		if (pPort == NULL)	//如果第四个参数中没有":"号（端口号），关闭客户端，提示错误信息
		{
			fdfs_client_destroy();
			fprintf(stderr, "invalid storage ip address and " \
				"port: %s\n", pIpAndPort);
			usage(argv);
			return 1;
		}

		storageServer.sock = -1;
		snprintf(storageServer.ip_addr, sizeof(storageServer.ip_addr), 	//保留IP地址，截取掉后面的端口号
			 "%.*s", (int)(pPort - pIpAndPort), pIpAndPort);
		storageServer.port = atoi(pPort + 1);	//去除掉冒号，获取端口号
		if (argc >= 5)
		{
			store_path_index = atoi(argv[4]);
		}
		else
		{
			store_path_index = -1;
		}
	}
	//获取Tracker连接后向该Tracker查询当前哪个Storage可用
	else if ((result=tracker_query_storage_store(pTrackerServer,
	          &storageServer, group_name, &store_path_index)) != 0)
	{
		fdfs_client_destroy();
		fprintf(stderr, "tracker_query_storage fail, " \
			"error no: %d, error info: %s\n", \
			result, STRERROR(result));
		return result;
	}
	
	result = storage_upload_by_filename1(pTrackerServer, \
			&storageServer, store_path_index, \
			local_filename, NULL, \
			NULL, 0, group_name, file_id);
	if (result == 0)	//文件上传成功，显示文件ID，包括组名和文件名
	{
		printf("%s\n", file_id);
	}
	else
	{
		fprintf(stderr, "upload file fail, " \
			"error no: %d, error info: %s\n", \
			result, STRERROR(result));
	}

	tracker_disconnect_server_ex(pTrackerServer, true);		//关闭和Tracker Server的连接
	fdfs_client_destroy();

	return result;
}

