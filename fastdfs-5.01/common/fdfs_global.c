/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <errno.h>
#include "logger.h"
#include "fdfs_global.h"

int g_fdfs_connect_timeout = DEFAULT_CONNECT_TIMEOUT;	/* 连接超时时间 */
int g_fdfs_network_timeout = DEFAULT_NETWORK_TIMEOUT;	/* 套接口读写超时时间 */
char g_fdfs_base_path[MAX_PATH_SIZE] = {'/', 't', 'm', 'p', '\0'};	/* 服务器存储数据和日志文件根目录 */
Version g_fdfs_version = {5, 1};		/* 版本号 */
bool g_use_connection_pool = false;	/*是否使用连接池 */
ConnectionPool g_connection_pool;		/* 连接池 */
int g_connection_pool_max_idle_time = 3600;	/* 连接池中连接最大空闲时间 */

/*
 * data filename format:
 * HH/HH/filename: HH for 2 uppercase hex chars
 * 检查文件名格式是否正确
 */
int fdfs_check_data_filename(const char *filename, const int len)
{
	if (len < 6)
	{
		logError("file: "__FILE__", line: %d, " \
			"the length=%d of filename \"%s\" is too short", \
			__LINE__, len, filename);
		return EINVAL;
	}

	/* 是否是16进制数 */
	if (!IS_UPPER_HEX(*filename) || !IS_UPPER_HEX(*(filename+1)) || \
	    *(filename+2) != '/' || \
	    !IS_UPPER_HEX(*(filename+3)) || !IS_UPPER_HEX(*(filename+4)) || \
	    *(filename+5) != '/')
	{
		logError("file: "__FILE__", line: %d, " \
			"the format of filename \"%s\" is invalid", \
			__LINE__, filename);
		return EINVAL;
	}

	if (strchr(filename + 6, '/') != NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"the format of filename \"%s\" is invalid", \
			__LINE__, filename);
		return EINVAL;
	}

	return 0;
}

/* 根据主文件及prefix_name以及文件后缀名生成从文件的名字 */
int fdfs_gen_slave_filename(const char *master_filename, \
		const char *prefix_name, const char *ext_name, \
		char *filename, int *filename_len)
{
	/* 从文件名 = 主文件名 + 从文件前缀名 + 从文件扩展名 */
	char true_ext_name[FDFS_FILE_EXT_NAME_MAX_LEN + 2];
	char *pDot;
	int master_file_len;

	master_file_len = strlen(master_filename);
	if (master_file_len < 28 + FDFS_FILE_EXT_NAME_MAX_LEN)
	{
		logError("file: "__FILE__", line: %d, " \
			"master filename \"%s\" is invalid", \
			__LINE__, master_filename);
		return EINVAL;
	}

	pDot = strchr(master_filename + (master_file_len - \
			(FDFS_FILE_EXT_NAME_MAX_LEN + 1)), '.');
	/* 如果有文件后缀名 */
	if (ext_name != NULL)
	{
		if (*ext_name == '\0')
		{
			*true_ext_name = '\0';
		}
		else if (*ext_name == '.')
		{
			snprintf(true_ext_name, sizeof(true_ext_name), \
				"%s", ext_name);
		}
		else
		{
			snprintf(true_ext_name, sizeof(true_ext_name), \
				".%s", ext_name);
		}
	}
	else
	{
		if (pDot == NULL)
		{
			*true_ext_name = '\0';
		}
		else
		{
			strcpy(true_ext_name, pDot);
		}
	}

	if (*true_ext_name == '\0' && strcmp(prefix_name, "-m") == 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"prefix_name \"%s\" is invalid", \
			__LINE__, prefix_name);
		return EINVAL;
	}

	/* when prefix_name is empty, the extension name of master file and 
	   slave file can not be same
	*/
	if ((*prefix_name == '\0') && ((pDot == NULL && *true_ext_name == '\0')
		 || (pDot != NULL && strcmp(pDot, true_ext_name) == 0)))
	{
		logError("file: "__FILE__", line: %d, " \
			"empty prefix_name is not allowed", __LINE__);
		return EINVAL;
	}

	if (pDot == NULL)
	{
		*filename_len = sprintf(filename, "%s%s%s", master_filename, \
			prefix_name, true_ext_name);
	}
	else
	{
		*filename_len = pDot - master_filename;
		memcpy(filename, master_filename, *filename_len);
		*filename_len += sprintf(filename + *filename_len, "%s%s", \
			prefix_name, true_ext_name);
	}

	return 0;
}

