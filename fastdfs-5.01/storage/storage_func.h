/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_func.h

#ifndef _STORAGE_FUNC_H_
#define _STORAGE_FUNC_H_

#include "tracker_types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef char * (*get_filename_func)(const void *pArg, \
			char *full_filename);

/* 将fd所指定文件清空，之后将buff中的内容写入fd指定的文件 */
int storage_write_to_fd(int fd, get_filename_func filename_func, \
		const void *pArg, const char *buff, const int len);

/* 初始化的相关工作，加载配置文件，创建存储目录，恢复磁盘数据等 */
int storage_func_init(const char *filename, \
		char *bind_addr, const int addr_size);

/* 销毁全局信息存储的相关空间及关闭资源占用 */
int storage_func_destroy();

/* 将storage的当前状态信息写入磁盘 */
int storage_write_to_stat_file();

/* storage将同步信息写入磁盘保存，供storage重启时使用 */
int storage_write_to_sync_ini_file();

/* 检查指定的Storage Server是否就是自身 */
bool storage_server_is_myself(const FDFSStorageBrief *pStorageBrief);

/* 检查指定的id是否就是自身的id */
bool storage_id_is_myself(const char *storage_id);

/* 设置指定目录的所属组和用户 */
#define STORAGE_CHOWN(path, current_uid, current_gid) \
	if (!(g_run_by_gid == current_gid && g_run_by_uid == current_uid)) \
	{ \
		if (chown(path, g_run_by_uid, g_run_by_gid) != 0) \
		{ \
			logError("file: "__FILE__", line: %d, " \
				"chown \"%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, path, \
				errno, STRERROR(errno)); \
			return errno != 0 ? errno : EPERM; \
		} \
	}


/* 修改文件的所属group和user */
#define STORAGE_FCHOWN(fd, path, current_uid, current_gid) \
	if (!(g_run_by_gid == current_gid && g_run_by_uid == current_uid)) \
	{ \
		if (fchown(fd, g_run_by_uid, g_run_by_gid) != 0) \
		{ \
			logError("file: "__FILE__", line: %d, " \
				"chown \"%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, path, \
				errno, STRERROR(errno)); \
			return errno != 0 ? errno : EPERM; \
		} \
	}


/*
int write_serialized(int fd, const char *buff, size_t count, const bool bSync);
int fsync_serialized(int fd);
int recv_file_serialized(int sock, const char *filename, \
		const int64_t file_bytes);
*/

#ifdef __cplusplus
}
#endif

#endif
