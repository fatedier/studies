/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdfs_shared_func.h

#ifndef _FDFS_SHARED_FUNC_H
#define _FDFS_SHARED_FUNC_H

#include "common_define.h"
#include "ini_file_reader.h"
#include "tracker_types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern FDFSStorageIdInfo *g_storage_ids_by_ip;  //sorted by group name and storage IP
extern FDFSStorageIdInfo **g_storage_ids_by_id;  //sorted by storage ID
extern int g_storage_id_count;		  //storage id count

/* 在所有的tracker server中查找指定ip:port的tracker的index */
int fdfs_get_tracker_leader_index_ex(TrackerServerGroup *pServerGroup, \
		const char *leaderIp, const int leaderPort);

/* 设置每台storage server保留存储空间大小 */
int fdfs_parse_storage_reserved_space(IniContext *pIniContext, \
		FDFSStorageReservedSpace *pStorageReservedSpace);

const char *fdfs_storage_reserved_space_to_string(FDFSStorageReservedSpace \
			*pStorageReservedSpace, char *buff);

const char *fdfs_storage_reserved_space_to_string_ex(const bool flag, \
	const int space_mb, const int total_mb, const double space_ratio, \
	char *buff);

/* 获取服务器保留存储空间的大小，返回值为mb */
int fdfs_get_storage_reserved_space_mb(const int total_mb, \
		FDFSStorageReservedSpace *pStorageReservedSpace);

/* 检查pGroup的空闲空间是否符合保留空间的限制 */
bool fdfs_check_reserved_space(FDFSGroupInfo *pGroup, \
	FDFSStorageReservedSpace *pStorageReservedSpace);

/* 检查pGroup的trunk空间空间是否符合保留空间的限制 */
bool fdfs_check_reserved_space_trunk(FDFSGroupInfo *pGroup, \
	FDFSStorageReservedSpace *pStorageReservedSpace);

/* 检查pGroup的space_path是否符合保留空间的限制 */
bool fdfs_check_reserved_space_path(const int64_t total_mb, \
	const int64_t free_mb, const int avg_mb, \
	FDFSStorageReservedSpace *pStorageReservedSpace);

bool fdfs_is_server_id_valid(const char *id);

/* 判断id是指server_id还是ip地址 */
int fdfs_get_server_id_type(const int id);

/* 从content中读取storage server分组及ip地址等信息 */
int fdfs_load_storage_ids(char *content, const char *pStorageIdsFilename);

FDFSStorageIdInfo *fdfs_get_storage_by_id(const char *id);

FDFSStorageIdInfo *fdfs_get_storage_id_by_ip(const char *group_name, \
		const char *pIpAddr);

/* 检查storage_id是否符合要求 */
int fdfs_check_storage_id(const char *group_name, const char *id);

int fdfs_get_storage_ids_from_tracker_server(ConnectionInfo *pTrackerServer);

int fdfs_get_storage_ids_from_tracker_group(TrackerServerGroup *pTrackerGroup);

int fdfs_load_storage_ids_from_file(const char *config_filename, \
		IniContext *pItemContext);

int fdfs_connection_pool_init(const char *config_filename, \
		IniContext *pItemContext);

void fdfs_connection_pool_destroy();

#ifdef __cplusplus
}
#endif

#endif

