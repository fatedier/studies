/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdfs_define.h

#ifndef _FDFS_DEFINE_H_
#define _FDFS_DEFINE_H_

#include <pthread.h>
#include "common_define.h"

#define FDFS_TRACKER_SERVER_DEF_PORT		22000		/* tracker server默认端口号 */
#define FDFS_STORAGE_SERVER_DEF_PORT		23000		/* storage server默认端口号 */
#define FDFS_DEF_STORAGE_RESERVED_MB		1024	/* storage server保留存储空间大小 */
#define TRACKER_ERROR_LOG_FILENAME      "trackerd"		/* tracker日志文件名 */
#define STORAGE_ERROR_LOG_FILENAME      "storaged"		/* storage日志文件名 */

#define FDFS_RECORD_SEPERATOR	'\x01'
#define FDFS_FIELD_SEPERATOR	'\x02'

/* 默认同步binglog（更新操作日志）到硬盘的时间间隔，单位为秒 */
#define SYNC_BINLOG_BUFF_DEF_INTERVAL  60
#define CHECK_ACTIVE_DEF_INTERVAL     100				/* 默认检查storage状态间隔时间 */

/* storage server之间同步文件的最大延迟时间 */
#define DEFAULT_STORAGE_SYNC_FILE_MAX_DELAY 	86400
/* storage server之间同步一个文件的最大超时时间 */
#define DEFAULT_STORAGE_SYNC_FILE_MAX_TIME	300

#ifdef __cplusplus
extern "C" {
#endif


#ifdef __cplusplus
}
#endif

#endif

