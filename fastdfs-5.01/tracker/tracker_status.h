/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_status.h

#ifndef _TRACKER_STATUS_H_
#define _TRACKER_STATUS_H_

#include <time.h>

typedef struct {
	time_t up_time;			/* tracker进程启动时间 */
	time_t last_check_time;	/* tracker状态检查时间 */
} TrackerStatus;	/* tracker服务器状态 */

#ifdef __cplusplus
extern "C" {
#endif

/* 将tracker的运行状态信息写入文件 */
int tracker_write_status_to_file(void *args);

/* 从文件中获取tracker server的状态 */
int tracker_load_status_from_file(TrackerStatus *pStatus);

#ifdef __cplusplus
}
#endif

#endif
