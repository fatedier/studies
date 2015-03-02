/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_service.h

#ifndef _STORAGE_SERVICE_H_
#define _STORAGE_SERVICE_H_

#define STORAGE_CREATE_FLAG_NONE  0
#define STORAGE_CREATE_FLAG_FILE  1
#define STORAGE_CREATE_FLAG_LINK  2

#define STORAGE_DELETE_FLAG_NONE  0
#define STORAGE_DELETE_FLAG_FILE  1
#define STORAGE_DELETE_FLAG_LINK  2

#include "logger.h"
#include "fdfs_define.h"
#include "fast_task_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int g_storage_thread_count;
extern pthread_mutex_t g_storage_thread_lock;

/* storage service的初始化工作 */
int storage_service_init();

/* 销毁storage service 相关资源 */
void storage_service_destroy();

/* 将storage的当前状态信息写入磁盘 */
int fdfs_stat_file_sync_func(void *args);

/* 处理请求报文 */
int storage_deal_task(struct fast_task_info *pTask);

/* 将此任务继续加入到IO事件集合中，等待处理 */
void storage_nio_notify(struct fast_task_info *pTask);

void storage_accept_loop(int server_sock);

int storage_terminate_threads();

/* 获取一个正确的存储路径 */
int storage_get_storage_path_index(int *store_path_index);

/* 根据文件名和存储路径选择方式返回一个存储路径 */
void storage_get_store_path(const char *filename, const int filename_len, \
		int *sub_path_high, int *sub_path_low);

#ifdef __cplusplus
}
#endif

#endif
