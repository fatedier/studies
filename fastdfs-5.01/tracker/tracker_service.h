/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_service.h

#ifndef _TRACKER_SERVICE_H_
#define _TRACKER_SERVICE_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fdfs_define.h"
#include "ioevent.h"
#include "fast_task_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

//typedef struct nio_thread_data struct nio_thread_data;

extern int g_tracker_thread_count;
extern struct nio_thread_data *g_thread_data;

/* tracker服务的初始化 */
int tracker_service_init();


/* 销毁tracker_service资源 */
int tracker_service_destroy();

/* 终止所有的工作线程，向每个线程的pipe[1]管道发送小于0的socket描述符 */
int tracker_terminate_threads();

/* 
 * 循环accept，主进程会阻塞在一个accept中
 * 如果g_accept_threads>1，会通过多线程来同时accept 
 */
void tracker_accept_loop(int server_sock);

/* tracker任务处理 */
int tracker_deal_task(struct fast_task_info *pTask);

#ifdef __cplusplus
}
#endif

#endif
