/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_nio.h

#ifndef _TRACKER_NIO_H
#define _TRACKER_NIO_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fast_task_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

/* 
 * 每个线程的管道pipe_fds[0]的READ事件的回调函数 
 * 每次读取一个int变量，是新建立连接的socket描述符，之后加入到IO事件集合
 * 按通道分发到相应工作线程中等待可读事件触发后调用client_sock_read函数进行处理
 */
void recv_notify_read(int sock, short event, void *arg);

/* 将发送报文的事件加入到IO事件集合中 */
int send_add_event(struct fast_task_info *pTask);

/* 任务结束后的清理函数 */
void task_finish_clean_up(struct fast_task_info *pTask);

#ifdef __cplusplus
}
#endif

#endif

