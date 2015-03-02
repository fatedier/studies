/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fast_task_queue.h

/* 任务队列 */

#ifndef _FAST_TASK_QUEUE_H
#define _FAST_TASK_QUEUE_H 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "common_define.h"
#include "ioevent.h"
#include "fast_timer.h"

struct fast_task_info;

/* 任务结束的回调函数 */
typedef int (*TaskFinishCallBack) (struct fast_task_info *pTask);

typedef void (*TaskCleanUpCallBack) (struct fast_task_info *pTask);

/* IO事件的回调函数 */
typedef void (*IOEventCallback) (int sock, short event, void *arg);

typedef struct ioevent_entry
{
	int fd;						/* 对应文件描述符 */
	FastTimerEntry timer;			/* 时间轮中某个slot中的一个节点，data信息也存放在这里 */
	IOEventCallback callback;		/* IO事件触发后的回调函数 */
} IOEventEntry;	/* IO事件节点 */

struct nio_thread_data		/* 工作线程数据 */
{
	struct ioevent_puller ev_puller;		/* IOEvent调度对象 */
	struct fast_timer timer;			/* 时间轮对象 */
        int pipe_fds[2];
	struct fast_task_info *deleted_list;
};

struct fast_task_info	/* 任务信息结构，每一个连接对应一个 */
{
	IOEventEntry event;				/* IO事件节点 */
	char client_ip[IP_ADDRESS_SIZE];		/* 连接中的客户端ip地址 */
	void *arg;  //extra argument pointer		/* 额外参数对象 */
	char *data; //buffer for write or recv
	int size;   //alloc size					/* data对象分配空间的大小 */
	int length; //data length					/* 报文体的长度 */
	int offset; //current offset				/* 读取socket时，已读到内容的偏移量 */
	int req_count; //request count
	TaskFinishCallBack finish_callback;		/* 任务结束的回调函数 */
	struct nio_thread_data *thread_data;	/* 线程数据指针，包括epoll等的封装，以及时间轮对象 */
	struct fast_task_info *next;	
};

struct fast_task_queue		/* 任务队列 */
{
	struct fast_task_info *head;	/* 任务队列头指针 */
	struct fast_task_info *tail;		/* 任务队列尾指针 */
	pthread_mutex_t lock;			/* 队列锁 */
	int max_connections;			/* 最大连接数(队列最大长度) */
	int min_buff_size;			/* data字段的长度 */
	int max_buff_size;
	int arg_size;					/* 参数对象的大小 */
	bool malloc_whole_block;		/* 是否为data对象在同一块block一起分配内存 */
};

#ifdef __cplusplus
extern "C" {
#endif

/* 连接池(内存池队列)的初始化 */
int free_queue_init(const int max_connections, const int min_buff_size, \
		const int max_buff_size, const int arg_size);

/* 销毁内存池队列 */
void free_queue_destroy();

/* 在内存池队列头新增节点 */
int free_queue_push(struct fast_task_info *pTask);

/* pop内存池队列头节点 */
struct fast_task_info *free_queue_pop();

/* 统计内存池队列中可用节点数 */
int free_queue_count();


/* 初始化任务队列 */
int task_queue_init(struct fast_task_queue *pQueue);

/* 将任务节点pTask加入到任务队列中 */
int task_queue_push(struct fast_task_queue *pQueue, \
		struct fast_task_info *pTask);

/* pop任务队列头节点 */
struct fast_task_info *task_queue_pop(struct fast_task_queue *pQueue);

/* 返回任务队列节点数 */
int task_queue_count(struct fast_task_queue *pQueue);

#ifdef __cplusplus
}
#endif

#endif

