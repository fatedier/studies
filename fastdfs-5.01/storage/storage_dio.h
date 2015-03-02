/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_dio.h

#ifndef _STORAGE_DIO_H
#define _STORAGE_DIO_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "tracker_types.h"
#include "fast_task_queue.h"

struct storage_dio_context		/* 每一个读写线程对应的线程变量 */
{
	struct fast_task_queue queue;		/* 该线程要处理的任务队列 */
	pthread_mutex_t lock;
	pthread_cond_t cond;
};

struct storage_dio_thread_data		/* 每个存储路径对应的dio线程变量 */
{
	/* for mixed read / write */
	struct storage_dio_context *contexts;	/* 所有读写线程的相关信息 */
	int count;  /* context count(读写线程总数，contexts元素个数) */

	/* for separated read / write */
	struct storage_dio_context *reader;	/* 所有读线程信息 */
	struct storage_dio_context *writer;	/* 所有写线程信息 */
};

#ifdef __cplusplus
extern "C" {
#endif

extern int g_dio_thread_count;

 /* 初始化磁盘IO相关资源，每一个存储路径都创建相关读写线程 */
int storage_dio_init();

 /* 
 * 向所有读写线程发送通知，不阻塞在条件变量上
 * 之前g_continue_flag置为false即线程全部退出 
 */
void storage_dio_terminate();

/* 获取指定存储路径的读或写线程的索引index */
int storage_dio_get_thread_index(struct fast_task_info *pTask, \
		const int store_path_index, const char file_op);

/* 将此任务加入到任务队列中并通知对应的读写线程进行处理 */
int storage_dio_queue_push(struct fast_task_info *pTask);

/* 将指定的文件内容写入pTask->data */
int dio_read_file(struct fast_task_info *pTask);

/* 将pTask->data 中的内容写入指定文件 */
int dio_write_file(struct fast_task_info *pTask);

/* 截取指定文件到pFileContext->offset，之后的内容清空 */
int dio_truncate_file(struct fast_task_info *pTask);

/* 删除指定的普通文件 */
int dio_delete_normal_file(struct fast_task_info *pTask);

/* 删除指定的trunk_file */
int dio_delete_trunk_file(struct fast_task_info *pTask);

/* 将已接收到的buff内容丢弃，之后继续处理 */
int dio_discard_file(struct fast_task_info *pTask);

/* 读取文件完成后的清理工作 */
void dio_read_finish_clean_up(struct fast_task_info *pTask);

/* 写入文件完成后的清理工作，如果没有全部写入成功，删除此文件 */
void dio_write_finish_clean_up(struct fast_task_info *pTask);

/* 添加数据到文件末尾完成后的清理工作，没有全写完的话还原 */
void dio_append_finish_clean_up(struct fast_task_info *pTask);

/* 写入trunk_file文件完成后的清理工作，如果没有全部写入成功，还原 */
void dio_trunk_write_finish_clean_up(struct fast_task_info *pTask);

/* 修改文件后的清理工作，如果没有全部完成，还原 */
void dio_modify_finish_clean_up(struct fast_task_info *pTask);

/* 截取文件完成后的清理工作 */
#define dio_truncate_finish_clean_up  dio_read_finish_clean_up

/* 检查指定的fd中的读写位置处是否是trunk_file */
int dio_check_trunk_file_ex(int fd, const char *filename, const int64_t offset);

/* 上传文件前检查trunk_file是否正确，如果没有，创建trunk_file */
int dio_check_trunk_file_when_upload(struct fast_task_info *pTask);

/* 同步的时候检查trunk_file是否正确 */
int dio_check_trunk_file_when_sync(struct fast_task_info *pTask);

/* 将trunk_header信息写入文件 */
int dio_write_chunk_header(struct fast_task_info *pTask);

#ifdef __cplusplus
}
#endif

#endif

