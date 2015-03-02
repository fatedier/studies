/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//trunk_mem.h

#ifndef _TRUNK_MEM_H_
#define _TRUNK_MEM_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include "common_define.h"
#include "fdfs_global.h"
#include "fast_mblock.h"
#include "trunk_shared.h"
#include "fdfs_shared_func.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int g_slot_min_size;    //slot min size, such as 256 bytes
extern int g_slot_max_size;    //slot max size
extern int g_trunk_file_size;  //the trunk file size, such as 64MB
extern int g_store_path_mode;  //store which path mode, fetch from tracker
extern FDFSStorageReservedSpace g_storage_reserved_space;  //fetch from tracker
extern int g_avg_storage_reserved_mb;  //calc by above var: g_storage_reserved_mb
extern int g_store_path_index;  //store to which path
extern int g_current_trunk_file_id;  //current trunk file id
extern TimeInfo g_trunk_create_file_time_base;
extern int g_trunk_create_file_interval;
extern int g_trunk_compress_binlog_min_interval;
extern ConnectionInfo g_trunk_server;  //the trunk server
extern bool g_if_use_trunk_file;   //if use trunk file
extern bool g_trunk_create_file_advance;
extern bool g_trunk_init_check_occupying;
extern bool g_trunk_init_reload_from_binlog;
extern bool g_if_trunker_self;   //if am i trunk server
extern int64_t g_trunk_create_file_space_threshold;
extern int64_t g_trunk_total_free_space;  //trunk total free space in bytes
extern time_t g_trunk_last_compress_time;

typedef struct tagFDFSTrunkNode {
	FDFSTrunkFullInfo trunk;    /* trunk_file小文件的详细信息结构 */
	struct fast_mblock_node *pMblockNode;   //for free
	struct tagFDFSTrunkNode *next;
} FDFSTrunkNode;		/* trunk_file中的单个小文件的信息 */

typedef struct {
	int size;					/* 小文件的大小 */
	FDFSTrunkNode *head;		/* 小文件对象的链表 */
	struct fast_mblock_node *pMblockNode;   //for free
} FDFSTrunkSlot;	/* 同样大小的小文件的信息(保存在avl树中的一个节点) */

/* storage trunk server的初始化工作 */
int storage_trunk_init();

/* 销毁trunk相关的资源 */
int storage_trunk_destroy_ex(const bool bNeedSleep);

/* 销毁trunk相关的资源 */
#define storage_trunk_destroy() storage_trunk_destroy_ex(false)

/* 为一个大小为size的trunk小文件分配空间用于存储小文件的信息 */
int trunk_alloc_space(const int size, FDFSTrunkFullInfo *pResult);

/* 确认pTrunkInfo这个小文件的对象空间的状态 */
int trunk_alloc_confirm(const FDFSTrunkFullInfo *pTrunkInfo, const int status);

/* 释放小文件pTrunkInfo占用的内存空间，供以后其他文件复用*/
int trunk_free_space(const FDFSTrunkFullInfo *pTrunkInfo, \
		const bool bWriteBinLog);

/* 检查file_size是否符合要求，只有文件大小<= 这个参数的文件才会合并) */
bool trunk_check_size(const int64_t file_size);

/* 初始化trunk_file，如果其他线程创建了，等待，最后截取至file_size的大小 */
#define trunk_init_file(filename) \
	trunk_init_file_ex(filename, g_trunk_file_size)

/* 检查并初始化指定trunk_file */
#define trunk_check_and_init_file(filename) \
	trunk_check_and_init_file_ex(filename, g_trunk_file_size)

/* 初始化trunk_file，如果其他线程创建了，等待，最后截取至file_size的大小 */
int trunk_init_file_ex(const char *filename, const int64_t file_size);

/* 检查并初始化指定trunk_file */
int trunk_check_and_init_file_ex(const char *filename, const int64_t file_size);

/* 在trunk文件中删除FDFSTrunkFullInfo中指定的小文件 */
int trunk_file_delete(const char *trunk_filename, \
		const FDFSTrunkFullInfo *pTrunkInfo);

/* 预创建一定数量的trunk_file */
int trunk_create_trunk_file_advance(void *args);

/* 删除trunk信息文件 */
int storage_delete_trunk_data_file();

/* 获取存储trunk信息的文件名 */
char *storage_trunk_get_data_filename(char *full_filename);

/* 检查pGroup的空闲空间是否符合保留空间的限制 */
#define storage_check_reserved_space(pGroup) \
        fdfs_check_reserved_space(pGroup, &g_storage_reserved_space)

/* 检查pGroup的trunk空间空间是否符合保留空间的限制 */
#define storage_check_reserved_space_trunk(pGroup) \
        fdfs_check_reserved_space_trunk(pGroup, &g_storage_reserved_space)

/* 检查指定的空间大小是否符合保留空间的限制 */
#define storage_check_reserved_space_path(total_mb, free_mb, avg_mb) \
        fdfs_check_reserved_space_path(total_mb, free_mb, avg_mb, \
                                &g_storage_reserved_space)

/* 获取服务器保留存储空间的大小，返回值为mb */
#define storage_get_storage_reserved_space_mb(total_mb) \
	fdfs_get_storage_reserved_space_mb(total_mb, &g_storage_reserved_space)

#ifdef __cplusplus
}
#endif

#endif

