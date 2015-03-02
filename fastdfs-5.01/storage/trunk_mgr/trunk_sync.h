/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//trunk_sync.h

#ifndef _TRUNK_SYNC_H_
#define _TRUNK_SYNC_H_

#include "tracker_types.h"
#include "storage_func.h"
#include "trunk_mem.h"

#define TRUNK_OP_TYPE_ADD_SPACE		'A'
#define TRUNK_OP_TYPE_DEL_SPACE		'D'

#define TRUNK_BINLOG_BUFFER_SIZE	(64 * 1024)	/* 读取trunk_binlog时的缓冲区 */
#define TRUNK_BINLOG_LINE_SIZE		128			/* trunk_binlog每一行的长度 */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
	char storage_id[FDFS_STORAGE_ID_MAX_SIZE];
	BinLogBuffer binlog_buff;
	int mark_fd;
	int binlog_fd;
	int64_t binlog_offset;
	int64_t last_binlog_offset;  //for write to mark file
} TrunkBinLogReader;		/* trunk binlog的读取信息 */

typedef struct
{
	time_t timestamp;		/* 操作时间戳 */
	char op_type;			/* 操作类型 */
	FDFSTrunkFullInfo trunk;
} TrunkBinLogRecord;		/* trunk binlog中的一个操作记录 */

extern int g_trunk_sync_thread_count;

/* trunk同步相关资源的初始化，包括创建trunk_binlog_file等 */
int trunk_sync_init();

/* trunk同步相关资源的销毁工作，关闭trunk_binlog_file */
int trunk_sync_destroy();

/* 将buff中的内容写入trunk_binlog_file */
int trunk_binlog_write_buffer(const char *buff, const int length);

/* 将操作记录写入trunk_binlog_file */
int trunk_binlog_write(const int timestamp, const char op_type, \
		const FDFSTrunkFullInfo *pTrunk);

/* 将trunk_binlog_file清空 */
int trunk_binlog_truncate();

/* 从trunk_binlog_file中读取一行记录并解析到TrunkBinLogRecord对象中 */
int trunk_binlog_read(TrunkBinLogReader *pReader, \
		      TrunkBinLogRecord *pRecord, int *record_length);

/* 为每一个同组的storage启动一个trunk同步线程 */
int trunk_sync_thread_start_all();

/* 启动trunk的同步线程 */
int trunk_sync_thread_start(const FDFSStorageBrief *pStorage);

/* 停止所有正在运行中的trunk同步线程 */
int kill_trunk_sync_threads();

/* 将缓冲区中的内容强制刷新到磁盘中 */
int trunk_binlog_sync_func(void *args);

/* 获取trunk_binlog_file的文件名 */
char *get_trunk_binlog_filename(char *full_filename);

/* 根据trunk_reader信息获取trunk的mark_file */
char *trunk_mark_filename_by_reader(const void *pArg, char *full_filename);

/* 删除所有同组的其他storage对应的mark_file文件(加上时间作为后缀) */
int trunk_unlink_all_mark_files();

/* 将指定storage_id对应的mark_file重命名，加上当前时间作为后缀 */
int trunk_unlink_mark_file(const char *storage_id);

/* 根据新的ip和port重命名trunk的mark_file */
int trunk_rename_mark_file(const char *old_ip_addr, const int old_port, \
		const char *new_ip_addr, const int new_port);

/* 打开trunk binlog file，定位到指定偏移量的位置 */
int trunk_open_readable_binlog(TrunkBinLogReader *pReader, \
		get_filename_func filename_func, const void *pArg);

/* 
 * 初始化指定storage对应的TrunkBinLogReader对象 
 * pStorage为NULL则获取自身的reader对象
 */
int trunk_reader_init(FDFSStorageBrief *pStorage, TrunkBinLogReader *pReader);

/* 销毁trunk_reader相关资源 */
void trunk_reader_destroy(TrunkBinLogReader *pReader);

/* 开始压缩trunk_binlog_file的准备工作，将trunk_binlog_file保存一个rollback文件以供恢复 */
int trunk_binlog_compress_apply();

/* 压缩操作，将storage_trunk.dat和trunk_binlog_file中的内容合并 */
int trunk_binlog_compress_commit();

/* 回滚操作，恢复trunk_binlog_file文件，如果两个文件都有信息，合并 */
int trunk_binlog_compress_rollback();

#ifdef __cplusplus
}
#endif

#endif
