/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_sync.h

#ifndef _STORAGE_SYNC_H_
#define _STORAGE_SYNC_H_

#include "storage_func.h"

#define STORAGE_OP_TYPE_SOURCE_CREATE_FILE	'C'  //upload file
#define STORAGE_OP_TYPE_SOURCE_APPEND_FILE	'A'  //append file
#define STORAGE_OP_TYPE_SOURCE_DELETE_FILE	'D'  //delete file
#define STORAGE_OP_TYPE_SOURCE_UPDATE_FILE	'U'  //for whole file update such as metadata file
#define STORAGE_OP_TYPE_SOURCE_MODIFY_FILE	'M'  //for part modify
#define STORAGE_OP_TYPE_SOURCE_TRUNCATE_FILE	'T'  //truncate file
#define STORAGE_OP_TYPE_SOURCE_CREATE_LINK	'L'  //create symbol link
#define STORAGE_OP_TYPE_REPLICA_CREATE_FILE	'c'
#define STORAGE_OP_TYPE_REPLICA_APPEND_FILE	'a'
#define STORAGE_OP_TYPE_REPLICA_DELETE_FILE	'd'
#define STORAGE_OP_TYPE_REPLICA_UPDATE_FILE	'u'
#define STORAGE_OP_TYPE_REPLICA_MODIFY_FILE	'm'
#define STORAGE_OP_TYPE_REPLICA_TRUNCATE_FILE	't'
#define STORAGE_OP_TYPE_REPLICA_CREATE_LINK	'l'

/* 每次从binlog_file加载内容到缓冲区中的长度 */
#define STORAGE_BINLOG_BUFFER_SIZE		64 * 1024
#define STORAGE_BINLOG_LINE_SIZE		256

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
	char storage_id[FDFS_STORAGE_ID_MAX_SIZE];
	bool need_sync_old;		/* binlog_file中小于最近一次同步时间戳的记录是否需要同步 */
	bool sync_old_done;		/* binlog_file中小于最近一次同步时间戳的记录是否同步完成 */
	bool last_file_exist;   /* if the last file exist on the dest server */
	BinLogBuffer binlog_buff;	/* binlog文件内容的缓冲区 */
	time_t until_timestamp;	/* 同源storage的最近一次同步时间戳 */
	int mark_fd;
	int binlog_index;			/* binlog索引，binlog.001，文件名中的后缀 */
	int binlog_fd;			/* binlog文件描述符 */
	int64_t binlog_offset;		/* 已从文件中加载内容的偏移量 */
	int64_t scan_row_count;	/* 当前扫描过的binlog_file记录行数 */
	int64_t sync_row_count;	/* 当前同步过的binlog_file记录行数 */

	int64_t last_scan_rows;  /* for write to mark file(上一次扫描的行数) */
	int64_t last_sync_rows;  /* for write to mark file(上一次同步过的行数) */
} StorageBinLogReader;	/* binlog的读取对象 */

typedef struct
{
	time_t timestamp;	/* 时间戳 */
	char op_type;		/* 操作类型 */
	char filename[128];  //filename with path index prefix which should be trimed
	char true_filename[128]; //pure filename
	char src_filename[128];  //src filename with path index prefix
	int filename_len;
	int true_filename_len;
	int src_filename_len;
	int store_path_index;
} StorageBinLogRecord;	/* binlog文件中的记录结构 */

extern int g_binlog_fd;
extern int g_binlog_index;

extern int g_storage_sync_thread_count;

/* storage同步文件相关的初始化工作，打开相应的binlog_file */
int storage_sync_init();

/* 销毁同步文件的相关资源 */
int storage_sync_destroy();

/* 将操作信息写入binlog_file，先写在缓冲区，满了以后写入文件 */
#define storage_binlog_write(timestamp, op_type, filename) \
	storage_binlog_write_ex(timestamp, op_type, filename, NULL)

/* 
 * 将操作信息写入binlog_file，先写在缓冲区，满了以后写入文件
 * extra为每一个操作的附加信息，可以为NULL 
 */
int storage_binlog_write_ex(const int timestamp, const char op_type, \
		const char *filename, const char *extra);

/* 读取binlog文件中的一行记录，解析出true_filename，store_path_index等存在pRecord中 */
int storage_binlog_read(StorageBinLogReader *pReader, \
			StorageBinLogRecord *pRecord, int *record_length);

/* 启动一个线程从指定的pStorage同步文件到当前storage */
int storage_sync_thread_start(const FDFSStorageBrief *pStorage);

/* 停止所有的stogae同步线程 */
int kill_storage_sync_threads();

/* 强制将缓冲区中的内容写入binlog_file文件 */
int fdfs_binlog_sync_func(void *args);

/* 根据(const StorageBinLogReader *)pArg中的storage_id获取mark_file文件名 */
char *get_mark_filename_by_reader(const void *pArg, char *full_filename);

/* 将指定storage_id对应的mark_file重命名，加上当前时间作为后缀 */
int storage_unlink_mark_file(const char *storage_id);

/* 根据新的ip和port修改指定mark_file的文件名 */
int storage_rename_mark_file(const char *old_ip_addr, const int old_port, \
		const char *new_ip_addr, const int new_port);

/* 打开binlog file 并定位到指定偏移量的位置，偏移量通过pReader->binlog_offset指定 */
int storage_open_readable_binlog(StorageBinLogReader *pReader, \
		get_filename_func filename_func, const void *pArg);

/* 初始化StorageBinLogReader对象 */
int storage_reader_init(FDFSStorageBrief *pStorage, StorageBinLogReader *pReader);

/* 销毁读取binlog file的相关资源 */
void storage_reader_destroy(StorageBinLogReader *pReader);

/* 向所有的tracker发送报文汇报指定storage的状态信息，只要有一个成功则返回0 */
int storage_report_storage_status(const char *storage_id, \
		const char *ip_addr, const char status);

#ifdef __cplusplus
}
#endif

#endif
