/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_mem.h

#ifndef _TRACKER_MEM_H_
#define _TRACKER_MEM_H_

#include <time.h>
#include <pthread.h>
#include "tracker_types.h"

#define TRACKER_SYS_FILE_COUNT  4
#define STORAGE_GROUPS_LIST_FILENAME_OLD   "storage_groups.dat"				/* groups信息文件old */
#define STORAGE_GROUPS_LIST_FILENAME_NEW   "storage_groups_new.dat"		/* groups信息文件new */
#define STORAGE_SERVERS_LIST_FILENAME_OLD  "storage_servers.dat"
#define STORAGE_SERVERS_LIST_FILENAME_NEW  "storage_servers_new.dat"		/* storage server信息文件 */
#define STORAGE_SERVERS_CHANGELOG_FILENAME "storage_changelog.dat"			/* changelog文件名 */
#define STORAGE_SYNC_TIMESTAMP_FILENAME	   "storage_sync_timestamp.dat"		/* 同步时间戳保存文件 */
#define TRUNK_SERVER_CHANGELOG_FILENAME    "trunk_server_change.log"			/* trunk_server变更信息文件 */
#define STORAGE_DATA_FIELD_SEPERATOR	   ','		/* 分隔符 */

typedef struct {
	ConnectionInfo *pTrackerServer;
	int running_time;     /* running seconds, more means higher weight(运行时间) */
	int restart_interval; /* restart interval, less mean higher weight(重新启动间隔的时间) */
	bool if_leader;       /* if leader(是否可以为leader) */
} TrackerRunningStatus;	/* tracker server运行状态 */

#ifdef __cplusplus
extern "C" {
#endif

extern TrackerServerGroup g_tracker_servers;  //save all tracker servers from storage server
extern ConnectionInfo *g_last_tracker_servers;  //for delay free
extern int g_next_leader_index;			   //next leader index
extern int g_tracker_leader_chg_count;		   //for notify storage servers
extern int g_trunk_server_chg_count;		   //for notify other trackers

extern int64_t g_changelog_fsize; //storage server change log file size
extern char *g_tracker_sys_filenames[TRACKER_SYS_FILE_COUNT];

int tracker_mem_init();
int tracker_mem_destroy();

int tracker_mem_init_pthread_lock(pthread_mutex_t *pthread_lock);
int tracker_mem_pthread_lock();
int tracker_mem_pthread_unlock();

/* 为读写tracker相关文件加锁 */
int tracker_mem_file_lock();
/* 为读写tracker相关文件解锁 */
int tracker_mem_file_unlock();

/* 根据group_name获取group信息，如果不存在，返回NULL */
#define tracker_mem_get_group(group_name) \
	tracker_mem_get_group_ex((&g_groups), group_name)

/* 根据group_name获取group信息，如果不存在，返回NULL */
FDFSGroupInfo *tracker_mem_get_group_ex(FDFSGroups *pGroups, \
		const char *group_name);

/* 根据id在pGroup中找到对应的storage_server */
FDFSStorageDetail *tracker_mem_get_storage(FDFSGroupInfo *pGroup, \
				const char *id);

/* 根据ip地址，获取storage对象 */
FDFSStorageDetail *tracker_mem_get_storage_by_ip(FDFSGroupInfo *pGroup, \
				const char *ip_addr);

/* 设置pGroup中的pStroageId为trunk_server,如果不指定，则选取trunk_binlog_size最大的storage */
const FDFSStorageDetail *tracker_mem_set_trunk_server( \
	FDFSGroupInfo *pGroup, const char *pStroageId, int *result);

/* 在pGroup中删除指定storage_id的storage */
int tracker_mem_delete_storage(FDFSGroupInfo *pGroup, const char *id);

/* 在pGroup中修改指定storage_id的ip */
int tracker_mem_storage_ip_changed(FDFSGroupInfo *pGroup, \
		const char *old_storage_ip, const char *new_storage_ip);

/* 
 * 将新加入的storage_server所在的group以及storage信息加入到内存的全局变量中
 * 并写入文件 
 */
int tracker_mem_add_group_and_storage(TrackerClientInfo *pClientInfo, \
		const char *ip_addr, FDFSStorageJoinBody *pJoinBody, \
		const bool bNeedSleep);

/* 将指定的storage设为offline状态 */
int tracker_mem_offline_store_server(FDFSGroupInfo *pGroup, \
			FDFSStorageDetail *pStorage);

/* 这个storage server如果没有，就加入到相应group中 */
int tracker_mem_active_store_server(FDFSGroupInfo *pGroup, \
			FDFSStorageDetail *pTargetServer);

int tracker_mem_sync_storages(FDFSGroupInfo *pGroup, \
                FDFSStorageBrief *briefServers, const int server_count);

/* 将storage的状态信息写入文件中保存 */
int tracker_save_storages();

/* 将所有group的同步时间戳信息写入文件中 */
int tracker_save_sync_timestamps();

/* 将系统数据文件在内存中对应的信息写入文件中 */
int tracker_save_sys_files();

int tracker_get_group_file_count(FDFSGroupInfo *pGroup);
int tracker_get_group_success_upload_count(FDFSGroupInfo *pGroup);

/* 找到同group中的一台storage，除了自己 */
FDFSStorageDetail *tracker_get_group_sync_src_server(FDFSGroupInfo *pGroup, \
			FDFSStorageDetail *pDestServer);

/* 在指定group中获取一个可上传文件的storage */
FDFSStorageDetail *tracker_get_writable_storage(FDFSGroupInfo *pStoreGroup);

#ifdef WITH_HTTPD
#define FDFS_DOWNLOAD_TYPE_PARAM  	const int download_type, 
#define FDFS_DOWNLOAD_TYPE_CALL		FDFS_DOWNLOAD_TYPE_TCP, 
#else
#define FDFS_DOWNLOAD_TYPE_PARAM 
#define FDFS_DOWNLOAD_TYPE_CALL 
#endif

/* 根据file_name获取应该去哪台storage server下载 */
int tracker_mem_get_storage_by_filename(const byte cmd,FDFS_DOWNLOAD_TYPE_PARAM\
	const char *group_name, const char *filename, const int filename_len, \
	FDFSGroupInfo **ppGroup, FDFSStorageDetail **ppStoreServers, \
	int *server_count);

/* 检测storage的状态是否正常 */
int tracker_mem_check_alive(void *arg);

/* 返回pStorage在pGroup->all_servers中的位置 */
int tracker_mem_get_storage_index(FDFSGroupInfo *pGroup, \
		FDFSStorageDetail *pStorage);

/* 发送开始获取配置文件的报文 */
#define tracker_get_sys_files_start(pTrackerServer) \
		 fdfs_deal_no_body_cmd(pTrackerServer, \
		TRACKER_PROTO_CMD_TRACKER_GET_SYS_FILES_START)

/* 发送结束获取配置文件的报文 */
#define tracker_get_sys_files_end(pTrackerServer) \
		 fdfs_deal_no_body_cmd(pTrackerServer, \
		TRACKER_PROTO_CMD_TRACKER_GET_SYS_FILES_END)

void tracker_calc_running_times(TrackerRunningStatus *pStatus);

/* 发送报文获取pTrackerServer的状态信息 */
int tracker_mem_get_status(ConnectionInfo *pTrackerServer, \
		TrackerRunningStatus *pStatus);

/* 将所有groups相关的信息写入groups_new文件 */
int tracker_save_groups();

/* 查找所有group的trunk_server */
void tracker_mem_find_trunk_servers();

#ifdef __cplusplus
}
#endif

#endif
