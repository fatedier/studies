/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_client_thread.h

#ifndef _TRACKER_CLIENT_THREAD_H_
#define _TRACKER_CLIENT_THREAD_H_

#include "tracker_types.h"
#include "storage_sync.h"

#ifdef __cplusplus
extern "C" {
#endif

/* report相关资源初始化工作 */
int tracker_report_init();

 /* 销毁同tracker交互线程资源 */
int tracker_report_destroy();

/* 启动同所有tracker server交互的线程 */
int tracker_report_thread_start();

/* 停止所有的同tracker交互的线程 */
int kill_tracker_report_threads();

/* 向tracker发送报文，申请加入指定group */
int tracker_report_join(ConnectionInfo *pTrackerServer, \
		const int tracker_index, const bool sync_old_done);

/*  
 * 向tracker发送报文汇报指定storage的状态信息
 * 报文体:group_name，之后是一个FDFSStorageBrief结构的报文
 * 返回报文:无
 */
int tracker_report_storage_status(ConnectionInfo *pTrackerServer, \
		FDFSStorageBrief *briefServer);

/* 
 * 获取pReader->storage_id对应的源storage和同步时间戳 
 * 发送报文:group_name+storage_id 
 * 返回报文:指定的storage的TrackerStorageSyncReqBody(包括源storage和同步时间戳) 
 */
int tracker_sync_src_req(ConnectionInfo *pTrackerServer, \
		StorageBinLogReader *pReader);

/* 更新多个storage_server的状态信息 */
int tracker_sync_diff_servers(ConnectionInfo *pTrackerServer, \
		FDFSStorageBrief *briefServers, const int server_count);

/* 
 * 向指定tracker发送报文获取changelog文件的内容 
 * 解析返回报文
 * 返回报文:changelog文件中的一段，由pTask->storage的偏移量来决定 
 * 解析出同组的storage的变更记录，修改或重命名相关的用于sync或trunk的mark_file文件
 */
int tracker_deal_changelog_response(ConnectionInfo *pTrackerServer);

#ifdef __cplusplus
}
#endif

#endif
