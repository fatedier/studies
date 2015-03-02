/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_ip_changed_dealer.h

#ifndef _STORAGE_IP_CHANGED_DEALER_H_
#define _STORAGE_IP_CHANGED_DEALER_H_

#include "tracker_types.h"
#include "tracker_client_thread.h"

#ifdef __cplusplus
extern "C" {
#endif

/* 获取与tracker建立连接的自身的ip地址 */
int storage_get_my_tracker_client_ip();

/* 
 * 向所有tracker发送报文获取changelog文件的内容 ，直到有一台成功返回
 * 发送报文:空或者group_name+storage_id 
 * 返回报文:changelog文件中的一段，由pTask->storage的偏移量来决定 
 * 解析出同组的storage的变更记录，修改或重命名相关的用于sync或trunk的mark_file文件
 */
int storage_changelog_req();

/* 
 * 如果ip改变后需要集群自动调整，向tracker发送报文通知
 * 并获取changelog文件更新 有变更的storage相对应的mark_file文件
 */
int storage_check_ip_changed();

#ifdef __cplusplus
}
#endif

#endif

