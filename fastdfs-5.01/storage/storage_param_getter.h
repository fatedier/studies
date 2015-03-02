/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_param_getter.h

#ifndef _STORAGE_PARAM_GETTER_H_
#define _STORAGE_PARAM_GETTER_H_

#include "tracker_types.h"
#include "tracker_client_thread.h"

#ifdef __cplusplus
extern "C" {
#endif

/* 
 * 向tracker发送报文获取一些配置的参数信息
 * 有一台成功返回则停止
 * 根据返回的配置信息设置相关的变量以及进行相应的初始化工作
 */
int storage_get_params_from_tracker();

#ifdef __cplusplus
}
#endif

#endif

