/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//trunk_client.h

#ifndef _TRUNK_CLIENT_H_
#define _TRUNK_CLIENT_H_

#include "common_define.h"
#include "tracker_types.h"
#include "trunk_mem.h"

#ifdef __cplusplus
extern "C" {
#endif

/* 
 * 如果自己是trunk_server，直接为小文件分配空间
 * 否则向trunk_server发送报文要求分配空间 
 */
int trunk_client_trunk_alloc_space(const int file_size, \
		FDFSTrunkFullInfo *pTrunkInfo);

/* storage向trunk_server确认小文件的状态是否已分配空间 */
int trunk_client_trunk_alloc_confirm(const FDFSTrunkFullInfo *pTrunkInfo, \
		const int status);

/* storage向trunk_server请求释放小文件占用的空间 */
int trunk_client_trunk_free_space(const FDFSTrunkFullInfo *pTrunkInfo);

#ifdef __cplusplus
}
#endif

#endif

