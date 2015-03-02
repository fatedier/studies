/**
* Copyright (C) 2012 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//trunk_free_block_checker.h

#ifndef _TRUNK_FREE_BLOCK_CHECKER_H_
#define _TRUNK_FREE_BLOCK_CHECKER_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include "common_define.h"
#include "fdfs_global.h"
#include "tracker_types.h"
#include "trunk_shared.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
	FDFSTrunkPathInfo path;  //trunk file path
	int id;                  //trunk file id
} FDFSTrunkFileIdentifier;		/* trunk_file的识别信息 */

typedef struct {
	int alloc;  /* alloc block count(总共分配的数量) */
	int count;  /* block count(一个trunk_file中小文件的数量) */
	FDFSTrunkFullInfo **blocks;   /* sort by FDFSTrunkFullInfo.file.offset(按照偏移量排序的小文件对应的对象的s内存空间) */
} FDFSBlockArray;		/* 一个trunk_file中的文件空间数组 */

typedef struct {
	FDFSTrunkFileIdentifier trunk_file_id;	/* trunk_file的path和id */
	FDFSBlockArray block_array;			/* 小文件信息数组 */
} FDFSTrunksById;		/* 根据id存储的trunk_file信息 */

/* 初始化tree_info_by_id这棵avl树，一个节点就是一个大的trunk_file文件 */
int trunk_free_block_checker_init();

/* 销毁tree_info_by_id这棵avl树 */
void trunk_free_block_checker_destroy();

/* 返回tree_info_by_id这棵树的节点树(即trunk_file的总数) */
int trunk_free_block_tree_node_count();

/* 返回tree_info_by_id中所有小文件的总数 */
int trunk_free_block_total_count();

/* 查看pTrunkInfo这个小文件是否已经存在，不存在返回0 */
int trunk_free_block_check_duplicate(FDFSTrunkFullInfo *pTrunkInfo);

/* 将pTrunkInfo这个小文件的信息加入到tree_info_by_id的avl树中 */
int trunk_free_block_insert(FDFSTrunkFullInfo *pTrunkInfo);

/* 在tree_info_by_id树中对应trunk_file_id的节点的链表中删除pTrunkInfo这个小文件的信息 */
int trunk_free_block_delete(FDFSTrunkFullInfo *pTrunkInfo);

/* 将tree_info_by_id这棵树中的信息dump到filename指定的文件中 */
int trunk_free_block_tree_print(const char *filename);

#ifdef __cplusplus
}
#endif

#endif

