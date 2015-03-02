/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fast_mblock.h

#ifndef _FAST_MBLOCK_H
#define _FAST_MBLOCK_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "common_define.h"
#include "chain.h"

/* free node chain */ 
struct fast_mblock_node	/* 可用内存块节点 */
{
	struct fast_mblock_node *next;
	char data[0];   //the data buffer
};

/* malloc chain */
struct fast_mblock_malloc		/* 已分配内存块节点 */
{
	struct fast_mblock_malloc *next;
};

struct fast_mblock_man		/* 内存块总体信息 */
{
	struct fast_mblock_node *free_chain_head;     /* free node chain(可用内存块) */
	struct fast_mblock_malloc *malloc_chain_head; /* malloc chain to be freed(已分配内存块) */
	int element_size;         /* element size(分配空间元素的大小) */
	int alloc_elements_once;  /* alloc elements once(一次分配多少个元素的空间) */
	pthread_mutex_t lock;     /* the lock for read / write free node chain */
};

#ifdef __cplusplus
extern "C" {
#endif

/**
mblock init
parameters:
	mblock: the mblock pointer
	element_size: element size, such as sizeof(struct xxx)
	alloc_elements_once: malloc elements once, 0 for malloc 1MB once
return error no, 0 for success, != 0 fail
*/
/* 初始化内存块的空间 */
int fast_mblock_init(struct fast_mblock_man *mblock, const int element_size, \
		const int alloc_elements_once);

/**
mblock destroy
parameters:
	mblock: the mblock pointer
*/
/* 销毁内存块的所有空间 */
void fast_mblock_destroy(struct fast_mblock_man *mblock);

/**
alloc a node from the mblock
parameters:
	mblock: the mblock pointer
return the alloced node, return NULL if fail
*/
/* 获取一块可用空间 */
struct fast_mblock_node *fast_mblock_alloc(struct fast_mblock_man *mblock);

/**
free a node (put a node to the mblock)
parameters:
	mblock: the mblock pointer
	pNode: the node to free
return the alloced node, return NULL if fail
*/
/* 将指定节点放回mblock链表中 */
int fast_mblock_free(struct fast_mblock_man *mblock, \
		     struct fast_mblock_node *pNode);

/**
get node count of the mblock
parameters:
	mblock: the mblock pointer
return the free node count of the mblock, return -1 if fail
*/
/* 获取当前可用内存块的数量 */
int fast_mblock_count(struct fast_mblock_man *mblock);

#ifdef __cplusplus
}
#endif

#endif

