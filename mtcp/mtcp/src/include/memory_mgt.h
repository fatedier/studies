#ifndef __MEMORY_MGT_H_
#define __MEMORY_MGT_H_

struct mem_pool;
typedef struct mem_pool* mem_pool_t;

/* create a memory pool with a chunk size and total size
   an return the pointer to the memory pool */
// 创建指定大小的内存池
mem_pool_t MPCreate(int chunk_size, size_t total_size, int is_hugepage);

/* allocate one chunk */
// 获取一个chunk的内存空间
void *MPAllocateChunk(mem_pool_t mp);

/* free one chunk */
// 释放一块内存空间
void MPFreeChunk(mem_pool_t mp, void *p);

/* destroy the memory pool */
// 释放内存池
void MPDestroy(mem_pool_t mp);

/* return the number of free chunks */
// 返回空闲内存池对象数
int MPGetFreeChunks(mem_pool_t mp);

#endif /* __MEMORY_MGT_H_ */
