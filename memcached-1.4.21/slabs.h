/* slabs memory allocation */
#ifndef SLABS_H
#define SLABS_H

/** Init the subsystem. 1st argument is the limit on no. of bytes to allocate,
    0 if no limit. 2nd argument is the growth factor; each slab will use a chunk
    size equal to the previous slab's chunk size times this factor.
    3rd argument specifies if the slab allocator should allocate all memory
    up front (if true), or allocate memory in chunks as it is needed (if false)
*/
/* 
 * prealloc为true，需要预分配内存空间，申请相同大小的slab页
 * 每个slab存放同样大小的chunk元素
 */
void slabs_init(const size_t limit, const double factor, const bool prealloc);


/**
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 * 根据元素的大小返回一个适合的slabs_classid，返回0代表出错
 */
unsigned int slabs_clsid(const size_t size);

/** Allocate object of given length. 0 on error */ /*@null@*/
/* 从指定id的slabclass中分配一个新的item */
void *slabs_alloc(const size_t size, unsigned int id);

/** Free previously allocated object */
/* 释放slabclass[id]中ptr指针所指向的元素，初始化相应值 */
void slabs_free(void *ptr, size_t size, unsigned int id);

/** Adjust the stats for memory requested */
/* 调整old_item和新item所占空间的差值 */
void slabs_adjust_mem_requested(unsigned int id, size_t old, size_t ntotal);

/** Return a datum for stats in binary protocol */
/* 更多的stats信息的处理 */
bool get_stats(const char *stat_type, int nkey, ADD_STAT add_stats, void *c);

/** Fill buffer with stats */ /*@null@*/
void slabs_stats(ADD_STAT add_stats, void *c);

int start_slab_maintenance_thread(void);
void stop_slab_maintenance_thread(void);

enum reassign_result_type {
    REASSIGN_OK=0, REASSIGN_RUNNING, REASSIGN_BADCLASS, REASSIGN_NOSPARE,
    REASSIGN_SRC_DST_SAME
};

enum reassign_result_type slabs_reassign(int src, int dst);

void slabs_rebalancer_pause(void);
void slabs_rebalancer_resume(void);

#endif
