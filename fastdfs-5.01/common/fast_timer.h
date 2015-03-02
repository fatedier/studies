#ifndef __FAST_TIMER_H__
#define __FAST_TIMER_H__

#include <stdint.h>
#include "common_define.h"

/* 时间轮算法相关函数 */

typedef struct fast_timer_entry {
  int64_t expires;					/* 过期时间 */
  void *data;						/* 节点存放数据 */
  struct fast_timer_entry *prev;		/* 前驱节点 */
  struct fast_timer_entry *next;		/* 后继节点 */
  bool rehash;
} FastTimerEntry;		/* 槽中的一个节点，双向链表 */

typedef struct fast_timer_slot {
  struct fast_timer_entry head;	/* 单个槽中的头节点，采用双向链表存储 */
} FastTimerSlot;	/* 时间轮算法中的一个槽 */

typedef struct fast_timer {
  int slot_count;    /* time wheel slot count(时间轮中的槽数) */
  int64_t base_time; /* base time for slot 0(slot 0 中的时间) */
  int64_t current_time;	/* 当前时间 */
  FastTimerSlot *slots;		/* slot数组 */
} FastTimer;	/* 时间轮结构体 */

#ifdef __cplusplus
extern "C" {
#endif

/* 初始化时间轮结构 */
int fast_timer_init(FastTimer *timer, const int slot_count,
    const int64_t current_time);

/* 销毁时间轮相关资源 */
void fast_timer_destroy(FastTimer *timer);

/* 将指定节点加入到时间轮中 */
int fast_timer_add(FastTimer *timer, FastTimerEntry *entry);

/* 删除时间轮的指定节点 */
int fast_timer_remove(FastTimer *timer, FastTimerEntry *entry);

/* 修改指定节点的过期时间 */
int fast_timer_modify(FastTimer *timer, FastTimerEntry *entry,
    const int64_t new_expires);

/* 根据指定时间和base_time返回所在slot的指针，并且timer->current_time加1 */
FastTimerSlot *fast_timer_slot_get(FastTimer *timer, const int64_t current_time);

/*
 * 获取所有过期时间在timer->current_time到指定current_time之间的节点
 * 所有的过期节点最终会存放在head所指向的双向链表中
 */
int fast_timer_timeouts_get(FastTimer *timer, const int64_t current_time,
   FastTimerEntry *head);

#ifdef __cplusplus
}
#endif

#endif

