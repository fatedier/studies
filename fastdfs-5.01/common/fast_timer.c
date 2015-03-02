#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "logger.h"
#include "fast_timer.h"

/* 初始化时间轮结构 */
int fast_timer_init(FastTimer *timer, const int slot_count,
    const int64_t current_time)
{
  int bytes;
  if (slot_count <= 0 || current_time <= 0) {
    return EINVAL;
  }

  /* 设置时间轮槽数 */
  timer->slot_count = slot_count;
  /* 设置时间轮base_time */
  timer->base_time = current_time; //base time for slot 0
  timer->current_time = current_time;
  bytes = sizeof(FastTimerSlot) * slot_count;
  /* 为FastTimerSlot分配空间 */
  timer->slots = (FastTimerSlot *)malloc(bytes);
  if (timer->slots == NULL) {
     return errno != 0 ? errno : ENOMEM;
  }
  memset(timer->slots, 0, bytes);
  return 0;
}

/* 销毁时间轮相关资源 */
void fast_timer_destroy(FastTimer *timer)
{
  if (timer->slots != NULL) {
    free(timer->slots);
    timer->slots = NULL;
  }
}

/* 根据指定时间和base_time计算在哪个slot中 */
#define TIMER_GET_SLOT_INDEX(timer, expires) \
  (((expires) - timer->base_time) % timer->slot_count)

/* 根据指定时间和base_time返回所在slot的指针 */
#define TIMER_GET_SLOT_POINTER(timer, expires) \
  (timer->slots + TIMER_GET_SLOT_INDEX(timer, expires))

/* 将指定节点加入到时间轮中 */
int fast_timer_add(FastTimer *timer, FastTimerEntry *entry)
{
  FastTimerSlot *slot;

  /* 根据要添加的节点的过期时间获取所在slot指针 */
  slot = TIMER_GET_SLOT_POINTER(timer, entry->expires >
     timer->current_time ? entry->expires : timer->current_time);
  /* 将新增节点添加到所在slot的head之后 */
  entry->next = slot->head.next;
  if (slot->head.next != NULL) {
    slot->head.next->prev = entry;
  }
  entry->prev = &slot->head;
  slot->head.next = entry;
  return 0;
}

/* 修改指定节点的过期时间 */
int fast_timer_modify(FastTimer *timer, FastTimerEntry *entry,
    const int64_t new_expires)
{
  if (new_expires == entry->expires) {
    return 0;
  }

  /* 如果新的过期时间小于旧的时间，删除这个节点，以当前时间新增一个节点 */
  if (new_expires < entry->expires) {
    fast_timer_remove(timer, entry);
    entry->expires = new_expires;
    return fast_timer_add(timer, entry);
  }

  /* 如果新的过期时间计算后的slot和原来不是同一个，需要rehash */
  entry->rehash = TIMER_GET_SLOT_INDEX(timer, new_expires) !=
      TIMER_GET_SLOT_INDEX(timer, entry->expires);
  entry->expires = new_expires;  //lazy move
  return 0;
}

/* 删除时间轮的指定节点 */
int fast_timer_remove(FastTimer *timer, FastTimerEntry *entry)
{
  /* 在双向链表中删除指定节点 */
  if (entry->prev == NULL) {
     return ENOENT;   //already removed
  }

  if (entry->next != NULL) {
     entry->next->prev = entry->prev;
     entry->prev->next = entry->next;
     entry->next = NULL;
  }
  else {
     entry->prev->next = NULL;
  }

  entry->prev = NULL;
  return 0;
}

/* 根据指定时间和base_time返回所在slot的指针，并且timer->current_time加1 */
FastTimerSlot *fast_timer_slot_get(FastTimer *timer, const int64_t current_time)
{
  /* 如果当前时间小于timer时间的当前时间，返回NULL */
  if (timer->current_time >= current_time) {
    return NULL;
  }

  /* 返回当前时间所在slot，并且timer的当前时间加1 */
  return TIMER_GET_SLOT_POINTER(timer, timer->current_time++);
}

/*
 * 获取所有过期时间在timer->current_time到指定current_time之间的节点
 * 所有的过期节点最终会存放在head所指向的双向链表中
 */
int fast_timer_timeouts_get(FastTimer *timer, const int64_t current_time,
   FastTimerEntry *head)
{
  FastTimerSlot *slot;
  FastTimerEntry *entry;
  FastTimerEntry *first;
  FastTimerEntry *last;
  FastTimerEntry *tail;
  int count;	/* 记录过期节点数 */
  head->prev = NULL;
  head->next = NULL;
  if (timer->current_time >= current_time) {
    return 0;
  }

  first = NULL;
  last = NULL;
  tail = head;
  count = 0;
  /* 
   * 从timer->current_time开始直到current_time，每次循环+1，遍历查找所有过期节点
   * 过期节点添加到head所指向的链表中
   */
  while (timer->current_time < current_time) 
  {
    /* 获取时间轮当前时间所在slot */
    slot = TIMER_GET_SLOT_POINTER(timer, timer->current_time++);
    /* entry指向slot的第一个有效节点 */
    entry = slot->head.next;
    while (entry != NULL) 
    {
      /* 没有达到过期时间的节点 */
      if (entry->expires >= current_time)
      {
      	  /* 此时first指向前面的一个已经过期的节点 */
         if (first != NULL) 
	  {
	     /* 在时间轮的slot的链表中剔除过期的节点，加入到head所指向的链表中 */
            first->prev->next = entry;
            entry->prev = first->prev;

            tail->next = first;
            first->prev = tail;
            tail = last;	/* 此时的last指向连续的过期节点中的最后一个 */
            first = NULL;
         }
	  /* 如果此节点没有过期并且需要rehash，重新分配slot，先删除再添加 */
         if (entry->rehash) 
	  {
           last = entry;
           entry = entry->next;

           last->rehash = false;
           fast_timer_remove(timer, last);
           fast_timer_add(timer, last);
           continue;
         }
      }
      /* 到达过期时间的节点 */
      else 
      {
         count++;
	  /* 如果是连续过期节点中的第一个，first指向该节点 */
         if (first == NULL) {
            first = entry;
         }
      }
	  
      last = entry;
      entry = entry->next;
    }

    /* 如果最后一个节点是过期节点 */
    if (first != NULL) 
    {
       first->prev->next = NULL;

       tail->next = first;
       first->prev = tail;
       tail = last;
       first = NULL;
    }
  }

  /* 如果有过期节点，tail->next指向NULL */
  if (count > 0) 
  {
     tail->next = NULL;
  }

  return count;
}

