/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#ifndef _SCHED_THREAD_H_
#define _SCHED_THREAD_H_

/* 定时任务调度线程模块 */

#include <time.h>
#include "common_define.h"

/* 回调函数声明 */
typedef int (*TaskFunc) (void *args);

typedef struct tagScheduleEntry
{
	int id;  //the task id

	/* the time base to execute task, such as 00:00, interval is 3600,
           means execute the task every hour as 1:00, 2:00, 3:00 etc. */
	TimeInfo time_base;

	int interval;   //the interval for execute task, unit is second

	TaskFunc task_func; //callback function
	void *func_args;    //arguments pass to callback function

	/* following are internal fields, do not set manually! */
	time_t next_call_time;  
	struct tagScheduleEntry *next;
} ScheduleEntry;		/* 定时任务节点结构 */

typedef struct
{
	ScheduleEntry *entries;	/* 节点 */
	int count;			/* 总数 */
} ScheduleArray;		/* 定时任务链表 */

typedef struct
{
	ScheduleArray scheduleArray;			/* 定时任务链表信息 */
	ScheduleEntry *head;  			/* schedule chain head(链表头指针) */
        ScheduleEntry *tail;  			/* schedule chain tail(链表尾指针) */
	bool *pcontinue_flag;			/* 是否继续的标志 */
} ScheduleContext;	/* 定时任务总体信息 */

#ifdef __cplusplus
extern "C" {
#endif

extern volatile bool g_schedule_flag; 	/* schedule continue running flag(是否继续执行定时任务的标志) */
extern volatile time_t g_current_time;     /* the current time */

/* 获取当前时间，定时任务线程在执行的话从g_current_time取，避免频繁进行系统调用 */
#define get_current_time() (g_schedule_flag ? g_current_time: time(NULL))

/* 添加定时任务 */
int sched_add_entries(const ScheduleArray *pScheduleArray);

/* 根据任务id删除指定的定时任务 */
int sched_del_entry(const int id);

/** execute the schedule thread
 *  parameters:
 *  	     pScheduleArray: schedule task
 *  	     ptid: store the schedule thread id
 *  	     stack_size: set thread stack size (byes)
 *  	     pcontinue_flag: main process continue running flag
 * return: error no, 0 for success, != 0 fail
*/
/* 开始运行定时任务调度线程 */
int sched_start(ScheduleArray *pScheduleArray, pthread_t *ptid, \
		const int stack_size, bool * volatile pcontinue_flag);

#ifdef __cplusplus
}
#endif

#endif

