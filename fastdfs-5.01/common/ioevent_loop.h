#ifndef _IOEVENT_LOOP_H
#define _IOEVENT_LOOP_H

#include "fast_task_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

/* 
 * 循环等待IO事件触发，调用回调函数进行处理 
 * pThreadData是线程自己的数据
 * recv_notify_callback是每个线程的管道pipe_fds[0]的READ事件的回调函数
 * clean_up_callback是需要删除的IO事件的回调函数
 * continue_flag 指向一个volatile 的bool变量，如果为false，则停止循环
 */
int ioevent_loop(struct nio_thread_data *pThreadData,
	IOEventCallback recv_notify_callback, TaskCleanUpCallBack
	clean_up_callback, volatile bool *continue_flag);

/* 
 * 将sock描述符加入到监听集合中
 * 等待event指定事件触发后，执行callback回调函数
 * timeout时间后过期，利用pThread线程数据中的timer定时轮对象来进行是否过期的判断
 */
int ioevent_set(struct fast_task_info *pTask, struct nio_thread_data *pThread,
	int sock, short event, IOEventCallback callback, const int timeout);

#ifdef __cplusplus
}
#endif

#endif

