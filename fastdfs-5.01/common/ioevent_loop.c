#include "sched_thread.h"
#include "logger.h"
#include "ioevent_loop.h"

/* 处理数量为count的已准备好的IO事件 */
static void deal_ioevents(IOEventPoller *ioevent, const int count)
{
	int i;
	int event;
	IOEventEntry *pEntry;
	for (i=0; i<count; i++)
	{
		/* 取第i个已就绪的文件描述符的事件 */
		event = IOEVENT_GET_EVENTS(ioevent, i);

		/* 取IO事件对应的数据 */
		pEntry = (IOEventEntry *)IOEVENT_GET_DATA(ioevent, i);

		/* 调用之前注册的回调函数处理此次IO事件 */
		pEntry->callback(pEntry->fd, event, pEntry->timer.data);
	}
}

/* 处理head指向的由所有过期事件组成的双向链表 */
static void deal_timeouts(FastTimerEntry *head)
{
	FastTimerEntry *entry;
	FastTimerEntry *curent;
	IOEventEntry *pEventEntry;

	entry = head->next;
	while (entry != NULL)
	{
		curent = entry;
		entry = entry->next;

		/* 遍历每一个节点，调用回调函数处理过期事件 */
		pEventEntry = (IOEventEntry *)curent->data;
		if (pEventEntry != NULL)
		{
			pEventEntry->callback(pEventEntry->fd, IOEVENT_TIMEOUT,
						curent->data);
		}
	}
}

/* 
 * 循环等待IO事件触发，调用回调函数进行处理 
 * pThreadData是线程自己的数据
 * recv_notify_callback是每个线程的管道pipe_fds[0]的READ事件的回调函数
 * clean_up_callback是需要删除的IO事件的回调函数
 * continue_flag 指向一个volatile 的bool变量，如果为false，则停止循环
 */
int ioevent_loop(struct nio_thread_data *pThreadData,
	IOEventCallback recv_notify_callback, TaskCleanUpCallBack
	clean_up_callback, volatile bool *continue_flag)
{
	int result;
	IOEventEntry ev_notify;
	FastTimerEntry head;
	struct fast_task_info *pTask;
	time_t last_check_time;
	int count;

	memset(&ev_notify, 0, sizeof(ev_notify));

	/* 每个工作线程对应的管道的读文件描述符 */
	ev_notify.fd = pThreadData->pipe_fds[0];
	/* 当pipe[0]可读时，调用recv_notify_callback进行处理，是新建立连接的socket描述符 */
	ev_notify.callback = recv_notify_callback;

	/* 将线程对应的管道的读文件描述符加入到监听集合中 */
	if (ioevent_attach(&pThreadData->ev_puller,
		pThreadData->pipe_fds[0], IOEVENT_READ,
		&ev_notify) != 0)
	{
		result = errno != 0 ? errno : ENOMEM;
		logCrit("file: "__FILE__", line: %d, " \
			"ioevent_attach fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	/* 初始化最后一次检查时间 */
	last_check_time = g_current_time;
	while (*continue_flag)
	{
		pThreadData->deleted_list = NULL;
		/* 阻塞直到有IO事件被触发，超时时间在ev_puller中timeout变量设置 */
		count = ioevent_poll(&pThreadData->ev_puller);
		/* count值为已准备好的文件描述符个数，如果返回0表示超时，<0表示出错 */
		if (count > 0)
		{
			/* 处理IO事件 */
			deal_ioevents(&pThreadData->ev_puller, count);
		}
		else if (count < 0)
		{
			result = errno != 0 ? errno : EINVAL;
			if (result != EINTR)
			{
				logError("file: "__FILE__", line: %d, " \
					"ioevent_poll fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, STRERROR(result));
				return result;
			}
		}

		/* 如果有需要删除的IO事件 */
		if (pThreadData->deleted_list != NULL)
		{
			count = 0;
			while (pThreadData->deleted_list != NULL)
			{
				pTask = pThreadData->deleted_list;
				pThreadData->deleted_list = pTask->next;

				/* 调用clean_up_callback回调函数处理要删除的IO事件 */
				clean_up_callback(pTask);
				count++;
			}
			logInfo("cleanup task count: %d", count);
		}

		if (g_current_time - last_check_time > 0)
		{
			/* 更新最近一次检查时间 */
			last_check_time = g_current_time;
			/*
			 * 获取所有过期时间在timer->current_time到g_current_time的节点
			 * 所有的过期节点最终会存放在head所指向的双向链表中
			 */
			count = fast_timer_timeouts_get(
				&pThreadData->timer, g_current_time, &head);

			if (count > 0)
			{
				/* 处理head指向的由所有过期事件组成的双向链表 */
				deal_timeouts(&head);
			}
		}
	}

	return 0;
}

/* 
 * 将sock描述符加入到监听集合中
 * 等待event指定事件触发后，执行callback回调函数
 * timeout时间后过期，利用pThread线程数据中的timer定时轮对象来进行是否过期的判断
 */
int ioevent_set(struct fast_task_info *pTask, struct nio_thread_data *pThread,
	int sock, short event, IOEventCallback callback, const int timeout)
{
	int result;

	/* 线程数据 */
	pTask->thread_data = pThread;
	/* IO事件关联的socket描述符 */
	pTask->event.fd = sock;
	/* IO事件触发后的回调函数 */
	pTask->event.callback = callback;

	/* 将此socket描述符加入到监听集合，等待事件触发 */
	if (ioevent_attach(&pThread->ev_puller,
		sock, event, pTask) < 0)
	{
		result = errno != 0 ? errno : ENOENT;
		logError("file: "__FILE__", line: %d, " \
			"ioevent_attach fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	/* 设置时间轮的节点中存放的任务数据 */
	pTask->event.timer.data = pTask;
	/* 设置当前事件的过期时间 */
	pTask->event.timer.expires = g_current_time + timeout;

	/* 将此事件加入到时间轮中 */
	result = fast_timer_add(&pThread->timer, &pTask->event.timer);
	if (result != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"fast_timer_add fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	return 0;
}

