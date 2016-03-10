#ifndef __EVENTPOLL_H_
#define __EVENTPOLL_H_

#include "mtcp_api.h"
#include "mtcp_epoll.h"

/*----------------------------------------------------------------------------*/
// mtcp-epoll状态信息
struct mtcp_epoll_stat
{
	uint64_t calls;
	uint64_t waits;				// wait次数
	uint64_t wakes;				// epoll_wait被唤醒次数

	uint64_t issued;
	uint64_t registered;		// 总注册事件数
	uint64_t invalidated;		// 处理过程中无效事件数
	uint64_t handled;			// 事件处理总数
};
/*----------------------------------------------------------------------------*/
// 事件及其所属socket_id
struct mtcp_epoll_event_int
{
	struct mtcp_epoll_event ev;
	int sockid;
};
/*----------------------------------------------------------------------------*/
// 事件队列的类型
enum event_queue_type
{
	USR_EVENT_QUEUE = 0, 
	USR_SHADOW_EVENT_QUEUE = 1, 
	MTCP_EVENT_QUEUE = 2
};
/*----------------------------------------------------------------------------*/
// 事件队列
struct event_queue
{
	struct mtcp_epoll_event_int *events;		// 具体事件
	int start;			// starting index		// 起始event的序号
	int end;			// ending index			// 最后一个event的序号
	
	int size;			// max size				// 最大长度
	int num_events;		// number of events		// 当前事件数
};
/*----------------------------------------------------------------------------*/
// mtcp的epoll总对象
struct mtcp_epoll
{
	struct event_queue *usr_queue;			// 从mtcp队列移过来的已经触发了的事件
	struct event_queue *usr_shadow_queue;	// 用户设置的需要监听的事件
	struct event_queue *mtcp_queue;			// 存放触发了的事件，比如从网卡收到数据包

	uint8_t waiting;						// 处于epoll_wait状态的标志
	struct mtcp_epoll_stat stat;			// 状态信息
	
	pthread_cond_t epoll_cond;				// 条件变量，epoll_wait利用pthread_cond_wait实现阻塞，当有事件触发时调用pthread_cond_signal激活
	pthread_mutex_t epoll_lock;				// 线程锁
};
/*----------------------------------------------------------------------------*/

int 
CloseEpollSocket(mctx_t mctx, int epid);

#endif /* __EVENTPOLL_H_ */
