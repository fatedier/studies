#ifndef __MTCP_H_
#define __MTCP_H_

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/queue.h>
#include <pthread.h>

#include "memory_mgt.h"
#include "tcp_ring_buffer.h"
#include "tcp_send_buffer.h"
#include "tcp_stream_queue.h"
#include "socket.h"
#include "mtcp_api.h"
#include "eventpoll.h"
#include "addr_pool.h"
#include "ps.h"
#include "logger.h"
#include "stat.h"
#include "io_module.h"

#ifndef TRUE
#define TRUE (1)
#endif

#ifndef FALSE
#define FALSE (0)
#endif

#ifndef ERROR
#define ERROR (-1)
#endif

#define MAX_CPUS 16		// 支持的最大cpu核数

#define ETHERNET_HEADER_LEN		14	// sizeof(struct ethhdr)
#define IP_HEADER_LEN			20	// sizeof(struct iphdr)
#define TCP_HEADER_LEN			20	// sizeof(struct tcphdr)
#define TOTAL_TCP_HEADER_LEN	54	// total header length

/* configrations */
#define BACKLOG_SIZE (10*1024)
#define MAX_PKT_SIZE (2*1024)
#define ETH_NUM 4

#define TCP_OPT_TIMESTAMP_ENABLED   TRUE	/* enabled for rtt measure */
#define TCP_OPT_SACK_ENABLED        FALSE	/* not implemented */

#define LOCK_STREAM_QUEUE	FALSE
#define USE_SPIN_LOCK		TRUE
#define INTR_SLEEPING_MTCP	TRUE
#define PROMISCUOUS_MODE	TRUE

/* blocking api became obsolete */
#define BLOCKING_SUPPORT	FALSE

/*----------------------------------------------------------------------------*/
/* Statistics */
#ifdef NETSTAT
#define NETSTAT_PERTHREAD	TRUE
#define NETSTAT_TOTAL		TRUE
#endif /* NETSTAT */
#define RTM_STAT			FALSE
/*----------------------------------------------------------------------------*/
/* Lock definitions for socket buffer */
#if USE_SPIN_LOCK
#define SBUF_LOCK_INIT(lock, errmsg, action);		\
	if (pthread_spin_init(lock, PTHREAD_PROCESS_PRIVATE)) {		\
		perror("pthread_spin_init" errmsg);			\
		action;										\
	}
#define SBUF_LOCK_DESTROY(lock)	pthread_spin_destroy(lock)
#define SBUF_LOCK(lock)			pthread_spin_lock(lock)
#define SBUF_UNLOCK(lock)		pthread_spin_unlock(lock)
#else
#define SBUF_LOCK_INIT(lock, errmsg, action);		\
	if (pthread_mutex_init(lock, NULL)) {			\
		perror("pthread_mutex_init" errmsg);		\
		action;										\
	}
#define SBUF_LOCK_DESTROY(lock)	pthread_mutex_destroy(lock)
#define SBUF_LOCK(lock)			pthread_mutex_lock(lock)
#define SBUF_UNLOCK(lock)		pthread_mutex_unlock(lock)
#endif /* USE_SPIN_LOCK */
/*----------------------------------------------------------------------------*/
struct eth_table
{
	char dev_name[128];					// 网卡端口名称
	int ifindex;						// dpdk网卡端口序号
	int stat_print;						// 是否打印统计信息
	unsigned char haddr[ETH_ALEN];		// mac地址
	uint32_t netmask;					// 子网掩码
//	unsigned char dst_haddr[ETH_ALEN];
	uint32_t ip_addr;					// 网卡ip地址
};
/*----------------------------------------------------------------------------*/
struct route_table
{
	uint32_t daddr;		// 目的地址
	uint32_t mask;		// 掩码
	uint32_t masked;
	int prefix;
	int nif;
};
/*----------------------------------------------------------------------------*/
struct arp_entry
{
	uint32_t ip;
	int8_t prefix;
	uint32_t ip_mask;
	uint32_t ip_masked;
	unsigned char haddr[ETH_ALEN];
};
/*----------------------------------------------------------------------------*/
struct arp_table
{
	struct arp_entry *entry;
	int entries;
};
/*----------------------------------------------------------------------------*/
struct mtcp_config
{
	/* socket mode */
	int8_t socket_mode;

	/* network interface config */
	struct eth_table *eths;			// 网卡信息
	int eths_num;					// 使用的网卡端口数

	/* route config */
	struct route_table *rtable;		// routing table(路由表)
	int routes;						// # of entries (总路由数)

	/* arp config */
	struct arp_table arp;			// arp列表

	int num_cores;				// 指定使用的cpu核数
	int num_mem_ch;				// 每个socket的内存通道数
	int max_concurrency;		// 每个核上的最高并发

	int max_num_buffers;		// 每个核预分配的缓存数
	int rcvbuf_size;
	int sndbuf_size;
	
	int tcp_timewait;
	int tcp_timeout;

	/* adding multi-process support */
	uint8_t multi_process;
	uint8_t multi_process_is_master;
	uint8_t multi_process_curr_core;
};
/*----------------------------------------------------------------------------*/
struct mtcp_context
{
	int cpu;
};
/*----------------------------------------------------------------------------*/
// mtcp数据发送管理对象
struct mtcp_sender
{
	int ifidx;

	/* TCP layer send queues */
	TAILQ_HEAD (control_head, tcp_stream) control_list;
	TAILQ_HEAD (send_head, tcp_stream) send_list;			// 待发送的tcp连接队列
	TAILQ_HEAD (ack_head, tcp_stream) ack_list;

	int control_list_cnt;
	int send_list_cnt;			// 待发送的tcp连接的数量
	int ack_list_cnt;
};
/*----------------------------------------------------------------------------*/
// mtcp管理对象
struct mtcp_manager
{
	// tcp_stream
	mem_pool_t flow_pool;		/* memory pool for tcp_stream */
	// tcp_recv_vars
	mem_pool_t rv_pool;			/* memory pool for recv variables */
	// tcp_send_vars
	mem_pool_t sv_pool;			/* memory pool for send variables */
	// 
	mem_pool_t mv_pool;			/* memory pool for monitor variables */

	//mem_pool_t socket_pool;
	sb_manager_t rbm_snd;		// tcp发送管理对象
	rb_manager_t rbm_rcv;		// tcp接收管理对象
	struct hashtable *tcp_flow_table;		// tcp流量表

	uint32_t s_index:24;		/* stream index */
	socket_map_t smap;								// socket对象数组，下标为socket_fd
	TAILQ_HEAD (, socket_map) free_smap;			// 空闲的socket对象队列

	addr_pool_t ap;			/* address pool */

	uint32_t g_id;			/* id space in a thread */
	uint32_t flow_cnt;		/* number of concurrent flows */ //同时进行的流的数量

	struct mtcp_thread_context* ctx;		// mtcp线程环境信息
	
	/* variables related to logger */
	int sp_fd;
	log_thread_context* logger;
	log_buff* w_buffer;
	FILE *log_fp;

	/* variables related to event */
	struct mtcp_epoll *ep;				// mtcp内部epoll管理对象
	uint32_t ts_last_event;				// 最后一次事件触发的时间

	struct tcp_listener *listener;

	stream_queue_t connectq;				/* streams need to connect */
	stream_queue_t sendq;				/* streams need to send data */
	stream_queue_t ackq;					/* streams need to send ack */

	stream_queue_t closeq;				/* streams need to close */
	stream_queue_int *closeq_int;		/* internally maintained closeq */
	stream_queue_t resetq;				/* streams need to reset */
	stream_queue_int *resetq_int;		/* internally maintained resetq */
	
	stream_queue_t destroyq;				/* streams need to be destroyed */

	struct mtcp_sender *g_sender;
	struct mtcp_sender *n_sender[ETH_NUM];

	/* lists related to timeout */
	struct rto_hashstore* rto_store;
	TAILQ_HEAD (timewait_head, tcp_stream) timewait_list;
	TAILQ_HEAD (timeout_head, tcp_stream) timeout_list;

	int rto_list_cnt;
	int timewait_list_cnt;
	int timeout_list_cnt;

#if BLOCKING_SUPPORT
	TAILQ_HEAD (rcv_br_head, tcp_stream) rcv_br_list;
	TAILQ_HEAD (snd_br_head, tcp_stream) snd_br_list;
	int rcv_br_list_cnt;
	int snd_br_list_cnt;
#endif

	uint32_t cur_ts;				// 当前时间

	int wakeup_flag;
	int is_sleeping;

	/* statistics */
	struct bcast_stat bstat;
	struct timeout_stat tstat;
#ifdef NETSTAT
	struct net_stat nstat;
	struct net_stat p_nstat;
	uint32_t p_nstat_ts;

	struct run_stat runstat;		// 运行状态
	struct run_stat p_runstat;

	struct time_stat rtstat;
#endif /* NETSTAT */
	struct io_module_func *iom;		// 底层io模块
};
/*----------------------------------------------------------------------------*/
typedef struct mtcp_manager* mtcp_manager_t;
/*----------------------------------------------------------------------------*/
mtcp_manager_t 
GetMTCPManager(mctx_t mctx);
/*----------------------------------------------------------------------------*/
// mtcp线程环境信息
struct mtcp_thread_context
{
	int cpu;							// 对应cpu序号
	pthread_t thread;					// 当前线程id
	uint8_t done:1, 					// 完成标志
			exit:1, 					// 退出标志
			interrupt:1;

	struct mtcp_manager* mtcp_manager;	// mtcp管理对象

	// 对应dpdk的环境信息
	void *io_private_context;
	pthread_mutex_t smap_lock;			// 操作socket对象时的锁
	pthread_mutex_t flow_pool_lock;
	pthread_mutex_t socket_pool_lock;

#if LOCK_STREAM_QUEUE
#if USE_SPIN_LOCK
	pthread_spinlock_t connect_lock;
	pthread_spinlock_t close_lock;
	pthread_spinlock_t reset_lock;
	pthread_spinlock_t sendq_lock;
	pthread_spinlock_t ackq_lock;
	pthread_spinlock_t destroyq_lock;
#else
	pthread_mutex_t connect_lock;
	pthread_mutex_t close_lock;
	pthread_mutex_t reset_lock;
	pthread_mutex_t sendq_lock;
	pthread_mutex_t ackq_lock;
	pthread_mutex_t destroyq_lock;
#endif /* USE_SPIN_LOCK */
#endif /* LOCK_STREAM_QUEUE */
};
/*----------------------------------------------------------------------------*/
typedef struct mtcp_thread_context* mtcp_thread_context_t;
/*----------------------------------------------------------------------------*/
struct mtcp_manager *g_mtcp[MAX_CPUS];		// 每个cpu对应的mtcp_manager数组
struct mtcp_config CONFIG;
addr_pool_t ap;								// 地址池，每个网卡端口的所有对应端口的socket都有一个
/*----------------------------------------------------------------------------*/

#endif /* __MTCP_H_ */
