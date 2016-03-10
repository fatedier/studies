#ifndef __LOGGER_H_
#define __LOGGER_H_

#include <stdint.h>

#define LOG_BUFF_SIZE (256*1024)
#define NUM_LOG_BUFF (100)

enum {
	IDLE_LOGT,
	ACTIVE_LOGT
} log_thread_state;

typedef struct log_buff
{
	int tid;							// 线程号
	FILE* fid;							// 日志文件描述符
	int buff_len;
	char buff[LOG_BUFF_SIZE];			// 缓冲区
	TAILQ_ENTRY(log_buff) buff_link;
} log_buff;

typedef struct log_thread_context {
	pthread_t thread;
	int cpu;			// 对应cpu
	int done;
	int sp_fd;			// 管道用于读
	int pair_sp_fd;		// 管道用于写
	int free_buff_cnt;
	int job_buff_cnt;

	uint8_t state;
	
	pthread_mutex_t mutex;
	pthread_mutex_t free_mutex;

	TAILQ_HEAD(, log_buff) working_queue;	// 工作队列
	TAILQ_HEAD(, log_buff) free_queue;		// 空闲队列

} log_thread_context;

log_buff* DequeueFreeBuffer (log_thread_context *ctx);
void EnqueueJobBuffer(log_thread_context *ctx, log_buff* working_bp);
void InitLogThreadContext (log_thread_context *ctx, int cpu);
void *ThreadLogMain(void* arg);

#endif /* __LOGGER_H_ */
