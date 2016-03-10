#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <sys/time.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <signal.h>
#include <assert.h>
#include <sched.h>

#include "cpu.h"
#include "ps.h"
#include "eth_in.h"
#include "fhash.h"
#include "tcp_send_buffer.h"
#include "tcp_ring_buffer.h"
#include "socket.h"
#include "eth_out.h"
#include "tcp_in.h"
#include "tcp_out.h"
#include "mtcp_api.h"
#include "eventpoll.h"
#include "logger.h"
#include "config.h"
#include "arp.h"
#include "ip_out.h"
#include "timer.h"
#include "debug.h"

#ifndef DISABLE_DPDK
/* for launching rte thread */
#include <rte_launch.h>
#include <rte_lcore.h>
#endif

#define PS_CHUNK_SIZE 64
#define RX_THRESH (PS_CHUNK_SIZE * 0.8)

#define ROUND_STAT FALSE
#define EVENT_STAT FALSE
#define TESTING FALSE

#define LOG_FILE_NAME "log"
#define MAX_FILE_NAME 1024

#define MAX(a, b) ((a)>(b)?(a):(b))
#define MIN(a, b) ((a)<(b)?(a):(b))

#define PER_STREAM_SLICE 0.1		// in ms
#define PER_STREAM_TCHECK 1			// in ms
#define PS_SELECT_TIMEOUT 100		// in us 

#define GBPS(bytes) (bytes * 8.0 / (1000 * 1000 * 1000))

/*----------------------------------------------------------------------------*/
/* handlers for threads */
struct mtcp_thread_context *g_pctx[MAX_CPUS];		// cpu对应线程的环境信息
struct log_thread_context *g_logctx[MAX_CPUS];		// 日志对象信息
/*----------------------------------------------------------------------------*/
static pthread_t g_thread[MAX_CPUS];
static pthread_t log_thread[MAX_CPUS];;
/*----------------------------------------------------------------------------*/
static sem_t g_init_sem[MAX_CPUS];			// 和cpu相关的信号量
static int running[MAX_CPUS];				// 是否正在运行的标志，索引为cpu序号
/*----------------------------------------------------------------------------*/
mtcp_sighandler_t app_signal_handler;
static int sigint_cnt[MAX_CPUS];
static struct timeval sigint_ts[MAX_CPUS];
/*----------------------------------------------------------------------------*/
static int mtcp_master = -1;				// 主cpu
/*----------------------------------------------------------------------------*/
// 信号处理函数
void
HandleSignal(int signal)
{
	int i = 0;

	if (signal == SIGINT) {
		int core;
		struct timeval cur_ts;

		core = sched_getcpu();
		gettimeofday(&cur_ts, NULL);

		if (sigint_cnt[core] > 0 && cur_ts.tv_sec > sigint_ts[core].tv_sec) {
			for (i = 0; i < num_cpus; i++) {
				if (running[i]) {
					g_pctx[i]->exit = TRUE;
				}
			}
		} else {
			for (i = 0; i < num_cpus; i++) {
				g_pctx[i]->interrupt = TRUE;
			}
			if (!app_signal_handler) {
				for (i = 0; i < num_cpus; i++) {
					if (running[i]) {
						g_pctx[i]->exit = TRUE;
					}
				}
			}
		}
		sigint_cnt[core]++;
		gettimeofday(&sigint_ts[core], NULL);
	}

	// 调用用户设置的其他信号处理函数
	if (signal != SIGUSR1) {
		if (app_signal_handler) {
			app_signal_handler(signal);
		}
	}
}
/*----------------------------------------------------------------------------*/
static int 
AttachDevice(struct mtcp_thread_context* ctx)
{
	int working = -1;
	mtcp_manager_t mtcp = ctx->mtcp_manager;

	// 这步放到mtcp_init里做了，绑定网卡之类的
	working = mtcp->iom->link_devices(ctx);

	return working;
}
/*----------------------------------------------------------------------------*/
#ifdef NETSTAT
static inline void 
InitStatCounter(struct stat_counter *counter)
{
	counter->cnt = 0;
	counter->sum = 0;
	counter->max = 0;
	counter->min = 0;
}
/*----------------------------------------------------------------------------*/
static inline void 
UpdateStatCounter(struct stat_counter *counter, int64_t value)
{
	counter->cnt++;
	counter->sum += value;
	if (value > counter->max)
		counter->max = value;
	if (counter->min == 0 || value < counter->min)
		counter->min = value;
}
/*----------------------------------------------------------------------------*/
static inline uint64_t 
GetAverageStat(struct stat_counter *counter)
{
	return counter->cnt ? (counter->sum / counter->cnt) : 0;
}
/*----------------------------------------------------------------------------*/
static inline int64_t 
TimeDiffUs(struct timeval *t2, struct timeval *t1)
{
	return (t2->tv_sec - t1->tv_sec) * 1000000 + 
			(int64_t)(t2->tv_usec - t1->tv_usec);
}
/*----------------------------------------------------------------------------*/
static inline void 
PrintThreadNetworkStats(mtcp_manager_t mtcp, struct net_stat *ns)
{
	int i;

	for (i = 0; i < CONFIG.eths_num; i++) {
		ns->rx_packets[i] = mtcp->nstat.rx_packets[i] - mtcp->p_nstat.rx_packets[i];
		ns->rx_errors[i] = mtcp->nstat.rx_errors[i] - mtcp->p_nstat.rx_errors[i];
		ns->rx_bytes[i] = mtcp->nstat.rx_bytes[i] - mtcp->p_nstat.rx_bytes[i];
		ns->tx_packets[i] = mtcp->nstat.tx_packets[i] - mtcp->p_nstat.tx_packets[i];
		ns->tx_drops[i] = mtcp->nstat.tx_drops[i] - mtcp->p_nstat.tx_drops[i];
		ns->tx_bytes[i] = mtcp->nstat.tx_bytes[i] - mtcp->p_nstat.tx_bytes[i];
#if NETSTAT_PERTHREAD
		if (CONFIG.eths[i].stat_print) {
			fprintf(stderr, "[CPU%2d] %s flows: %6u, "
					"RX: %7ld(pps) (err: %5ld), %5.2lf(Gbps), "
					"TX: %7ld(pps), %5.2lf(Gbps)\n", 
					mtcp->ctx->cpu, CONFIG.eths[i].dev_name, mtcp->flow_cnt, 
					ns->rx_packets[i], ns->rx_errors[i], GBPS(ns->rx_bytes[i]), 
					ns->tx_packets[i], GBPS(ns->tx_bytes[i]));
		}
#endif
	}
	mtcp->p_nstat = mtcp->nstat;

}
/*----------------------------------------------------------------------------*/
#if ROUND_STAT
static inline void 
PrintThreadRoundStats(mtcp_manager_t mtcp, struct run_stat *rs)
{
#define ROUND_DIV (1000)
	rs->rounds = mtcp->runstat.rounds - mtcp->p_runstat.rounds;
	rs->rounds_rx = mtcp->runstat.rounds_rx - mtcp->p_runstat.rounds_rx;
	rs->rounds_rx_try = mtcp->runstat.rounds_rx_try - mtcp->p_runstat.rounds_rx_try;
	rs->rounds_tx = mtcp->runstat.rounds_tx - mtcp->p_runstat.rounds_tx;
	rs->rounds_tx_try = mtcp->runstat.rounds_tx_try - mtcp->p_runstat.rounds_tx_try;
	rs->rounds_select = mtcp->runstat.rounds_select - mtcp->p_runstat.rounds_select;
	rs->rounds_select_rx = mtcp->runstat.rounds_select_rx - mtcp->p_runstat.rounds_select_rx;
	rs->rounds_select_tx = mtcp->runstat.rounds_select_tx - mtcp->p_runstat.rounds_select_tx;
	rs->rounds_select_intr = mtcp->runstat.rounds_select_intr - mtcp->p_runstat.rounds_select_intr;
	rs->rounds_twcheck = mtcp->runstat.rounds_twcheck - mtcp->p_runstat.rounds_twcheck;
	mtcp->p_runstat = mtcp->runstat;
#if NETSTAT_PERTHREAD
	fprintf(stderr, "[CPU%2d] Rounds: %4ldK, "
			"rx: %3ldK (try: %4ldK), tx: %3ldK (try: %4ldK), "
			"ps_select: %4ld (rx: %4ld, tx: %4ld, intr: %3ld)\n", 
			mtcp->ctx->cpu, rs->rounds / ROUND_DIV, 
			rs->rounds_rx / ROUND_DIV, rs->rounds_rx_try / ROUND_DIV, 
			rs->rounds_tx / ROUND_DIV, rs->rounds_tx_try / ROUND_DIV, 
			rs->rounds_select, 
			rs->rounds_select_rx, rs->rounds_select_tx, rs->rounds_select_intr);
#endif
}
#endif /* ROUND_STAT */
/*----------------------------------------------------------------------------*/
#endif /* NETSTAT */
/*----------------------------------------------------------------------------*/
#if EVENT_STAT
static inline void 
PrintEventStat(int core, struct mtcp_epoll_stat *stat)
{
	fprintf(stderr, "[CPU%2d] calls: %lu, waits: %lu, wakes: %lu, "
			"issued: %lu, registered: %lu, invalidated: %lu, handled: %lu\n", 
			core, stat->calls, stat->waits, stat->wakes, 
			stat->issued, stat->registered, stat->invalidated, stat->handled);
	memset(stat, 0, sizeof(struct mtcp_epoll_stat));
}
#endif /* EVENT_STAT */
/*----------------------------------------------------------------------------*/
#ifdef NETSTAT
static inline void
PrintNetworkStats(mtcp_manager_t mtcp, uint32_t cur_ts)
{
#define TIMEOUT 1
	int i;
	struct net_stat ns;
#if ROUND_STAT
	struct run_stat rs;
#endif /* ROUND_STAT */
#ifdef NETSTAT_TOTAL
	int j;
	uint32_t gflow_cnt = 0;
	struct net_stat g_nstat;
#if ROUND_STAT
	struct run_stat g_runstat;
#endif /* ROUND_STAT */
#endif /* NETSTAT_TOTAL */

	if (TS_TO_MSEC(cur_ts - mtcp->p_nstat_ts) < SEC_TO_MSEC(TIMEOUT)) {
		return;
	}

	mtcp->p_nstat_ts = cur_ts;
	gflow_cnt = 0;
	memset(&g_nstat, 0, sizeof(struct net_stat));
	for (i = 0; i < CONFIG.num_cores; i++) {
		if (running[i]) {
			PrintThreadNetworkStats(g_mtcp[i], &ns);
#if NETSTAT_TOTAL
			gflow_cnt += g_mtcp[i]->flow_cnt;
			for (j = 0; j < CONFIG.eths_num; j++) {
				g_nstat.rx_packets[j] += ns.rx_packets[j];
				g_nstat.rx_errors[j] += ns.rx_errors[j];
				g_nstat.rx_bytes[j] += ns.rx_bytes[j];
				g_nstat.tx_packets[j] += ns.tx_packets[j];
				g_nstat.tx_drops[j] += ns.tx_drops[j];
				g_nstat.tx_bytes[j] += ns.tx_bytes[j];
			}
#endif
		}
	}
#if NETSTAT_TOTAL
	for (i = 0; i < CONFIG.eths_num; i++) {
		if (CONFIG.eths[i].stat_print) {
			fprintf(stderr, "[ ALL ] %s flows: %6u, "
					"RX: %7ld(pps) (err: %5ld), %5.2lf(Gbps), "
					"TX: %7ld(pps), %5.2lf(Gbps)\n", CONFIG.eths[i].dev_name, 
					gflow_cnt, g_nstat.rx_packets[i], g_nstat.rx_errors[i], 
					GBPS(g_nstat.rx_bytes[i]), g_nstat.tx_packets[i], 
					GBPS(g_nstat.tx_bytes[i]));
		}
	}
#endif

#if ROUND_STAT
	memset(&g_runstat, 0, sizeof(struct run_stat));
	for (i = 0; i < CONFIG.num_cores; i++) {
		if (running[i]) {
			PrintThreadRoundStats(g_mtcp[i], &rs);
#if 0
			g_runstat.rounds += rs.rounds;
			g_runstat.rounds_rx += rs.rounds_rx;
			g_runstat.rounds_rx_try += rs.rounds_rx_try;
			g_runstat.rounds_tx += rs.rounds_tx;
			g_runstat.rounds_tx_try += rs.rounds_tx_try;
			g_runstat.rounds_select += rs.rounds_select;
			g_runstat.rounds_select_rx += rs.rounds_select_rx;
			g_runstat.rounds_select_tx += rs.rounds_select_tx;
#endif
		}
	}
#if 0
	fprintf(stderr, "[ ALL ] Rounds: %4ldK, "
			"rx: %3ldK (try: %4ldK), tx: %3ldK (try: %4ldK), "
			"ps_select: %4ld (rx: %4ld, tx: %4ld)\n", 
			g_runstat.rounds / 1000, g_runstat.rounds_rx / 1000, 
			g_runstat.rounds_rx_try / 1000, g_runstat.rounds_tx / 1000, 
			g_runstat.rounds_tx_try / 1000, g_runstat.rounds_select, 
			g_runstat.rounds_select_rx, g_runstat.rounds_select_tx);
#endif
#endif /* ROUND_STAT */

#if EVENT_STAT
	for (i = 0; i < CONFIG.num_cores; i++) {
		if (running[i] && g_mtcp[i]->ep) {
			PrintEventStat(i, &g_mtcp[i]->ep->stat);
		}
	}
#endif

	fflush(stderr);
}
#endif /* NETSTAT */
/*----------------------------------------------------------------------------*/
#if BLOCKING_SUPPORT
static inline void 
FlushAcceptEvents(mtcp_manager_t mtcp)
{
	STAT_COUNT(mtcp->runstat.rounds_accept);

	pthread_mutex_lock(&mtcp->listener->accept_lock);
	if (!StreamQueueIsEmpty(mtcp->listener->acceptq)) {
		pthread_cond_signal(&mtcp->listener->accept_cond);
	}
	pthread_mutex_unlock(&mtcp->listener->accept_lock);
}
/*----------------------------------------------------------------------------*/
static inline void
FlushWriteEvents(mtcp_manager_t mtcp, int thresh)
{
	tcp_stream *walk;
	tcp_stream *next, *last;
	int cnt;

	STAT_COUNT(mtcp->runstat.rounds_write);

	/* Notify available sending buffer (recovered peer window) */
	cnt = 0;
	walk = TAILQ_FIRST(&mtcp->snd_br_list);
	last = TAILQ_LAST(&mtcp->snd_br_list, snd_br_head);
	while (walk) {
		if (++cnt > thresh)
			break;

		next = TAILQ_NEXT(walk, sndvar->snd_br_link);
		TRACE_LOOP("Inside send broadcasting list. cnt: %u\n", cnt);
		TAILQ_REMOVE(&mtcp->snd_br_list, walk, sndvar->snd_br_link);
		mtcp->snd_br_list_cnt--;
		if (walk->on_snd_br_list) {
			TRACE_SNDBUF("Broadcasting available sending buffer!\n");
			if (!(walk->epoll & MTCP_EPOLLOUT)) {
				pthread_cond_signal(&walk->write_cond);
				walk->on_snd_br_list = FALSE;
			}
		}

		if (walk == last)
			break;
		walk = next;
	}
}
/*----------------------------------------------------------------------------*/
static inline void 
FlushReadEvents(mtcp_manager_t mtcp, int thresh)
{	
	tcp_stream *walk;
	tcp_stream *next, *last;
	int cnt;

	STAT_COUNT(mtcp->runstat.rounds_read);

	/* Notify receiving event */
	cnt = 0;
	walk = TAILQ_FIRST(&mtcp->rcv_br_list);
	last = TAILQ_LAST(&mtcp->rcv_br_list, rcv_br_head);
	while (walk) {
		if (++cnt > thresh)
			break;

		next = TAILQ_NEXT(walk, rcvvar->rcv_br_link);
		TRACE_LOOP("Inside recv broadcasting list. cnt: %u\n", cnt);
		TAILQ_REMOVE(&mtcp->rcv_br_list, walk, rcvvar->rcv_br_link);
		mtcp->rcv_br_list_cnt--;
		if (walk->on_rcv_br_list) {
			if (!(walk->epoll & MTCP_EPOLLIN)) {
				TRACE_TEMP("Broadcasting read contition\n");
				pthread_cond_signal(&walk->read_cond);
				walk->on_rcv_br_list = FALSE;
			}
		}
		
		if (walk == last)
			break;
		walk = next;
	}
}
#endif
/*----------------------------------------------------------------------------*/
// 将事件从mtcp队列移到usr队列，并且使用pthread_cond_signal唤醒处于epoll_wait中的线程
static inline void 
FlushEpollEvents(mtcp_manager_t mtcp, uint32_t cur_ts)
{
	struct mtcp_epoll *ep = mtcp->ep;
	struct event_queue *usrq = ep->usr_queue;
	struct event_queue *mtcpq = ep->mtcp_queue;

	pthread_mutex_lock(&ep->epoll_lock);
	// 有待触发的事件
	if (ep->mtcp_queue->num_events > 0) {
		/* while mtcp_queue have events */
		/* and usr_queue is not full */
		while (mtcpq->num_events > 0 && usrq->num_events < usrq->size) {
			/* copy the event from mtcp_queue to usr_queue */
			// 从mtcp事件队列移到user事件队列
			usrq->events[usrq->end++] = mtcpq->events[mtcpq->start++];

			if (usrq->end >= usrq->size)
				usrq->end = 0;
			usrq->num_events++;

			if (mtcpq->start >= mtcpq->size)
				mtcpq->start = 0;
			mtcpq->num_events--;
		}
	}

	/* if there are pending events, wake up user */
	// 通过pthread_cond_signal唤醒epoll_wait的线程
	if (ep->waiting && (ep->usr_queue->num_events > 0 || 
				ep->usr_shadow_queue->num_events > 0)) {
		STAT_COUNT(mtcp->runstat.rounds_epoll);
		TRACE_EPOLL("Broadcasting events. num: %d, cur_ts: %u, prev_ts: %u\n", 
				ep->usr_queue->num_events, cur_ts, mtcp->ts_last_event);
		mtcp->ts_last_event = cur_ts;
		ep->stat.wakes++;
		pthread_cond_signal(&ep->epoll_cond);
	}
	pthread_mutex_unlock(&ep->epoll_lock);
}
/*----------------------------------------------------------------------------*/
static inline void 
HandleApplicationCalls(mtcp_manager_t mtcp, uint32_t cur_ts)
{
	tcp_stream *stream;
	int cnt, max_cnt;
	int handled, delayed;
	int control, send, ack;

	/* connect handling */
	while ((stream = StreamDequeue(mtcp->connectq))) {
		AddtoControlList(mtcp, stream, cur_ts);
	}

	/* send queue handling */
	while ((stream = StreamDequeue(mtcp->sendq))) {
		stream->sndvar->on_sendq = FALSE;
		AddtoSendList(mtcp, stream);
	}
	
	/* ack queue handling */
	while ((stream = StreamDequeue(mtcp->ackq))) {
		stream->sndvar->on_ackq = FALSE;
		EnqueueACK(mtcp, stream, cur_ts, ACK_OPT_AGGREGATE);
	}

	/* close handling */
	handled = delayed = 0;
	control = send = ack = 0;
	while ((stream = StreamDequeue(mtcp->closeq))) {
		struct tcp_send_vars *sndvar = stream->sndvar;
		sndvar->on_closeq = FALSE;
		
		if (sndvar->sndbuf) {
			sndvar->fss = sndvar->sndbuf->head_seq + sndvar->sndbuf->len;
		} else {
			sndvar->fss = stream->snd_nxt;
		}

		if (CONFIG.tcp_timeout > 0)
			RemoveFromTimeoutList(mtcp, stream);

		if (stream->have_reset) {
			handled++;
			if (stream->state != TCP_ST_CLOSED) {
				stream->close_reason = TCP_RESET;
				stream->state = TCP_ST_CLOSED;
				TRACE_STATE("Stream %d: TCP_ST_CLOSED\n", stream->id);
				DestroyTCPStream(mtcp, stream);
			} else {
				TRACE_ERROR("Stream already closed.\n");
			}

		} else if (sndvar->on_control_list) {
			sndvar->on_closeq_int = TRUE;
			StreamInternalEnqueue(mtcp->closeq_int, stream);
			delayed++;
			if (sndvar->on_control_list)
				control++;
			if (sndvar->on_send_list)
				send++;
			if (sndvar->on_ack_list)
				ack++;

		} else if (sndvar->on_send_list || sndvar->on_ack_list) {
			handled++;
			if (stream->state == TCP_ST_ESTABLISHED) {
				stream->state = TCP_ST_FIN_WAIT_1;
				TRACE_STATE("Stream %d: TCP_ST_FIN_WAIT_1\n", stream->id);

			} else if (stream->state == TCP_ST_CLOSE_WAIT) {
				stream->state = TCP_ST_LAST_ACK;
				TRACE_STATE("Stream %d: TCP_ST_LAST_ACK\n", stream->id);
			}
			stream->control_list_waiting = TRUE;

		} else if (stream->state != TCP_ST_CLOSED) {
			handled++;
			if (stream->state == TCP_ST_ESTABLISHED) {
				stream->state = TCP_ST_FIN_WAIT_1;
				TRACE_STATE("Stream %d: TCP_ST_FIN_WAIT_1\n", stream->id);

			} else if (stream->state == TCP_ST_CLOSE_WAIT) {
				stream->state = TCP_ST_LAST_ACK;
				TRACE_STATE("Stream %d: TCP_ST_LAST_ACK\n", stream->id);
			}
			//sndvar->rto = TCP_FIN_RTO;
			//UpdateRetransmissionTimer(mtcp, stream, mtcp->cur_ts);
			AddtoControlList(mtcp, stream, cur_ts);
		} else {
			TRACE_ERROR("Already closed connection!\n");
		}
	}
	TRACE_ROUND("Handling close connections. cnt: %d\n", cnt);

	cnt = 0;
	max_cnt = mtcp->closeq_int->count;
	while (cnt++ < max_cnt) {
		stream = StreamInternalDequeue(mtcp->closeq_int);

		if (stream->sndvar->on_control_list) {
			StreamInternalEnqueue(mtcp->closeq_int, stream);

		} else if (stream->state != TCP_ST_CLOSED) {
			handled++;
			stream->sndvar->on_closeq_int = FALSE;
			if (stream->state == TCP_ST_ESTABLISHED) {
				stream->state = TCP_ST_FIN_WAIT_1;
				TRACE_STATE("Stream %d: TCP_ST_FIN_WAIT_1\n", stream->id);

			} else if (stream->state == TCP_ST_CLOSE_WAIT) {
				stream->state = TCP_ST_LAST_ACK;
				TRACE_STATE("Stream %d: TCP_ST_LAST_ACK\n", stream->id);
			}
			AddtoControlList(mtcp, stream, cur_ts);
		} else {
			stream->sndvar->on_closeq_int = FALSE;
			TRACE_ERROR("Already closed connection!\n");
		}
	}

	/* reset handling */
	while ((stream = StreamDequeue(mtcp->resetq))) {
		stream->sndvar->on_resetq = FALSE;
		
		if (CONFIG.tcp_timeout > 0)
			RemoveFromTimeoutList(mtcp, stream);

		if (stream->have_reset) {
			if (stream->state != TCP_ST_CLOSED) {
				stream->close_reason = TCP_RESET;
				stream->state = TCP_ST_CLOSED;
				TRACE_STATE("Stream %d: TCP_ST_CLOSED\n", stream->id);
				DestroyTCPStream(mtcp, stream);
			} else {
				TRACE_ERROR("Stream already closed.\n");
			}

		} else if (stream->sndvar->on_control_list || 
				stream->sndvar->on_send_list || stream->sndvar->on_ack_list) {
			/* wait until all the queues are flushed */
			stream->sndvar->on_resetq_int = TRUE;
			StreamInternalEnqueue(mtcp->resetq_int, stream);

		} else {
			if (stream->state != TCP_ST_CLOSED) {
				stream->close_reason = TCP_ACTIVE_CLOSE;
				stream->state = TCP_ST_CLOSED;
				TRACE_STATE("Stream %d: TCP_ST_CLOSED\n", stream->id);
				AddtoControlList(mtcp, stream, cur_ts);
			} else {
				TRACE_ERROR("Stream already closed.\n");
			}
		}
	}
	TRACE_ROUND("Handling reset connections. cnt: %d\n", cnt);

	cnt = 0;
	max_cnt = mtcp->resetq_int->count;
	while (cnt++ < max_cnt) {
		stream = StreamInternalDequeue(mtcp->resetq_int);
		
		if (stream->sndvar->on_control_list || 
				stream->sndvar->on_send_list || stream->sndvar->on_ack_list) {
			/* wait until all the queues are flushed */
			StreamInternalEnqueue(mtcp->resetq_int, stream);

		} else {
			stream->sndvar->on_resetq_int = FALSE;

			if (stream->state != TCP_ST_CLOSED) {
				stream->close_reason = TCP_ACTIVE_CLOSE;
				stream->state = TCP_ST_CLOSED;
				TRACE_STATE("Stream %d: TCP_ST_CLOSED\n", stream->id);
				AddtoControlList(mtcp, stream, cur_ts);
			} else {
				TRACE_ERROR("Stream already closed.\n");
			}
		}
	}

	/* destroy streams in destroyq */
	while ((stream = StreamDequeue(mtcp->destroyq))) {
		DestroyTCPStream(mtcp, stream);
	}

	mtcp->wakeup_flag = FALSE;
}
/*----------------------------------------------------------------------------*/
static inline void 
WritePacketsToChunks(mtcp_manager_t mtcp, uint32_t cur_ts)
{
	int thresh = CONFIG.max_concurrency;
	int i;

	/* Set the threshold to CONFIG.max_concurrency to send ACK immediately */
	/* Otherwise, set to appropriate value (e.g. thresh) */
	assert(mtcp->g_sender != NULL);
	if (mtcp->g_sender->control_list_cnt)
		WriteTCPControlList(mtcp, mtcp->g_sender, cur_ts, thresh);
	if (mtcp->g_sender->ack_list_cnt)
		WriteTCPACKList(mtcp, mtcp->g_sender, cur_ts, thresh);
	if (mtcp->g_sender->send_list_cnt)
		WriteTCPDataList(mtcp, mtcp->g_sender, cur_ts, thresh);

	for (i = 0; i < CONFIG.eths_num; i++) {
		assert(mtcp->n_sender[i] != NULL);
		if (mtcp->n_sender[i]->control_list_cnt)
			WriteTCPControlList(mtcp, mtcp->n_sender[i], cur_ts, thresh);
		if (mtcp->n_sender[i]->ack_list_cnt)
			WriteTCPACKList(mtcp, mtcp->n_sender[i], cur_ts, thresh);
		if (mtcp->n_sender[i]->send_list_cnt)
			WriteTCPDataList(mtcp, mtcp->n_sender[i], cur_ts, thresh);
	}
}
/*----------------------------------------------------------------------------*/
#if TESTING
static int
DestroyRemainingFlows(mtcp_manager_t mtcp)
{
	struct hashtable *ht = mtcp->tcp_flow_table;
	tcp_stream *walk;
	int cnt, i;

	cnt = 0;
#if 0
	thread_printf(mtcp, mtcp->log_fp, 
			"CPU %d: Flushing remaining flows.\n", mtcp->ctx->cpu);
#endif
	for (i = 0; i < NUM_BINS; i++) {
		TAILQ_FOREACH(walk, &ht->ht_table[i], rcvvar->he_link) {
#if 0
			thread_printf(mtcp, mtcp->log_fp, 
					"CPU %d: Destroying stream %d\n", mtcp->ctx->cpu, walk->id);
			DumpStream(mtcp, walk);
#endif
			DestroyTCPStream(mtcp, walk);
			cnt++;
		}
	}

	return cnt;
}
#endif
/*----------------------------------------------------------------------------*/
static void 
InterruptApplication(mtcp_manager_t mtcp)
{
	/* interrupt if the mtcp_epoll_wait() is waiting */
	if (mtcp->ep) {
		pthread_mutex_lock(&mtcp->ep->epoll_lock);
		if (mtcp->ep->waiting) {
			pthread_cond_signal(&mtcp->ep->epoll_cond);
		}
		pthread_mutex_unlock(&mtcp->ep->epoll_lock);
	}
	/* interrupt if the accept() is waiting */
	if (mtcp->listener) {
		if (mtcp->listener->socket) {
			pthread_mutex_lock(&mtcp->listener->accept_lock);
			if (!(mtcp->listener->socket->opts & MTCP_NONBLOCK)) {
				pthread_cond_signal(&mtcp->listener->accept_cond);
			}
			pthread_mutex_unlock(&mtcp->listener->accept_lock);
		}
	}
}
/*----------------------------------------------------------------------------*/
// 主循环函数
static void 
RunMainLoop(struct mtcp_thread_context *ctx)
{
	mtcp_manager_t mtcp = ctx->mtcp_manager;
	int i;
	int recv_cnt;
	int rx_inf, tx_inf;
	struct timeval cur_ts = {0};
	uint32_t ts, ts_prev;
	int thresh;

	gettimeofday(&cur_ts, NULL);
	TRACE_DBG("CPU %d: mtcp thread running.\n", ctx->cpu);

	ts = ts_prev = 0;
	// 循环直到完成或者主动退出
	while ((!ctx->done || mtcp->flow_cnt) && !ctx->exit) {
		
		STAT_COUNT(mtcp->runstat.rounds);	// 计数加1
		recv_cnt = 0;
			
		gettimeofday(&cur_ts, NULL);
		ts = TIMEVAL_TO_TS(&cur_ts);
		mtcp->cur_ts = ts;

		// 遍历所有网卡端口
		for (rx_inf = 0; rx_inf < CONFIG.eths_num; rx_inf++) {

			// 从dpdk网卡获取数据包，recv_cnt为收到包的数量
			recv_cnt = mtcp->iom->recv_pkts(ctx, rx_inf);
			STAT_COUNT(mtcp->runstat.rounds_rx_try);

			// 依次处理收到的每一个包
			for (i = 0; i < recv_cnt; i++) {
				uint16_t len;
				uint8_t *pktbuf;
				// 获取指定包的mbuf地址，len为数据包的总长度
				pktbuf = mtcp->iom->get_rptr(mtcp->ctx, rx_inf, i, &len);
				// 处理接收到的数据包，相关事件会被放到epoll管理对象的mtcp事件队列中
				ProcessPacket(mtcp, rx_inf, ts, pktbuf, len);
			}
		}
		STAT_COUNT(mtcp->runstat.rounds_rx);

		/* interaction with application */
		if (mtcp->flow_cnt > 0) {
			
			/* check retransmission timeout and timewait expire */
#if 0
			thresh = (int)mtcp->flow_cnt / (TS_TO_USEC(PER_STREAM_TCHECK));
			assert(thresh >= 0);
			if (thresh == 0)
				thresh = 1;
			if (recv_cnt > 0 && thresh > recv_cnt)
				thresh = recv_cnt;
#endif
			thresh = CONFIG.max_concurrency;

			/* Eunyoung, you may fix this later 
			 * if there is no rcv packet, we will send as much as possible
			 */
			if (thresh == -1)
				thresh = CONFIG.max_concurrency;

			CheckRtmTimeout(mtcp, ts, thresh);
			CheckTimewaitExpire(mtcp, ts, CONFIG.max_concurrency);

			if (CONFIG.tcp_timeout > 0 && ts != ts_prev) {
				CheckConnectionTimeout(mtcp, ts, thresh);
			}
		}

		/* if epoll is in use, flush all the queued events */
		if (mtcp->ep) {
			// 将事件从mtcp队列移到usr队列，并且使用pthread_cond_signal唤醒处于epoll_wait中的线程
			FlushEpollEvents(mtcp, ts);
		}

		if (mtcp->flow_cnt > 0) {
			/* hadnle stream queues  */
			HandleApplicationCalls(mtcp, ts);
		}

		WritePacketsToChunks(mtcp, ts);

		/* send packets from write buffer */
		/* send until tx is available */
		// 将每块网卡端口的发送缓冲区中的内容依次发送出去
		for (tx_inf = 0; tx_inf < CONFIG.eths_num; tx_inf++) {
			mtcp->iom->send_pkts(ctx, tx_inf);
		}

		if (ts != ts_prev) {
			ts_prev = ts;
			if (ctx->cpu == mtcp_master) {
				ARPTimer(mtcp, ts);
				PrintNetworkStats(mtcp, ts);
			}
		}

		mtcp->iom->select(ctx);

		if (ctx->interrupt) {
			InterruptApplication(mtcp);
		}
	}

#if TESTING
	DestroyRemainingFlows(mtcp);
#endif

	TRACE_DBG("MTCP thread %d out of main loop.\n", ctx->cpu);
	/* flush logs */
	flush_log_data(mtcp);
	TRACE_DBG("MTCP thread %d flushed logs.\n", ctx->cpu);
	InterruptApplication(mtcp);
	TRACE_INFO("MTCP thread %d finished.\n", ctx->cpu);
}
/*----------------------------------------------------------------------------*/
struct mtcp_sender *
CreateMTCPSender(int ifidx)
{
	struct mtcp_sender *sender;

	sender = (struct mtcp_sender *)calloc(1, sizeof(struct mtcp_sender));
	if (!sender) {
		return NULL;
	}

	sender->ifidx = ifidx;

	TAILQ_INIT(&sender->control_list);
	TAILQ_INIT(&sender->send_list);
	TAILQ_INIT(&sender->ack_list);

	sender->control_list_cnt = 0;
	sender->send_list_cnt = 0;
	sender->ack_list_cnt = 0;

	return sender;
}
/*----------------------------------------------------------------------------*/
void 
DestroyMTCPSender(struct mtcp_sender *sender)
{
	free(sender);
}
/*----------------------------------------------------------------------------*/
// 初始化mtcp管理对象
static mtcp_manager_t 
InitializeMTCPManager(struct mtcp_thread_context* ctx)
{
	mtcp_manager_t mtcp;
	char log_name[MAX_FILE_NAME];
	int i;

	mtcp = (mtcp_manager_t)calloc(1, sizeof(struct mtcp_manager));
	if (!mtcp) {
		perror("malloc");
		CTRACE_ERROR("Failed to allocate mtcp_manager.\n");
		return NULL;
	}
	g_mtcp[ctx->cpu] = mtcp;

	// 创建tcp流量表
	mtcp->tcp_flow_table = CreateHashtable(HashFlow, EqualFlow);
	if (!mtcp->tcp_flow_table) {
		CTRACE_ERROR("Falied to allocate tcp flow table.\n");
		return NULL;
	}

#ifdef HUGEPAGE
#define	IS_HUGEPAGE 1
#else
#define	IS_HUGEPAGE 0
#endif

	mtcp->flow_pool = MPCreate(sizeof(tcp_stream),
								sizeof(tcp_stream) * CONFIG.max_concurrency, IS_HUGEPAGE);
	if (!mtcp->flow_pool) {
		CTRACE_ERROR("Failed to allocate tcp flow pool.\n");
		return NULL;
	}
	mtcp->rv_pool = MPCreate(sizeof(struct tcp_recv_vars), 
			sizeof(struct tcp_recv_vars) * CONFIG.max_concurrency, IS_HUGEPAGE);
	if (!mtcp->rv_pool) {
		CTRACE_ERROR("Failed to allocate tcp recv variable pool.\n");
		return NULL;
	}
	mtcp->sv_pool = MPCreate(sizeof(struct tcp_send_vars), 
			sizeof(struct tcp_send_vars) * CONFIG.max_concurrency, IS_HUGEPAGE);
	if (!mtcp->sv_pool) {
		CTRACE_ERROR("Failed to allocate tcp send variable pool.\n");
		return NULL;
	}

	// 创建tcp发送管理对象
	mtcp->rbm_snd = SBManagerCreate(CONFIG.sndbuf_size, CONFIG.max_num_buffers);
	if (!mtcp->rbm_snd) {
		CTRACE_ERROR("Failed to create send ring buffer.\n");
		return NULL;
	}
	// 创建tcp接收管理对象
	mtcp->rbm_rcv = RBManagerCreate(CONFIG.rcvbuf_size, CONFIG.max_num_buffers);
	if (!mtcp->rbm_rcv) {
		CTRACE_ERROR("Failed to create recv ring buffer.\n");
		return NULL;
	}

	// 创建socket对象map，初始化后插入空闲队列
	mtcp->smap = (socket_map_t)calloc(CONFIG.max_concurrency, sizeof(struct socket_map));
	if (!mtcp->smap) {
		perror("calloc");
		CTRACE_ERROR("Failed to allocate memory for stream map.\n");
		return NULL;
	}
	TAILQ_INIT(&mtcp->free_smap);
	// 初始化空闲socket对象队列
	for (i = 0; i < CONFIG.max_concurrency; i++) {
		mtcp->smap[i].id = i;
		mtcp->smap[i].socktype = MTCP_SOCK_UNUSED;
		memset(&mtcp->smap[i].saddr, 0, sizeof(struct sockaddr_in));
		mtcp->smap[i].stream = NULL;
		TAILQ_INSERT_TAIL(&mtcp->free_smap, &mtcp->smap[i], free_smap_link);
	}

	mtcp->ctx = ctx;
	mtcp->ep = NULL;

	// 日志对象初始化
	snprintf(log_name, MAX_FILE_NAME, LOG_FILE_NAME"_%d", ctx->cpu);
	mtcp->log_fp = fopen(log_name, "w");
	if (!mtcp->log_fp) {
		perror("fopen");
		CTRACE_ERROR("Failed to create file for logging.\n");
		return NULL;
	}
	mtcp->sp_fd = g_logctx[ctx->cpu]->pair_sp_fd;
	mtcp->logger = g_logctx[ctx->cpu];

	// 创建tcp连接队列
	mtcp->connectq = CreateStreamQueue(BACKLOG_SIZE);
	if (!mtcp->connectq) {
		CTRACE_ERROR("Failed to create connect queue.\n");
		return NULL;
	}
	// 创建tcp发送队列
	mtcp->sendq = CreateStreamQueue(CONFIG.max_concurrency);
	if (!mtcp->sendq) {
		CTRACE_ERROR("Failed to create send queue.\n");
		return NULL;
	}
	// 创建ack队列
	mtcp->ackq = CreateStreamQueue(CONFIG.max_concurrency);
	if (!mtcp->ackq) {
		CTRACE_ERROR("Failed to create ack queue.\n");
		return NULL;
	}
	mtcp->closeq = CreateStreamQueue(CONFIG.max_concurrency);
	if (!mtcp->closeq) {
		CTRACE_ERROR("Failed to create close queue.\n");
		return NULL;
	}
	mtcp->closeq_int = CreateInternalStreamQueue(CONFIG.max_concurrency);
	if (!mtcp->closeq_int) {
		CTRACE_ERROR("Failed to create close queue.\n");
		return NULL;
	}
	mtcp->resetq = CreateStreamQueue(CONFIG.max_concurrency);
	if (!mtcp->resetq) {
		CTRACE_ERROR("Failed to create reset queue.\n");
		return NULL;
	}
	mtcp->resetq_int = CreateInternalStreamQueue(CONFIG.max_concurrency);
	if (!mtcp->resetq_int) {
		CTRACE_ERROR("Failed to create reset queue.\n");
		return NULL;
	}
	mtcp->destroyq = CreateStreamQueue(CONFIG.max_concurrency);
	if (!mtcp->destroyq) {
		CTRACE_ERROR("Failed to create destroy queue.\n");
		return NULL;
	}

	mtcp->g_sender = CreateMTCPSender(-1);
	if (!mtcp->g_sender) {
		CTRACE_ERROR("Failed to create global sender structure.\n");
		return NULL;
	}
	for (i = 0; i < CONFIG.eths_num; i++) {
		mtcp->n_sender[i] = CreateMTCPSender(i);
		if (!mtcp->n_sender[i]) {
			CTRACE_ERROR("Failed to create per-nic sender structure.\n");
			return NULL;
		}
	}
		
	mtcp->rto_store = InitRTOHashstore();
	TAILQ_INIT(&mtcp->timewait_list);
	TAILQ_INIT(&mtcp->timeout_list);

#if BLOCKING_SUPPORT
	TAILQ_INIT(&mtcp->rcv_br_list);
	TAILQ_INIT(&mtcp->snd_br_list);
#endif

	return mtcp;
}
/*----------------------------------------------------------------------------*/
// mtcp线程函数
static void *
MTCPRunThread(void *arg)
{
	mctx_t mctx = (mctx_t)arg;
	int cpu = mctx->cpu;
	int working;
	struct mtcp_manager *mtcp;
	struct mtcp_thread_context *ctx;

	/* affinitize the thread to this core first */
	// 绑定当前线程到指定id的cpu
	mtcp_core_affinitize(cpu);

	/* memory alloc after core affinitization would use local memory
	   most time */
	// 创建线程环境信息
	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		perror("calloc");
		TRACE_ERROR("Failed to calloc mtcp context.\n");
		exit(-1);
	}
	// 设置当前线程id
	ctx->thread = pthread_self();
	ctx->cpu = cpu;
	// 初始化mtcp管理对象
	mtcp = ctx->mtcp_manager = InitializeMTCPManager(ctx);
	if (!mtcp) {
		TRACE_ERROR("Failed to initialize mtcp manager.\n");
		exit(-1);
	}

	/* assign mtcp context's underlying I/O module */
	mtcp->iom = current_iomodule_func;

	/* I/O initializing */
	// 初始化dpdk模块
	mtcp->iom->init_handle(ctx);

	if (pthread_mutex_init(&ctx->smap_lock, NULL)) {
		perror("pthread_mutex_init of ctx->smap_lock\n");
		exit(-1);
	}

	if (pthread_mutex_init(&ctx->flow_pool_lock, NULL)) {
		perror("pthread_mutex_init of ctx->flow_pool_lock\n");
		exit(-1);
	}
	
	if (pthread_mutex_init(&ctx->socket_pool_lock, NULL)) {
		perror("pthread_mutex_init of ctx->socket_pool_lock\n");
		exit(-1);
	}

	SQ_LOCK_INIT(&ctx->connect_lock, "ctx->connect_lock", exit(-1));
	SQ_LOCK_INIT(&ctx->close_lock, "ctx->close_lock", exit(-1));
	SQ_LOCK_INIT(&ctx->reset_lock, "ctx->reset_lock", exit(-1));
	SQ_LOCK_INIT(&ctx->sendq_lock, "ctx->sendq_lock", exit(-1));
	SQ_LOCK_INIT(&ctx->ackq_lock, "ctx->ackq_lock", exit(-1));
	SQ_LOCK_INIT(&ctx->destroyq_lock, "ctx->destroyq_lock", exit(-1));

	/* remember this context pointer for signal processing */
	g_pctx[cpu] = ctx;
	mlockall(MCL_CURRENT);

	// attach (nic device, queue)
	// 貌似放到mtcp_init做了，这里没什么用
	working = AttachDevice(ctx);
	if (working != 0) {
		perror("attach");
		return NULL;
	}

	TRACE_DBG("CPU %d: initialization finished.\n", cpu);

	fprintf(stderr, "CPU %d: initialization finished.\n", cpu);

	// 表示初始化完成，阻塞在这个信号量的wait操作被唤醒
	sem_post(&g_init_sem[ctx->cpu]);

	/* start the main loop */
	// 启动主循环函数
	RunMainLoop(ctx);
	
	TRACE_DBG("MTCP thread %d finished.\n", ctx->cpu);
	
	return 0;
}
/*----------------------------------------------------------------------------*/
static int MTCPDPDKRunThread(void *arg)
{
	MTCPRunThread(arg);
	return 0;
}
/*----------------------------------------------------------------------------*/
// 创建指定cpu核上的上下文环境
mctx_t 
mtcp_create_context(int cpu)
{
	mctx_t mctx;
	int ret;

	if (cpu >=  CONFIG.num_cores) {
		TRACE_ERROR("Failed initialize new mtcp context. "
					"Requested cpu id %d exceed the number of cores %d configured to use.\n",
					cpu, CONFIG.num_cores);
		return NULL;
	}

	// 初始化信号量
	// 这里相当于是一个用于等待某个线程结束的条件变量，当另一处POST时，WAIT操作被唤醒，可以结束
	ret = sem_init(&g_init_sem[cpu], 0, 0);
	if (ret) {
		TRACE_ERROR("Failed initialize init_sem.\n");
		return NULL;
	}

	mctx = (mctx_t)calloc(1, sizeof(struct mtcp_context));
	if (!mctx) {
		TRACE_ERROR("Failed to allocate memory for mtcp_context.\n");
		return NULL;
	}
	mctx->cpu = cpu;

	/* initialize logger */
	g_logctx[cpu] = (struct log_thread_context *)
			calloc(1, sizeof(struct log_thread_context));
	if (!g_logctx[cpu]) {
		perror("malloc");
		TRACE_ERROR("Failed to allocate memory for log thread context.\n");
		return NULL;
	}
	// 初始化日志线程信息
	InitLogThreadContext(g_logctx[cpu], cpu);
	if (pthread_create(&log_thread[cpu], 
				NULL, ThreadLogMain, (void *)g_logctx[cpu])) {
		perror("pthread_create");
		TRACE_ERROR("Failed to create log thread\n");
		return NULL;
	}

	/* Wake up mTCP threads (wake up I/O threads) */
	if (current_iomodule_func == &dpdk_module_func) {
#ifndef DISABLE_DPDK
		int master;
		// 获取管理lcore的master-id
		master = rte_get_master_lcore();
		// 如果当前cpu是管理lcore
		if (master == cpu) {
			lcore_config[master].ret = 0;
			lcore_config[master].state = FINISHED;
			if (pthread_create(&g_thread[cpu], 
					   NULL, MTCPRunThread, (void *)mctx) != 0) {
				TRACE_ERROR("pthread_create of mtcp thread failed!\n");
				return NULL;
			}
		} else
			rte_eal_remote_launch(MTCPDPDKRunThread, mctx, cpu);
#endif /* !DISABLE_DPDK */
	} else {
		if (pthread_create(&g_thread[cpu], 
				   NULL, MTCPRunThread, (void *)mctx) != 0) {
			TRACE_ERROR("pthread_create of mtcp thread failed!\n");
			return NULL;
		}
	}

	// 等待线程创建成功后这个就可以退出了
	sem_wait(&g_init_sem[cpu]);
	sem_destroy(&g_init_sem[cpu]);

	running[cpu] = TRUE;

	// 如果还没有master-cpu，则指定当前cpu为master-cpu
	if (mtcp_master < 0) {
		mtcp_master = cpu;
		TRACE_INFO("CPU %d is now the master thread.\n", mtcp_master);
	}

	return mctx;
}
/*----------------------------------------------------------------------------*/
void 
mtcp_destroy_context(mctx_t mctx)
{
	struct mtcp_thread_context *ctx = g_pctx[mctx->cpu];
	struct mtcp_manager *mtcp = ctx->mtcp_manager;
	struct log_thread_context *log_ctx = mtcp->logger;
	int ret, i;

	TRACE_DBG("CPU %d: mtcp_destroy_context()\n", mctx->cpu);

	/* close all stream sockets that are still open */
	if (!ctx->exit) {
		for (i = 0; i < CONFIG.max_concurrency; i++) {
			if (mtcp->smap[i].socktype == MTCP_SOCK_STREAM) {
				TRACE_DBG("Closing remaining socket %d (%s)\n", 
						i, TCPStateToString(mtcp->smap[i].stream));
				DumpStream(mtcp, mtcp->smap[i].stream);
				mtcp_close(mctx, i);
			}
		}
	}

	ctx->done = 1;

	//pthread_kill(g_thread[mctx->cpu], SIGINT);
	/* XXX - dpdk logic changes */
	if (current_iomodule_func == &dpdk_module_func) {
#ifndef DISABLE_DPDK
		int master = rte_get_master_lcore();
		if (master == mctx->cpu)
			pthread_join(g_thread[mctx->cpu], NULL);
		else
			rte_eal_wait_lcore(mctx->cpu);
#endif /* !DISABLE_DPDK */
	} else
		pthread_join(g_thread[mctx->cpu], NULL);

	TRACE_INFO("MTCP thread %d joined.\n", mctx->cpu);
	running[mctx->cpu] = FALSE;

	if (mtcp_master == mctx->cpu) {
		for (i = 0; i < num_cpus; i++) {
			if (i != mctx->cpu && running[i]) {
				mtcp_master = i;
				break;
			}
		}
	}

	log_ctx->done = 1;
	ret = write(log_ctx->pair_sp_fd, "F", 1);
	assert(ret == 1);
	UNUSED(ret);
	pthread_join(log_thread[ctx->cpu], NULL);
	fclose(mtcp->log_fp);
	TRACE_LOG("Log thread %d joined.\n", mctx->cpu);

	if (mtcp->connectq) {
		DestroyStreamQueue(mtcp->connectq);
		mtcp->connectq = NULL;
	}
	if (mtcp->sendq) {
		DestroyStreamQueue(mtcp->sendq);
		mtcp->sendq = NULL;
	}
	if (mtcp->ackq) {
		DestroyStreamQueue(mtcp->ackq);
		mtcp->ackq = NULL;
	}
	if (mtcp->closeq) {
		DestroyStreamQueue(mtcp->closeq);
		mtcp->closeq = NULL;
	}
	if (mtcp->closeq_int) {
		DestroyInternalStreamQueue(mtcp->closeq_int);
		mtcp->closeq_int = NULL;
	}
	if (mtcp->resetq) {
		DestroyStreamQueue(mtcp->resetq);
		mtcp->resetq = NULL;
	}
	if (mtcp->resetq_int) {
		DestroyInternalStreamQueue(mtcp->resetq_int);
		mtcp->resetq_int = NULL;
	}
	if (mtcp->destroyq) {
		DestroyStreamQueue(mtcp->destroyq);
		mtcp->destroyq = NULL;
	}

	DestroyMTCPSender(mtcp->g_sender);
	for (i = 0; i < CONFIG.eths_num; i++) {
		DestroyMTCPSender(mtcp->n_sender[i]);
	}

	MPDestroy(mtcp->rv_pool);
	MPDestroy(mtcp->sv_pool);
	MPDestroy(mtcp->flow_pool);
	
	if (mtcp->ap) {
		DestroyAddressPool(mtcp->ap);
	}

	SQ_LOCK_DESTROY(&ctx->connect_lock);
	SQ_LOCK_DESTROY(&ctx->close_lock);
	SQ_LOCK_DESTROY(&ctx->reset_lock);
	SQ_LOCK_DESTROY(&ctx->sendq_lock);
	SQ_LOCK_DESTROY(&ctx->ackq_lock);
	SQ_LOCK_DESTROY(&ctx->destroyq_lock);

	//TRACE_INFO("MTCP thread %d destroyed.\n", mctx->cpu);
	mtcp->iom->destroy_handle(ctx);
	free(ctx);
	free(mctx);
}
/*----------------------------------------------------------------------------*/
mtcp_sighandler_t 
mtcp_register_signal(int signum, mtcp_sighandler_t handler)
{
	mtcp_sighandler_t prev;

	if (signum == SIGINT) {
		prev = app_signal_handler;
		app_signal_handler = handler;
	} else {
		if ((prev = signal(signum, handler)) == SIG_ERR) {
			perror("signal");
			return SIG_ERR;
		}
	}

	return prev;
}
/*----------------------------------------------------------------------------*/
int 
mtcp_getconf(struct mtcp_conf *conf)
{
	if (!conf)
		return -1;

	conf->num_cores = CONFIG.num_cores;
	conf->max_concurrency = CONFIG.max_concurrency;

	conf->max_num_buffers = CONFIG.max_num_buffers;
	conf->rcvbuf_size = CONFIG.rcvbuf_size;
	conf->sndbuf_size = CONFIG.sndbuf_size;

	conf->tcp_timewait = CONFIG.tcp_timewait;
	conf->tcp_timeout = CONFIG.tcp_timeout;

	return 0;
}
/*----------------------------------------------------------------------------*/
int 
mtcp_setconf(const struct mtcp_conf *conf)
{
	if (!conf)
		return -1;

	CONFIG.num_cores = conf->num_cores;
	CONFIG.max_concurrency = conf->max_concurrency;

	CONFIG.max_num_buffers = conf->max_num_buffers;
	CONFIG.rcvbuf_size = conf->rcvbuf_size;
	CONFIG.sndbuf_size = conf->sndbuf_size;

	CONFIG.tcp_timewait = conf->tcp_timewait;
	CONFIG.tcp_timeout = conf->tcp_timeout;

	TRACE_CONFIG("Configuration updated by mtcp_setconf().\n");
	PrintConfiguration();

	return 0;
}
/*----------------------------------------------------------------------------*/
int
mtcp_init(char *config_file)
{
	int i;
	int ret;

	if (geteuid()) {
		TRACE_CONFIG("[CAUTION] Run as root if mlock is necessary.\n");
	}

	/* getting cpu and NIC */
	num_cpus = GetNumCPUs();
	assert(num_cpus >= 1);

	if (num_cpus > MAX_CPUS) {
		TRACE_ERROR("You cannot run mTCP with more than %d cores due "
			    "to NIC hardware queues restriction. Please disable "
			    "the last %d cores in your system\n",
			    MAX_CPUS, num_cpus - MAX_CPUS);
		exit(EXIT_FAILURE);
	}

	// 初始化全局参数
	for (i = 0; i < num_cpus; i++) {
		g_mtcp[i] = NULL;
		running[i] = FALSE;
		sigint_cnt[i] = 0;
	}

	// 读取配置文件
	ret = LoadConfiguration(config_file);
	if (ret) {
		TRACE_CONFIG("Error occured while loading configuration.\n");
		return -1;
	}
	// 打印读取到的配置信息
	PrintConfiguration();

	/* TODO: this should be fixed */
	// 创建地址池
	ap = CreateAddressPool(CONFIG.eths[0].ip_addr, 1);
	if (!ap) {
		TRACE_CONFIG("Error occured while creating address pool.\n");
		return -1;
	}
	// 打印网卡端口信息
	PrintInterfaceInfo();

	// 设置路由表信息
	ret = SetRoutingTable();
	if (ret) {
		TRACE_CONFIG("Error occured while loading routing table.\n");
		return -1;
	}
	// 打印路由信息
	PrintRoutingTable();

	LoadARPTable();
	PrintARPTable();

	if (signal(SIGUSR1, HandleSignal) == SIG_ERR) {
		perror("signal, SIGUSR1");
		return -1;
	}
	if (signal(SIGINT, HandleSignal) == SIG_ERR) {
		perror("signal, SIGINT");
		return -1;
	}
	app_signal_handler = NULL;

	/* load system-wide io module specs */
	// 加载dpdk模块
	current_iomodule_func->load_module();

	return 0;
}
/*----------------------------------------------------------------------------*/
void 
mtcp_destroy()
{
	int i;

	/* wait until all threads are closed */
	for (i = 0; i < num_cpus; i++) {
		if (running[i]) {
			pthread_join(g_thread[i], NULL);
		}
	}

	DestroyAddressPool(ap);

	TRACE_INFO("All MTCP threads are joined.\n");
}
/*----------------------------------------------------------------------------*/
