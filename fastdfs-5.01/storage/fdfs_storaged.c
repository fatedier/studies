/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include "shared_func.h"
#include "pthread_func.h"
#include "process_ctrl.h"
#include "logger.h"
#include "fdfs_global.h"
#include "ini_file_reader.h"
#include "sockopt.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "tracker_client_thread.h"
#include "storage_global.h"
#include "storage_func.h"
#include "storage_sync.h"
#include "storage_service.h"
#include "sched_thread.h"
#include "storage_dio.h"
#include "trunk_mem.h"
#include "trunk_sync.h"
#include "trunk_shared.h"

#ifdef WITH_HTTPD
#include "storage_httpd.h"
#endif

#if defined(DEBUG_FLAG) 

/*
#if defined(OS_LINUX)
#include "linux_stack_trace.h"
static bool bSegmentFault = false;
#endif
*/

#include "storage_dump.h"
#endif

static bool bTerminateFlag = false;
static bool bAcceptEndFlag = false;

static void sigQuitHandler(int sig);
static void sigHupHandler(int sig);
static void sigUsrHandler(int sig);
static void sigAlarmHandler(int sig);

#if defined(DEBUG_FLAG)

/*
#if defined(OS_LINUX)
static void sigSegvHandler(int signum, siginfo_t *info, void *ptr);
#endif
*/

static void sigDumpHandler(int sig);
#endif

#define SCHEDULE_ENTRIES_MAX_COUNT 7		/* 初始的定时任务个数 */

static void usage(const char *program)
{
	fprintf(stderr, "Usage: %s <config_file> [start | stop | restart]\n",
		program);
}

int main(int argc, char *argv[])
{
	char *conf_filename;		/* 配置文件路径 */
	int result;
	int sock;
	int wait_count;
	pthread_t schedule_tid;	/* 定时任务线程tid */
	struct sigaction act;
	ScheduleEntry scheduleEntries[SCHEDULE_ENTRIES_MAX_COUNT];	/* 定时任务链表对应的数组 */
	ScheduleArray scheduleArray;			/* 定时任务链表 */
	char pidFilename[MAX_PATH_SIZE];		/* 进程号文件 */
	bool stop;							/* 是否停止storage守护进程的标志 */

	if (argc < 2)
	{
		usage(argv[0]);
		return 1;
	}

	g_current_time = time(NULL);
	g_up_time = g_current_time;		/* 设置进程启动时间 */

	/* 初始化日志对象 */
	log_init();

	/* 初始化base64相关数据等 */
	trunk_shared_init();

	conf_filename = argv[1];	/* storage server配置文件名 */
	/* 加载storage配置文件到内存中并且获取配置文件中的"base_path"字段值 */
	if ((result=get_base_path_from_conf_file(conf_filename,
		g_fdfs_base_path, sizeof(g_fdfs_base_path))) != 0)
	{
		log_destroy();
		return result;
	}

	/* 设置pid文件路径 */
	snprintf(pidFilename, sizeof(pidFilename),
		"%s/data/fdfs_storaged.pid", g_fdfs_base_path);
	/* 处理启动参数，start | restart | stop */
	if ((result=process_action(pidFilename, argv[2], &stop)) != 0)
	{
		if (result == EINVAL)
		{
			usage(argv[0]);
		}
		log_destroy();
		return result;
	}
	/* 
	 * 如果启动参数是stop，这里退出
	 * 如果是start或者是restart，后面会根据配置文件参数启动守护进程
	 */
	if (stop)
	{
		log_destroy();
		return 0;
	}

#if defined(DEBUG_FLAG) && defined(OS_LINUX)
	if (getExeAbsoluteFilename(argv[0], g_exe_name, \
		sizeof(g_exe_name)) == NULL)
	{
		logCrit("exit abnormally!\n");
		log_destroy();
		return errno != 0 ? errno : ENOENT;
	}
#endif

	memset(g_bind_addr, 0, sizeof(g_bind_addr));
	/* 初始化的相关工作，加载配置文件，创建存储目录，恢复磁盘数据等 */
	if ((result=storage_func_init(conf_filename, \
			g_bind_addr, sizeof(g_bind_addr))) != 0)
	{
		logCrit("exit abnormally!\n");
		log_destroy();
		return result;
	}

	/* 创建socket 服务端 */
	sock = socketServer(g_bind_addr, g_server_port, &result);
	if (sock < 0)
	{
		logCrit("exit abnormally!\n");
		log_destroy();
		return result;
	}

	/* 设置socket服务端相关参数 */
	if ((result=tcpsetserveropt(sock, g_fdfs_network_timeout)) != 0)
	{
		logCrit("exit abnormally!\n");
		log_destroy();
		return result;
	}

	/* 创建守护进程 */
	daemon_init(true);
	umask(0);

	/* 将storage守护进程的进程号写入到pid文件中 */
	if ((result=write_to_pid_file(pidFilename)) != 0)
	{
		log_destroy();
		return result;
	}

	/* 将log文件描述符dup到标准输出或错误输出*/
	if (dup2(g_log_context.log_fd, STDOUT_FILENO) < 0 || \
		dup2(g_log_context.log_fd, STDERR_FILENO) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call dup2 fail, errno: %d, error info: %s, " \
			"program exit!", __LINE__, errno, STRERROR(errno));
		g_continue_flag = false;
		return errno;
	}

	/* storage同步文件相关的初始化工作，打开相应的binlog_file */
	if ((result=storage_sync_init()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"storage_sync_init fail, program exit!", __LINE__);
		g_continue_flag = false;
		return result;
	}

	/* report相关资源初始化工作 */
	if ((result=tracker_report_init()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"tracker_report_init fail, program exit!", __LINE__);
		g_continue_flag = false;
		return result;
	}

	/* storage service的初始化工作，创建处理具体业务的工作线程 */
	if ((result=storage_service_init()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"storage_service_init fail, program exit!", __LINE__);
		g_continue_flag = false;
		return result;
	}

	/* 根据当前时间设置随机数种子 */
	if ((result=set_rand_seed()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"set_rand_seed fail, program exit!", __LINE__);
		g_continue_flag = false;
		return result;
	}

	memset(&act, 0, sizeof(act));
	sigemptyset(&act.sa_mask);

	act.sa_handler = sigUsrHandler;
	if(sigaction(SIGUSR1, &act, NULL) < 0 || \
		sigaction(SIGUSR2, &act, NULL) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		logCrit("exit abnormally!\n");
		return errno;
	}

	/* SIGHUP信号，轮转日志 */
	act.sa_handler = sigHupHandler;
	if(sigaction(SIGHUP, &act, NULL) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		logCrit("exit abnormally!\n");
		return errno;
	}

	/* 忽略SIGPIPE信号 */
	act.sa_handler = SIG_IGN;
	if(sigaction(SIGPIPE, &act, NULL) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		logCrit("exit abnormally!\n");
		return errno;
	}

	/* 指定SIGINT,SIGTERM,SIGQUIT的信号处理函数为sigQuitHandler() */
	act.sa_handler = sigQuitHandler;
	if(sigaction(SIGINT, &act, NULL) < 0 || \
		sigaction(SIGTERM, &act, NULL) < 0 || \
		sigaction(SIGQUIT, &act, NULL) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		logCrit("exit abnormally!\n");
		return errno;
	}

#if defined(DEBUG_FLAG)

/*
#if defined(OS_LINUX)
	memset(&act, 0, sizeof(act));
        act.sa_sigaction = sigSegvHandler;
        act.sa_flags = SA_SIGINFO;
        if (sigaction(SIGSEGV, &act, NULL) < 0 || \
        	sigaction(SIGABRT, &act, NULL) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		logCrit("exit abnormally!\n");
		return errno;
	}
#endif
*/

	memset(&act, 0, sizeof(act));
	sigemptyset(&act.sa_mask);
	/* SIGUSR1和SIGUSR2信号的处理函数，将数据dump到文件中 */
	act.sa_handler = sigDumpHandler;
	if(sigaction(SIGUSR1, &act, NULL) < 0 || \
		sigaction(SIGUSR2, &act, NULL) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		logCrit("exit abnormally!\n");
		return errno;
	}
#endif

#ifdef WITH_HTTPD
	if (!g_http_params.disabled)
	{
		if ((result=storage_httpd_start(g_bind_addr)) != 0)
		{
			logCrit("file: "__FILE__", line: %d, " \
				"storage_httpd_start fail, " \
				"program exit!", __LINE__);
			return result;
		}
	}
#endif

	/* 启动同所有tracker server交互的线程 */
	if ((result=tracker_report_thread_start()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"tracker_report_thread_start fail, " \
			"program exit!", __LINE__);
		g_continue_flag = false;
		storage_func_destroy();
		log_destroy();
		return result;
	}

	scheduleArray.entries = scheduleEntries;

	/* 将日志缓存刷新到文件中 */
	memset(scheduleEntries, 0, sizeof(scheduleEntries));
	scheduleEntries[0].id = 1;
	scheduleEntries[0].time_base.hour = TIME_NONE;
	scheduleEntries[0].time_base.minute = TIME_NONE;
	scheduleEntries[0].interval = g_sync_log_buff_interval;
	scheduleEntries[0].task_func = log_sync_func;
	scheduleEntries[0].func_args = &g_log_context;

	/* 强制将缓冲区中的内容写入binlog_file文件 */
	scheduleEntries[1].id = 2;
	scheduleEntries[1].time_base.hour = TIME_NONE;
	scheduleEntries[1].time_base.minute = TIME_NONE;
	scheduleEntries[1].interval = g_sync_binlog_buff_interval;
	scheduleEntries[1].task_func = fdfs_binlog_sync_func;
	scheduleEntries[1].func_args = NULL;

	/* 将storage的当前状态信息写入磁盘 */
	scheduleEntries[2].id = 3;
	scheduleEntries[2].time_base.hour = TIME_NONE;
	scheduleEntries[2].time_base.minute = TIME_NONE;
	scheduleEntries[2].interval = g_sync_stat_file_interval;
	scheduleEntries[2].task_func = fdfs_stat_file_sync_func;
	scheduleEntries[2].func_args = NULL;

	scheduleArray.count = 3;
	if (g_if_use_trunk_file)
	{
		/* 将trunk_binlog缓冲区中的内容强制刷新到磁盘中 */
		scheduleEntries[scheduleArray.count].id = 4;
		scheduleEntries[scheduleArray.count].time_base.hour = TIME_NONE;
		scheduleEntries[scheduleArray.count].time_base.minute=TIME_NONE;
		scheduleEntries[scheduleArray.count].interval = 1;
		scheduleEntries[scheduleArray.count].task_func = \
					trunk_binlog_sync_func;
		scheduleEntries[scheduleArray.count].func_args = NULL;
		scheduleArray.count++;
	}

	if (g_use_access_log)
	{
		/* 将accesslog日志缓存同步到文件中，此函数作为回调函数在定时任务线程中调用 */
		scheduleEntries[scheduleArray.count].id = 5;
		scheduleEntries[scheduleArray.count].time_base.hour = TIME_NONE;
		scheduleEntries[scheduleArray.count].time_base.minute=TIME_NONE;
		scheduleEntries[scheduleArray.count].interval = \
					g_sync_log_buff_interval;
		scheduleEntries[scheduleArray.count].task_func = log_sync_func;
		scheduleEntries[scheduleArray.count].func_args = \
					&g_access_log_context;
		scheduleArray.count++;

		if (g_rotate_access_log)
		{
			scheduleEntries[scheduleArray.count].id = 6;
			scheduleEntries[scheduleArray.count].time_base = \
					g_access_log_rotate_time;
			scheduleEntries[scheduleArray.count].interval = \
					24 * 3600;
			scheduleEntries[scheduleArray.count].task_func = \
					log_notify_rotate;
			scheduleEntries[scheduleArray.count].func_args = \
					&g_access_log_context;
			scheduleArray.count++;
		}
	}

	if (g_rotate_error_log)
	{
		/* 将轮转标志rotate_immediately置为true，之后定时检测标志并进行轮转 */
		scheduleEntries[scheduleArray.count].id = 7;
		scheduleEntries[scheduleArray.count].time_base = \
				g_error_log_rotate_time;
		scheduleEntries[scheduleArray.count].interval = \
				24 * 3600;
		scheduleEntries[scheduleArray.count].task_func = \
				log_notify_rotate;
		scheduleEntries[scheduleArray.count].func_args = \
				&g_log_context;
		scheduleArray.count++;
	}

	/* 开始运行定时任务调度线程 */
	if ((result=sched_start(&scheduleArray, &schedule_tid, \
		g_thread_stack_size, (bool * volatile)&g_continue_flag)) != 0)
	{
		logCrit("exit abnormally!\n");
		log_destroy();
		return result;
	}

	/* 设置运行的group_name和username */
	if ((result=set_run_by(g_run_by_group, g_run_by_user)) != 0)
	{
		logCrit("exit abnormally!\n");
		log_destroy();
		return result;
	}

	 /* 初始化磁盘IO相关资源，每一个存储路径都创建相关读写线程 */
	if ((result=storage_dio_init()) != 0)
	{
		logCrit("exit abnormally!\n");
		log_destroy();
		return result;
	}

	/* 设置为使用日志缓存 */
	log_set_cache(true);

	bTerminateFlag = false;
	bAcceptEndFlag = false;

	/* accept主循环 */
	storage_accept_loop(sock);
	bAcceptEndFlag = true;

	/* 强制将缓冲区中的内容写入binlog_file文件 */
	fdfs_binlog_sync_func(NULL);  //binlog fsync

	if (g_schedule_flag)
	{
		pthread_kill(schedule_tid, SIGINT);
	}

	/* 结束所有的工作线程 */
	storage_terminate_threads();
	storage_dio_terminate();

	kill_tracker_report_threads();
	kill_storage_sync_threads();

	wait_count = 0;
	while (g_storage_thread_count != 0 || \
		g_dio_thread_count != 0 || \
		g_tracker_reporter_count > 0 || \
		g_schedule_flag)
	{
/*
#if defined(DEBUG_FLAG) && defined(OS_LINUX)
		if (bSegmentFault)
		{
			sleep(5);
			break;
		}
#endif
*/

		usleep(10000);
		if (++wait_count > 6000)
		{
			logWarning("waiting timeout, exit!");
			break;
		}
	}

	/* 销毁相关资源 */
	tracker_report_destroy();
	storage_service_destroy();
	storage_sync_destroy();
	storage_func_destroy();

	if (g_if_use_trunk_file)
	{
		trunk_sync_destroy();
		storage_trunk_destroy();
	}

	logInfo("exit normally.\n");
	log_destroy();
	
	delete_pid_file(pidFilename);
	return 0;
}

/* 如果没有停止，就不断发送报文要求停止 */
static void sigQuitHandler(int sig)
{
	if (!bTerminateFlag)
	{
		set_timer(1, 1, sigAlarmHandler);

		bTerminateFlag = true;
		g_continue_flag = false;

		logCrit("file: "__FILE__", line: %d, " \
			"catch signal %d, program exiting...", \
			__LINE__, sig);
	}
}

static void sigAlarmHandler(int sig)
{
	ConnectionInfo server;

	if (bAcceptEndFlag)
	{
		return;
	}

	logDebug("file: "__FILE__", line: %d, " \
		"signal server to quit...", __LINE__);

	if (*g_bind_addr != '\0')
	{
		strcpy(server.ip_addr, g_bind_addr);
	}
	else
	{
		strcpy(server.ip_addr, "127.0.0.1");
	}
	server.port = g_server_port;
	server.sock = -1;

	if (conn_pool_connect_server(&server, g_fdfs_connect_timeout) != 0)
	{
		return;
	}

	fdfs_quit(&server);
	conn_pool_disconnect_server(&server);

	logDebug("file: "__FILE__", line: %d, " \
		"signal server to quit done", __LINE__);
}

/* SIGHUP信号，轮转日志 */
static void sigHupHandler(int sig)
{
	if (g_rotate_error_log)
	{
		g_log_context.rotate_immediately = true;
	}

	if (g_rotate_access_log)
	{
		g_access_log_context.rotate_immediately = true;
	}

	logInfo("file: "__FILE__", line: %d, " \
		"catch signal %d, rotate log", __LINE__, sig);
}

static void sigUsrHandler(int sig)
{
	logInfo("file: "__FILE__", line: %d, " \
		"catch signal %d, ignore it", __LINE__, sig);
}

#if defined(DEBUG_FLAG)

/*
#if defined(OS_LINUX)
static void sigSegvHandler(int signum, siginfo_t *info, void *ptr)
{
	bSegmentFault = true;

	if (!bTerminateFlag)
	{
		set_timer(1, 1, sigAlarmHandler);

		bTerminateFlag = true;
		g_continue_flag = false;

		logCrit("file: "__FILE__", line: %d, " \
			"catch signal %d, program exiting...", \
			__LINE__, signum);
	
		signal_stack_trace_print(signum, info, ptr);
	}
}
#endif
*/

/* SIGUSR1和SIGUSR2信号的处理函数，将数据dump到文件中 */
static void sigDumpHandler(int sig)
{
	static bool bDumpFlag = false;
	char filename[MAX_PATH_SIZE];

	if (bDumpFlag)
	{
		return;
	}

	bDumpFlag = true;

	snprintf(filename, sizeof(filename), 
		"%s/logs/storage_dump.log", g_fdfs_base_path);
	fdfs_dump_storage_global_vars_to_file(filename);

	bDumpFlag = false;
}
#endif

