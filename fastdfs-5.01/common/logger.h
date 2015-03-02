/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//logger.h
#ifndef LOGGER_H
#define LOGGER_H

#include <syslog.h>
#include <sys/time.h>
#include "common_define.h"

#ifdef __cplusplus
extern "C" {
#endif

#define LOG_TIME_PRECISION_SECOND	's'  //second
#define LOG_TIME_PRECISION_MSECOND	'm'  //millisecond
#define LOG_TIME_PRECISION_USSECOND	'u'  //microsecond

typedef struct log_context /* 日志结构体 */
{
	/* log level value please see: sys/syslog.h
  	   default value is LOG_INFO */
	int log_level;

	/* default value is STDERR_FILENO */
	int log_fd;

	/* cache buffer */
	char *log_buff;

	/* string end in the cache buffer for next sprintf */
	char *pcurrent_buff;

	/* mutext lock */
	pthread_mutex_t log_thread_lock;

	/*
	 * rotate the log when the log file exceeds this parameter
	 * rotate_size > 0 means need rotate log by log file size
	 * 达到指定大小后轮转
	 */
	int64_t rotate_size;

	/* log file current size */
	int64_t current_size;

	/* 
	 * if write to buffer firstly, then sync to disk.
	 * default value is false (no cache) 
	 * 是否启用日志缓存
	 */
	bool log_to_cache;

	/* if rotate the access log (是否立即轮转日志)*/
	bool rotate_immediately;

	/* time precision */
	char time_precision;

	/* save the log filename */
	char log_filename[MAX_PATH_SIZE];
} LogContext;	/* 全局日志信息 */

extern LogContext g_log_context;

/** init function using global log context
 *  return: 0 for success, != 0 fail
*/
int log_init();

/* 创建日志及文件，将文件名及文件大小保存在全局变量中 */
#define log_set_prefix(base_path, filename_prefix) \
	log_set_prefix_ex(&g_log_context, base_path, filename_prefix)

#define log_set_filename(log_filename) \
	log_set_filename_ex(&g_log_context, log_filename)

/* 设置是否使用日志缓存 */
#define log_set_cache(bLogCache)  log_set_cache_ex(&g_log_context, bLogCache)

/* 销毁日志相关资源 */
#define log_destroy()  log_destroy_ex(&g_log_context)

/** init function, use stderr for output by default
 *  parameters:
 *           pContext: the log context
 *  return: 0 for success, != 0 fail
*/
int log_init_ex(LogContext *pContext);

/** set log filename prefix, such as "tracker", the log filename will be 
 *  ${base_path}/logs/tracker.log
 *  parameters:
 *           pContext: the log context
 *           base_path: base path
 *           filename_prefix: log filename prefix
 *  return: 0 for success, != 0 fail
*/
int log_set_prefix_ex(LogContext *pContext, const char *base_path, \
		const char *filename_prefix);

/** set log filename
 *  parameters:
 *           pContext: the log context
 *           log_filename: log filename
 *  return: 0 for success, != 0 fail
*/
int log_set_filename_ex(LogContext *pContext, const char *log_filename);

/** set if use log cache
 *  parameters:
 *           pContext: the log context
 *           bLogCache: true for cache in buffer, false directly write to disk
 *  return: none
*/
/* 设置是否使用日志缓存 */
void log_set_cache_ex(LogContext *pContext, const bool bLogCache);

/** set time precision
 *  parameters:
 *           pContext: the log context
 *           time_precision: the time precision
 *  return: none
*/
void log_set_time_precision(LogContext *pContext, const int time_precision);

/** destroy function
 *  parameters:
 *           pContext: the log context
 *           bLogCache: true for cache in buffer, false directly write to disk
 *  return: none
*/
/* 销毁日志相关资源 */
void log_destroy_ex(LogContext *pContext);

/** log to file
 *  parameters:
 *           pContext: the log context
 *           priority: unix priority
 *           format: printf format
 *           ...:    arguments for printf format
 *  return: none
*/
void log_it_ex(LogContext *pContext, const int priority, \
		const char *format, ...);

/** log to file
 *  parameters:
 *           pContext: the log context
 *           priority: unix priority
 *           text: text string to log
 *           text_len: text string length (bytes)
 *  return: none
*/
void log_it_ex1(LogContext *pContext, const int priority, \
		const char *text, const int text_len);

/** sync log buffer to log file
 *  parameters:
 *           args: should be (LogContext *)
 *  return: error no, 0 for success, != 0 fail
*/
/* 将日志缓存同步到文件中 */
int log_sync_func(void *args);

/** set rotate flag to true
 *  parameters:
 *           args: should be (LogContext *)
 *  return: error no, 0 for success, != 0 fail
*/
/* 将轮转标志rotate_immediately置为true，之后定时检测标志并进行轮转 */
int log_notify_rotate(void *args);

void logEmergEx(LogContext *pContext, const char *format, ...);
void logCritEx(LogContext *pContext, const char *format, ...);
void logAlertEx(LogContext *pContext, const char *format, ...);
void logErrorEx(LogContext *pContext, const char *format, ...);
void logWarningEx(LogContext *pContext, const char *format, ...);
void logNoticeEx(LogContext *pContext, const char *format, ...);
void logInfoEx(LogContext *pContext, const char *format, ...);
void logDebugEx(LogContext *pContext, const char *format, ...);
void logAccess(LogContext *pContext, struct timeval *tvStart, \
		const char *format, ...);

//#define LOG_FORMAT_CHECK

#ifdef LOG_FORMAT_CHECK  /*only for format check*/

#define logEmerg   printf
#define logCrit    printf
#define logAlert   printf
#define logError   printf
#define logWarning printf
#define logNotice  printf
#define logInfo    printf
#define logDebug   printf

#else

/* following functions use global log context: g_log_context */
void logEmerg(const char *format, ...);
void logCrit(const char *format, ...);
void logAlert(const char *format, ...);
void logError(const char *format, ...);
void logWarning(const char *format, ...);
void logNotice(const char *format, ...);
void logInfo(const char *format, ...);
void logDebug(const char *format, ...);

#endif

#ifdef __cplusplus
}
#endif

#endif

