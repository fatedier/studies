
#ifndef PROCESS_CTRL_H
#define PROCESS_CTRL_H

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>

#ifdef __cplusplus
extern "C" {
#endif

/* 加载配置文件到内存中并且获取配置文件中的"base_path"字段值 */
int get_base_path_from_conf_file(const char *filename, char *base_path,
	const int path_size);

/* 从文件中获取pid值 */
int get_pid_from_file(const char *pidFilename, pid_t *pid);

/* 将守护进程的进程号写入到pid文件中 */
int write_to_pid_file(const char *pidFilename);

/* 删除pid_file */
int delete_pid_file(const char *pidFilename);

/* 停止pid文件中所写指定进程*/
int process_stop(const char *pidFilename);

/* 
 * 重启pid文件中所写指定进程
 * 其实就是关闭，因为main函数中后面会根据bool stop的值来判断是否再次start
 */
int process_restart(const char *pidFilename);

int process_exist(const char *pidFilename);

/* 处理启动参数，start | restart | stop */
int process_action(const char *pidFilename, const char *action, bool *stop);

#ifdef __cplusplus
}
#endif

#endif

