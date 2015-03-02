#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "shared_func.h"
#include "logger.h"
#include "process_ctrl.h"

/* 从文件中获取pid值 */
int get_pid_from_file(const char *pidFilename, pid_t *pid)
{
  char buff[32];
  int64_t file_size;
  int result;

  if (access(pidFilename, F_OK) != 0) {
    return errno != 0 ? errno : EPERM;
  }

  file_size = sizeof(buff) - 1;
  if ((result=getFileContentEx(pidFilename, buff, 0, &file_size)) != 0) {
    return result;
  }

  *(buff + file_size) = '\0';
  *pid = strtol(buff, NULL, 10);
  if (*pid == 0) {
    return EINVAL;
  }

  return 0;
}

/* 将守护进程的进程号写入到pid文件中 */
int write_to_pid_file(const char *pidFilename)
{
  char buff[32];
  int len;

  len = sprintf(buff, "%d", (int)getpid());
  return writeToFile(pidFilename, buff, len);
}

/* 删除pid_file */
int delete_pid_file(const char *pidFilename)
{
  if (unlink(pidFilename) == 0) {
    return 0;
  }
  else {
    return errno != 0 ? errno : ENOENT;
  }
}

/* 根据pid文件中内容停止指定进程 */
static int do_stop(const char *pidFilename, const bool bShowError, pid_t *pid)
{
  int result;

  /* 从文件中获取守护进程pid值 */
  if ((result=get_pid_from_file(pidFilename, pid)) != 0) {
    if (bShowError) {
      if (result == ENOENT) {
        fprintf(stderr, "pid file: %s not exist!\n", pidFilename);
      }
      else {
        fprintf(stderr, "get pid from file: %s fail, " \
            "errno: %d, error info: %s\n",
            pidFilename, result, strerror(result));
      }
    }

    return result;
  }

	/* 使用SIGTERM信号关闭指定pid的进程 */
  if (kill(*pid, SIGTERM) == 0) {
    return 0;
  }
  else {
    result = errno != 0 ? errno : EPERM;
    if (bShowError || result != ESRCH) {
      fprintf(stderr, "kill pid: %d fail, errno: %d, error info: %s\n",
          (int)*pid, result, strerror(result));
    }
    return result;
  }
}

/* 停止pid文件中所写指定进程*/
int process_stop(const char *pidFilename)
{
  pid_t pid;
  int result;

  /* 根据pid文件中内容停止指定进程 */
  result = do_stop(pidFilename, true, &pid);
  if (result != 0) {
    return result;
  }

  fprintf(stderr, "waiting for pid [%d] exit ...\n", (int)pid);

  /* 每隔一秒钟发送一次SIGTERM信号直到进程被关闭 */
  do {
    sleep(1);
  } while (kill(pid, SIGTERM) == 0);
  fprintf(stderr, "pid [%d] exit.\n", (int)pid);

  return 0;
}

/* 
 * 重启pid文件中所写指定进程
 * 其实就是关闭，因为main函数中后面会根据bool stop的值来判断是否再次start
 */
int process_restart(const char *pidFilename)
{
  int result;
  pid_t pid;

   /* 从文件中获取需要停止的进程号 */
  result = do_stop(pidFilename, false, &pid);
  if (result == 0) {
    fprintf(stderr, "waiting for pid [%d] exit ...\n", (int)pid);
    do {
      sleep(1);
    } while (kill(pid, SIGTERM) == 0);
    fprintf(stderr, "starting ...\n");
  }

  if (result == ENOENT || result == ESRCH) {
    return 0;
  }

  return result;
}

int process_exist(const char *pidFilename)
{
  pid_t pid;
  int result;

  if ((result=get_pid_from_file(pidFilename, &pid)) != 0) {
    if (result == ENOENT) {
      return false;
    }
    else {
      fprintf(stderr, "get pid from file: %s fail, " \
          "errno: %d, error info: %s\n",
          pidFilename, result, strerror(result));
      return true;
    }
  }

  if (kill(pid, 0) == 0) {
    return true;
  }
  else if (errno == ENOENT || errno == ESRCH) {
    return false;
  }
  else {
    fprintf(stderr, "kill pid: %d fail, errno: %d, error info: %s\n",
        (int)pid, errno, strerror(errno));
    return true;
  }
}

/* 加载配置文件到内存中并且获取配置文件中的"base_path"字段值 */
int get_base_path_from_conf_file(const char *filename, char *base_path,
	const int path_size) 
{
	char *pBasePath;
	IniContext iniContext;
	int result;

	memset(&iniContext, 0, sizeof(IniContext));
	/* 加载配置文件到相应结构体对象中 */
	if ((result=iniLoadFromFile(filename, &iniContext)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"load conf file \"%s\" fail, ret code: %d", \
			__LINE__, filename, result);
		return result;
	}

	do
	{
		/* 获取"base_path"对应的值 */
		pBasePath = iniGetStrValue(NULL, "base_path", &iniContext);
		if (pBasePath == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\" must have item " \
				"\"base_path\"!", __LINE__, filename);
			result = ENOENT;
			break;
		}

		snprintf(base_path, path_size, "%s", pBasePath);
		/* 去除结尾的"/" */
		chopPath(base_path);
		/* 检查base_path对应目录是否存在 */
		if (!fileExists(base_path))
		{
			logError("file: "__FILE__", line: %d, " \
				"\"%s\" can't be accessed, error info: %s", \
				__LINE__, base_path, STRERROR(errno));
			result = errno != 0 ? errno : ENOENT;
			break;
		}
		/* 检查base_path是否是目录名 */
		if (!isDir(base_path))
		{
			logError("file: "__FILE__", line: %d, " \
				"\"%s\" is not a directory!", \
				__LINE__, base_path);
			result = ENOTDIR;
			break;
		}
	} while (0);

	iniFreeContext(&iniContext);
	return result;
}

/* 处理启动参数，start | restart | stop */
int process_action(const char *pidFilename, const char *action, bool *stop)
{
	*stop = false;
	/* 没有传启动参数 */
	if (action == NULL)
	{
		return 0;
	}

	/* 停止守护进程 */
	if (strcmp(action, "stop") == 0)
	{
		*stop = true;
		return process_stop(pidFilename);
	}
	/* 重启守护进程 */
	else if (strcmp(action, "restart") == 0)
	{
		/* 将当前守护进程关闭 */
		return process_restart(pidFilename);
	}
	/* 启动守护进程 */
	else if (strcmp(action, "start") == 0)
	{
		return 0;
	}
	else
	{
		fprintf(stderr, "invalid action: %s\n", action);
		return EINVAL;
	}
}

