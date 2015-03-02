#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "shared_func.h"
#include "logger.h"
#include "process_ctrl.h"

/* ���ļ��л�ȡpidֵ */
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

/* ���ػ����̵Ľ��̺�д�뵽pid�ļ��� */
int write_to_pid_file(const char *pidFilename)
{
  char buff[32];
  int len;

  len = sprintf(buff, "%d", (int)getpid());
  return writeToFile(pidFilename, buff, len);
}

/* ɾ��pid_file */
int delete_pid_file(const char *pidFilename)
{
  if (unlink(pidFilename) == 0) {
    return 0;
  }
  else {
    return errno != 0 ? errno : ENOENT;
  }
}

/* ����pid�ļ�������ָֹͣ������ */
static int do_stop(const char *pidFilename, const bool bShowError, pid_t *pid)
{
  int result;

  /* ���ļ��л�ȡ�ػ�����pidֵ */
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

	/* ʹ��SIGTERM�źŹر�ָ��pid�Ľ��� */
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

/* ֹͣpid�ļ�����дָ������*/
int process_stop(const char *pidFilename)
{
  pid_t pid;
  int result;

  /* ����pid�ļ�������ָֹͣ������ */
  result = do_stop(pidFilename, true, &pid);
  if (result != 0) {
    return result;
  }

  fprintf(stderr, "waiting for pid [%d] exit ...\n", (int)pid);

  /* ÿ��һ���ӷ���һ��SIGTERM�ź�ֱ�����̱��ر� */
  do {
    sleep(1);
  } while (kill(pid, SIGTERM) == 0);
  fprintf(stderr, "pid [%d] exit.\n", (int)pid);

  return 0;
}

/* 
 * ����pid�ļ�����дָ������
 * ��ʵ���ǹرգ���Ϊmain�����к�������bool stop��ֵ���ж��Ƿ��ٴ�start
 */
int process_restart(const char *pidFilename)
{
  int result;
  pid_t pid;

   /* ���ļ��л�ȡ��Ҫֹͣ�Ľ��̺� */
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

/* ���������ļ����ڴ��в��һ�ȡ�����ļ��е�"base_path"�ֶ�ֵ */
int get_base_path_from_conf_file(const char *filename, char *base_path,
	const int path_size) 
{
	char *pBasePath;
	IniContext iniContext;
	int result;

	memset(&iniContext, 0, sizeof(IniContext));
	/* ���������ļ�����Ӧ�ṹ������� */
	if ((result=iniLoadFromFile(filename, &iniContext)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"load conf file \"%s\" fail, ret code: %d", \
			__LINE__, filename, result);
		return result;
	}

	do
	{
		/* ��ȡ"base_path"��Ӧ��ֵ */
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
		/* ȥ����β��"/" */
		chopPath(base_path);
		/* ���base_path��ӦĿ¼�Ƿ���� */
		if (!fileExists(base_path))
		{
			logError("file: "__FILE__", line: %d, " \
				"\"%s\" can't be accessed, error info: %s", \
				__LINE__, base_path, STRERROR(errno));
			result = errno != 0 ? errno : ENOENT;
			break;
		}
		/* ���base_path�Ƿ���Ŀ¼�� */
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

/* ��������������start | restart | stop */
int process_action(const char *pidFilename, const char *action, bool *stop)
{
	*stop = false;
	/* û�д��������� */
	if (action == NULL)
	{
		return 0;
	}

	/* ֹͣ�ػ����� */
	if (strcmp(action, "stop") == 0)
	{
		*stop = true;
		return process_stop(pidFilename);
	}
	/* �����ػ����� */
	else if (strcmp(action, "restart") == 0)
	{
		/* ����ǰ�ػ����̹ر� */
		return process_restart(pidFilename);
	}
	/* �����ػ����� */
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
