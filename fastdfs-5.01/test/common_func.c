#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "common_func.h"

/* 读取指定文件中全部内容放到*buff字符串中，文件大小放到file_size中 */
int getFileContent(const char *filename, char **buff, int64_t *file_size)
{
	int fd;
	
	fd = open(filename, O_RDONLY);
	if (fd < 0)
	{
		*buff = NULL;
		*file_size = 0;
		return errno != 0 ? errno : ENOENT;
	}

	if ((*file_size=lseek(fd, 0, SEEK_END)) < 0)
	{
		*buff = NULL;
		*file_size = 0;
		close(fd);
		return errno != 0 ? errno : EIO;
	}

	*buff = (char *)malloc(*file_size + 1);
	if (*buff == NULL)
	{
		*file_size = 0;
		close(fd);

		return errno != 0 ? errno : ENOMEM;
	}

	if (lseek(fd, 0, SEEK_SET) < 0)
	{
		*buff = NULL;
		*file_size = 0;
		close(fd);
		return errno != 0 ? errno : EIO;
	}
	if (read(fd, *buff, *file_size) != *file_size)
	{
		free(*buff);
		*buff = NULL;
		*file_size = 0;
		close(fd);
		return errno != 0 ? errno : EIO;
	}

	(*buff)[*file_size] = '\0';
	close(fd);

	return 0;
}

