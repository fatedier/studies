/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//trunk_shared.h

#ifndef _TRUNK_SHARED_H_
#define _TRUNK_SHARED_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include "base64.h"
#include "common_define.h"
#include "ini_file_reader.h"
#include "fdfs_global.h"
#include "tracker_types.h"

#define FDFS_TRUNK_STATUS_FREE  0
#define FDFS_TRUNK_STATUS_HOLD  1

#define FDFS_TRUNK_FILE_TYPE_NONE     '\0'
#define FDFS_TRUNK_FILE_TYPE_REGULAR  'F'
#define FDFS_TRUNK_FILE_TYPE_LINK     'L'

#define FDFS_STAT_FUNC_STAT     0
#define FDFS_STAT_FUNC_LSTAT    1

#define FDFS_TRUNK_FILE_FILE_TYPE_OFFSET	0
#define FDFS_TRUNK_FILE_ALLOC_SIZE_OFFSET	1
#define FDFS_TRUNK_FILE_FILE_SIZE_OFFSET	5
#define FDFS_TRUNK_FILE_FILE_CRC32_OFFSET	9
#define FDFS_TRUNK_FILE_FILE_MTIME_OFFSET  	13
#define FDFS_TRUNK_FILE_FILE_EXT_NAME_OFFSET	17
#define FDFS_TRUNK_FILE_HEADER_SIZE	(17 + FDFS_FILE_EXT_NAME_MAX_LEN + 1)

#define TRUNK_CALC_SIZE(file_size) (FDFS_TRUNK_FILE_HEADER_SIZE + file_size)

/* 去掉trunk_header后的实际的小文件内容的首地址 */
#define TRUNK_FILE_START_OFFSET(trunkInfo) \
		(FDFS_TRUNK_FILE_HEADER_SIZE + trunkInfo.file.offset)

/* 根据id检查是否是trunkfile */
#define IS_TRUNK_FILE_BY_ID(trunkInfo) (trunkInfo.file.id > 0)

#define TRUNK_GET_FILENAME(file_id, filename) \
	sprintf(filename, "%06d", file_id)

#ifdef __cplusplus
extern "C" {
#endif

extern FDFSStorePaths g_fdfs_store_paths;  //file store paths
extern struct base64_context g_fdfs_base64_context;   //base64 context

typedef int (*stat_func)(const char *filename, struct stat *buf);

typedef struct tagFDFSTrunkHeader {
	char file_type;
	char formatted_ext_name[FDFS_FILE_EXT_NAME_MAX_LEN + 2];
	int alloc_size;
	int file_size;
	int crc32;
	int mtime;
} FDFSTrunkHeader;	/* trunk_header，单个小文件的头部信息 */

typedef struct tagFDFSTrunkPathInfo {
	unsigned char store_path_index;   /* store which path as Mxx(存储在哪个index的store_path中) */
	unsigned char sub_path_high;      /* high sub dir index, front part of HH/HH(二级目录名，2位16进制数) */
	unsigned char sub_path_low;       /* low sub dir index, tail part of HH/HH(三级目录名，2位16进制数) */
} FDFSTrunkPathInfo;		/* trunk的path信息结构 */

typedef struct tagFDFSTrunkFileInfo {
	int id;      /* trunk file id(所属trunk_file的id) */
	int offset;  /* file offset(在此trunk_file中的便宜位置) */
	int size;    /* space size(此小文件的大小) */
} FDFSTrunkFileInfo;		/* trunk_file中单个小文件的信息结构 */

typedef struct tagFDFSTrunkFullInfo {
	char status;  /* normal or hold(状态) */
	FDFSTrunkPathInfo path;	/* trunk file的路径 */
	FDFSTrunkFileInfo file;		/* trunk小文件的信息 */
} FDFSTrunkFullInfo;		/* trunk_file小文件的详细信息结构 */

/* 
 * 从读取到的配置文件数据中获取store_path的信息
 * store_path路径数组通过返回值返回
 * 路径个数通过path_count返回
 */
char **storage_load_paths_from_conf_file_ex(IniContext *pItemContext, \
	const char *szSectionName, const bool bUseBasePath, \
	int *path_count, int *err_no);

/* 从storage配置文件中获取相关路径，包括base_path, store_path等 */
int storage_load_paths_from_conf_file(IniContext *pItemContext);


void trunk_shared_init();

/* 解析logic_filename，解析出true_filename，最后ppStorePath指向存储路径名 */
int storage_split_filename(const char *logic_filename, \
		int *filename_len, char *true_filename, char **ppStorePath);

/* 
 * 解析filename，store_path_index为M后面的两位，并且true_filename减去四个字节 "M00/" 
 * 需要检查store_path_index是否正确
 */
int storage_split_filename_ex(const char *logic_filename, \
		int *filename_len, char *true_filename, int *store_path_index);

/* 
 * 解析filename，store_path_index为M后面的两位，并且true_filename减去四个字节 "M00/" 
 * 不检查store_path_index是否正确
 */
int storage_split_filename_no_check(const char *logic_filename, \
		int *filename_len, char *true_filename, int *store_path_index);

/* 将trunk_file的信息由int转换为字符串并进行base64加密 */
void trunk_file_info_encode(const FDFSTrunkFileInfo *pTrunkFile, char *str);

/* 对str解密，解析出FDFSTrunkFileInfo的id，offset，size相关信息 */
void trunk_file_info_decode(const char *str, FDFSTrunkFileInfo *pTrunkFile);

/* 将pTrunkInfo的信息以字符串的形式写入buff中 */
char *trunk_info_dump(const FDFSTrunkFullInfo *pTrunkInfo, char *buff, \
				const int buff_size);

/* 将trunk_header信息dump到buff中 */
char *trunk_header_dump(const FDFSTrunkHeader *pTrunkHeader, char *buff, \
				const int buff_size);

/* 获取trunk_file的完整路径名称 */
#define trunk_get_full_filename(pTrunkInfo, full_filename, buff_size) \
	trunk_get_full_filename_ex(&g_fdfs_store_paths, pTrunkInfo, \
		full_filename, buff_size)

/* 获取trunk_file的完整路径名称 */
char *trunk_get_full_filename_ex(const FDFSStorePaths *pStorePaths, \
		const FDFSTrunkFullInfo *pTrunkInfo, \
		char *full_filename, const int buff_size);

/* 将FDFSTrunkHeader中的内容转换成字符串 */
void trunk_pack_header(const FDFSTrunkHeader *pTrunkHeader, char *buff);

/* 用相应的字符串构造FDFSTrunkHeader结构 */
void trunk_unpack_header(const char *buff, FDFSTrunkHeader *pTrunkHeader);

/* 获取指定小文件在trunk_file中对应的内容，获取长度为file_size，存放在buff中 */
#define trunk_file_get_content(pTrunkInfo, file_size, pfd, buff, buff_size) \
	trunk_file_get_content_ex(&g_fdfs_store_paths, pTrunkInfo, \
		file_size, pfd, buff, buff_size)

/* 获取指定小文件在trunk_file中对应的内容，存放在buff中 */
int trunk_file_get_content_ex(const FDFSStorePaths *pStorePaths, \
		const FDFSTrunkFullInfo *pTrunkInfo, const int file_size, \
		int *pfd, char *buff, const int buff_size);

/* 获取文件链接的状态信息 */
#define trunk_file_do_lstat_func(store_path_index, true_filename, \
		filename_len, stat_func, pStat, pTrunkInfo, pTrunkHeader, pfd) \
	trunk_file_do_lstat_func_ex(&g_fdfs_store_paths, store_path_index, \
		true_filename, filename_len, stat_func, pStat, pTrunkInfo, \
		pTrunkHeader, pfd)

/* 获取指定文件的状态信息，如果是链接，获取链接的源文件的状态信息 */
#define trunk_file_stat_func(store_path_index, true_filename, filename_len, \
		stat_func, pStat, pTrunkInfo, pTrunkHeader, pfd) \
	trunk_file_stat_func_ex(&g_fdfs_store_paths, store_path_index, \
		true_filename, filename_len, stat_func, pStat, pTrunkInfo, \
		pTrunkHeader, pfd)

/* 获取指定普通文件的状态信息，如果是链接，获取链接的源文件的状态信息 */
#define trunk_file_stat(store_path_index, true_filename, filename_len, \
		pStat, pTrunkInfo, pTrunkHeader) \
	trunk_file_stat_func(store_path_index, true_filename, filename_len, \
		FDFS_STAT_FUNC_STAT, pStat, pTrunkInfo, pTrunkHeader, NULL)

/* 获取文件链接的状态信息 */
#define trunk_file_lstat(store_path_index, true_filename, filename_len, \
		pStat, pTrunkInfo, pTrunkHeader) \
	trunk_file_do_lstat_func(store_path_index, true_filename, filename_len, \
		FDFS_STAT_FUNC_LSTAT, pStat, pTrunkInfo, pTrunkHeader, NULL)

/* 获取文件链接的状态信息，需要返回对应文件描述符 */
#define trunk_file_lstat_ex(store_path_index, true_filename, filename_len, \
		pStat, pTrunkInfo, pTrunkHeader, pfd) \
	trunk_file_do_lstat_func(store_path_index, true_filename, filename_len, \
		FDFS_STAT_FUNC_LSTAT, pStat, pTrunkInfo, pTrunkHeader, pfd)

/* 获取普通文件的状态信息，需要返回对应文件描述符 */
#define trunk_file_stat_ex(store_path_index, true_filename, filename_len, \
		pStat, pTrunkInfo, pTrunkHeader, pfd) \
	trunk_file_stat_func(store_path_index, true_filename, filename_len, \
		FDFS_STAT_FUNC_STAT, pStat, pTrunkInfo, pTrunkHeader, pfd)

/* 获取普通文件的状态信息，需要返回对应文件描述符 */
#define trunk_file_stat_ex1(pStorePaths, store_path_index, true_filename, \
		filename_len, pStat, pTrunkInfo, pTrunkHeader, pfd) \
	trunk_file_stat_func_ex(pStorePaths, store_path_index, true_filename, \
		filename_len, FDFS_STAT_FUNC_STAT, pStat, pTrunkInfo, \
		pTrunkHeader, pfd)

/* 获取指定文件的状态信息，如果是链接，获取链接的源文件的状态信息 */
int trunk_file_stat_func_ex(const FDFSStorePaths *pStorePaths, \
	const int store_path_index, const char *true_filename, \
	const int filename_len, const int stat_func, \
	struct stat *pStat, FDFSTrunkFullInfo *pTrunkInfo, \
	FDFSTrunkHeader *pTrunkHeader, int *pfd);

/* 获取文件的状态信息 */
int trunk_file_do_lstat_func_ex(const FDFSStorePaths *pStorePaths, \
	const int store_path_index, const char *true_filename, \
	const int filename_len, const int stat_func, \
	struct stat *pStat, FDFSTrunkFullInfo *pTrunkInfo, \
	FDFSTrunkHeader *pTrunkHeader, int *pfd);

/* 根据文件名检查是否是trunk_file */
bool fdfs_is_trunk_file(const char *remote_filename, const int filename_len);

/* 根据相关参数构造FDFSTrunkFullInfo对象 */
int fdfs_decode_trunk_info(const int store_path_index, \
		const char *true_filename, const int filename_len, \
		FDFSTrunkFullInfo *pTrunkInfo);

#ifdef __cplusplus
}
#endif

#endif

