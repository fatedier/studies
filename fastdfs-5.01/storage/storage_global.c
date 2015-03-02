/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <netdb.h>
#include <unistd.h>
#include <errno.h>
#include "logger.h"
#include "sockopt.h"
#include "shared_func.h"
#include "storage_global.h"

volatile bool g_continue_flag = true;				/* 进程是否继续的标志 */
FDFSStorePathInfo *g_path_space_list = NULL;		/* 所有存储路径的信息 */
int g_subdir_count_per_path = DEFAULT_DATA_DIR_COUNT_PER_PATH;		/* 每个存储路径下的目录数量 */

int g_server_port = FDFS_STORAGE_SERVER_DEF_PORT;		/* storage server端口号，从配置文件获取 */

/*  
 * storage server上web server域名，通常仅针对单独部署的web server
 * 这样URL中就可以通过域名方式来访问storage server上的文件了 
 */
char g_http_domain[FDFS_DOMAIN_NAME_MAX_SIZE] = {0};
int g_http_port = 80;			/* http端口 */
int g_last_server_port = 0;	/* 之前本机的通信端口 */
int g_last_http_port = 0;		/* 之前使用的http端口 */
int g_max_connections = DEFAULT_MAX_CONNECTONS;
int g_accept_threads = 1;
int g_work_threads = DEFAULT_WORK_THREADS;		/* 工作线程数 */
int g_buff_size = STORAGE_DEFAULT_BUFF_SIZE;		/* 发送和接收数据的最大长度 */

bool g_disk_rw_direct = false;
bool g_disk_rw_separated = true;			/* 磁盘IO读写是否分离 */
int g_disk_reader_threads = DEFAULT_DISK_READER_THREADS;	/* 单个存储路径的读线程数 */
int g_disk_writer_threads = DEFAULT_DISK_WRITER_THREADS;		/* 单个存储路径的写线程数 */
int g_extra_open_file_flags = 0;		/* 打开文件的附加标志位 */

/* 文件在data目录下分散存储策略 */
int g_file_distribute_path_mode = FDFS_FILE_DIST_PATH_ROUND_ROBIN;

/* 
 * 当上面的参数g_file_distribute_path_mode配置为0（轮流存放方式）时，本参数有效。
 * 当一个目录下的文件存放的文件数达到本参数值时
 * 后续上传的文件存储到下一个目录中
 */
int g_file_distribute_rotate_count = FDFS_FILE_DIST_DEFAULT_ROTATE_COUNT;

/* 
 * 当写入大文件时
 * 每写入N个字节调用一次系统函数fsync将内容强行同步到硬盘
 * 0表示从不调用fsync  
 */
int g_fsync_after_written_bytes = -1;

struct timeval g_network_tv = {DEFAULT_NETWORK_TIMEOUT, 0};

int g_dist_path_index_high = 0; /* current write to high path(当前使用的二级目录index) */
int g_dist_path_index_low = 0;  /* current write to low path(当前使用的三级目录index) */
int g_dist_write_file_count = 0;  /* current write file count(当前目录写入文件的次数) */

int g_storage_count = 0;
FDFSStorageServer g_storage_servers[FDFS_MAX_SERVERS_EACH_GROUP];	  /* 同group中的所有storage的集合 */
FDFSStorageServer *g_sorted_storages[FDFS_MAX_SERVERS_EACH_GROUP];	  /* 同group中所有排好序的storage的集合 */

int g_tracker_reporter_count = 0;		/* tracker reporter线程数 */
int g_heart_beat_interval  = STORAGE_BEAT_DEF_INTERVAL;		/* 发送心跳包的间隔时间 */
int g_stat_report_interval = STORAGE_REPORT_DEF_INTERVAL;		/* 汇报状态的间隔时间 */

/* 读取binlog中需要同步的文件，如果获取到，休眠的时间 */
int g_sync_wait_usec = STORAGE_DEF_SYNC_WAIT_MSEC;
/* unit: milliseconds(同步完一个文件后的间隔时间) */
int g_sync_interval = 0;
TimeInfo g_sync_start_time = {0, 0};		/* 每天开始同步文件的时间 */
TimeInfo g_sync_end_time = {23, 59};		/* 获取每天结束同步文件的时间 */
bool g_sync_part_time = false;				/* 是否一天中的部分时段才同步文件 */
/* 日志从内存中刷新到文件中的间隔时间 */
int g_sync_log_buff_interval = SYNC_LOG_BUFF_DEF_INTERVAL;
/* 同步binglog（更新操作日志）到硬盘的时间间隔，单位为秒 */
int g_sync_binlog_buff_interval = SYNC_BINLOG_BUFF_DEF_INTERVAL;
/* 同步完多少个文件后，把storage的mark文件同步到磁盘 */
int g_write_mark_file_freq = FDFS_DEFAULT_SYNC_MARK_FILE_FREQ;
/* 把storage的stat文件同步到磁盘的时间间隔，单位为秒*/
int g_sync_stat_file_interval = DEFAULT_SYNC_STAT_FILE_INTERVAL;

FDFSStorageStat g_storage_stat;
int g_stat_change_count = 1;
int g_sync_change_count = 0;

int g_storage_join_time = 0;		/* storage加入的时间 */
int g_sync_until_timestamp = 0;	/* 最近一次同步时间戳 */
bool g_sync_old_done = false;		/* 之前的同步是否完成 */
char g_sync_src_id[FDFS_STORAGE_ID_MAX_SIZE] = {0};		/* 同步的源storage server */

char g_group_name[FDFS_GROUP_NAME_MAX_LEN + 1] = {0};		/* storage所属group */
char g_my_server_id_str[FDFS_STORAGE_ID_MAX_SIZE] = {0}; /* my server id string(自身的id) */
char g_tracker_client_ip[IP_ADDRESS_SIZE] = {0}; /* storage ip as tracker client(与tracker建立连接的自身的ip) */
char g_last_storage_ip[IP_ADDRESS_SIZE] = {0};	 /* the last storage ip address(之前本机的ip地址) */

/* access_log的全局日志对象 */
LogContext g_access_log_context = {LOG_INFO, STDERR_FILENO, NULL};

in_addr_t g_server_id_in_filename = 0;
bool g_use_access_log = false;    /* if log to access log(是否将文件操作记录到access log ) */
bool g_rotate_access_log = false; /* if rotate the access log every day(是否每天轮转access log) */
/* 设置error log日志是否每天轮转 */
bool g_rotate_error_log = false;
bool g_use_storage_id = false;    //identify storage by ID instead of IP address
byte g_id_type_in_filename = FDFS_ID_TYPE_IP_ADDRESS; //id type of the storage server in the filename
bool g_store_slave_file_use_link = false; //if store slave file use symbol link

/* 
 * 是否检测上传文件已经存在
 * 如果已经存在，则不存在文件内容，建立一个符号链接以节省磁盘空间
 * 这个应用要配合FastDHT 使用，所以打开前要先安装FastDHT 
 */
bool g_check_file_duplicate = false;	/* 是否检测文件重复 */
byte g_file_signature_method = STORAGE_FILE_SIGNATURE_METHOD_HASH;
char g_key_namespace[FDHT_MAX_NAMESPACE_LEN+1] = {0};
int g_namespace_len = 0;
int g_allow_ip_count = 0;
in_addr_t *g_allow_ip_addrs = NULL;

TimeInfo g_access_log_rotate_time = {0, 0}; /* rotate access log time base(access log每天轮转时间点) */
TimeInfo g_error_log_rotate_time  = {0, 0}; /* rotate error log time base(error log每天轮转时间点) */

gid_t g_run_by_gid;					/* 当前守护进程运行gid */
uid_t g_run_by_uid;					/* 当前守护进程运行uid */

char g_run_by_group[32] = {0};		/* 运行用户组 */
char g_run_by_user[32] = {0};			/* 运行用户 */

char g_bind_addr[IP_ADDRESS_SIZE] = {0};	/* storage server绑定的ip地址 */
bool g_client_bind_addr = true;		/* storage作为client时是否使用绑定的ip地址 */
/* 该参数控制当storage server ip改变时集群是否自动调整 */
bool g_storage_ip_changed_auto_adjust = false;
bool g_thread_kill_done = false;
bool g_file_sync_skip_invalid_record = false;	/* 同步时是否跳过无效的记录 */

/* 线程栈大小 */
int g_thread_stack_size = 512 * 1024;

/* 
 * 本storage server作为源服务器，上传文件的优先级，可以为负数
 * 值越小，优先级越高 
 */
int g_upload_priority = DEFAULT_UPLOAD_PRIORITY;
time_t g_up_time = 0;		/* 进程启动时间 */

#ifdef WITH_HTTPD
FDFSHTTPParams g_http_params;
int g_http_trunk_size = 64 * 1024;
#endif

#if defined(DEBUG_FLAG) && defined(OS_LINUX)
char g_exe_name[256] = {0};
#endif

struct storage_nio_thread_data *g_nio_thread_data = NULL;	/* 每个工作线程的线程数据 */
struct storage_dio_thread_data *g_dio_thread_data = NULL;	/* dio线程变量数组，每一个存储路径对应一个 */

/* 比较两个FDFSStorageServer对象，根据server.id比较 */
int storage_cmp_by_server_id(const void *p1, const void *p2)
{
	return strcmp((*((FDFSStorageServer **)p1))->server.id,
		(*((FDFSStorageServer **)p2))->server.id);
}

