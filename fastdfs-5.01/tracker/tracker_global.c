/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include "tracker_global.h"

volatile bool g_continue_flag = true;
int g_server_port = FDFS_TRACKER_SERVER_DEF_PORT;		/* tracker server端口号，从配置文件获取 */
int g_max_connections = DEFAULT_MAX_CONNECTONS;		/* 最大连接数 */
int g_accept_threads = 1;									/* accept线程数 */
int g_work_threads = DEFAULT_WORK_THREADS;				/* 工作线程数 */
int g_sync_log_buff_interval = SYNC_LOG_BUFF_DEF_INTERVAL;	/* 日志从内存中刷新到文件中的间隔时间 */
int g_check_active_interval = CHECK_ACTIVE_DEF_INTERVAL;	/* 检查storage状态间隔时间 */

struct timeval g_network_tv = {DEFAULT_NETWORK_TIMEOUT, 0};

FDFSGroups g_groups;	/* 所有组信息，包括每一台storage sever */
int g_storage_stat_chg_count = 0;
int g_storage_sync_time_chg_count = 0; 		/* sync timestamp(同步时间戳的变化次数) */
FDFSStorageReservedSpace g_storage_reserved_space = {	/* 服务器保留存储空间大小 */
		TRACKER_STORAGE_RESERVED_SPACE_FLAG_MB};

int g_allow_ip_count = 0;				/* 允许连接ip地址的个数 */
in_addr_t *g_allow_ip_addrs = NULL;	/* 允许连接ip地址数组 */

struct base64_context g_base64_context;

extern gid_t g_run_by_gid;		/* 运行用户组id */
extern uid_t g_run_by_uid;		/* 运行用户id */

char g_run_by_group[32] = {0};	/* 运行用户组 */
char g_run_by_user[32] = {0};		/* 运行用户 */

/* 该参数控制当storage server ip改变时集群是否自动调整 */
bool g_storage_ip_changed_auto_adjust = true;
/* if use storage ID instead of IP address(设置是否使用server id作为storage server的标识) */
bool g_use_storage_id = false;  
/* id type of the storage server in the filename(设置文件名中id格式) */
byte g_id_type_in_filename = FDFS_ID_TYPE_IP_ADDRESS;
/* 设置error log日志是否每天轮转 */
bool g_rotate_error_log = false;
/* 设置error log每天轮转的时间点 */
TimeInfo g_error_log_rotate_time  = {0, 0};

/* 线程栈的大小(单位:字节) */
int g_thread_stack_size = 64 * 1024;
/* storage server之间同步文件的最大延迟时间 */
int g_storage_sync_file_max_delay = DEFAULT_STORAGE_SYNC_FILE_MAX_DELAY;
/* 设置storage server之间同步一个文件的最大超时时间 */
int g_storage_sync_file_max_time = DEFAULT_STORAGE_SYNC_FILE_MAX_TIME;
/* if store slave file use symbol link(重复文件是否使用链接存储) */
bool g_store_slave_file_use_link = false;
/* if use trunk file(是否使用小文件合并存储特性) */
bool g_if_use_trunk_file = false;
/* 设置是否提前创建trunk file  */
bool g_trunk_create_file_advance = false;
/* trunk初始化时，是否检查可用空间被占用 */
bool g_trunk_init_check_occupying = false;
/* 是否无条件从trunk binlog中加载trunk可用空间信息 */
bool g_trunk_init_reload_from_binlog = false;
int g_slot_min_size = 256;    	/* slot min size, such as 256 bytes(trunk file分配的最小字节数) */
/* slot max size, such as 16MB(只有文件大小<= 这个参数的文件才会合并) */
int g_slot_max_size = 16 * 1024 * 1024;
/* the trunk file size, such as 64MB(合并存储的trunk file大小) */
int g_trunk_file_size = 64 * 1024 * 1024;
/* 提前创建trunk file的起始时间点 */
TimeInfo g_trunk_create_file_time_base = {0, 0};
/* 创建trunk file的时间间隔 */
int g_trunk_create_file_interval = 86400;
/* 设置trunk file binlog的压缩时间间隔 */
int g_trunk_compress_binlog_min_interval = 0;
/* 如果提前创建trunk file时，需要达到的空闲trunk大小 */
int64_t g_trunk_create_file_space_threshold = 0;

time_t g_up_time = 0;
TrackerStatus g_tracker_last_status = {0, 0};		/* tracker服务器的状态信息 */

#ifdef WITH_HTTPD
FDFSHTTPParams g_http_params;
int g_http_check_interval = 30;
int g_http_check_type = FDFS_HTTP_CHECK_ALIVE_TYPE_TCP;
char g_http_check_uri[128] = {0};
bool g_http_servers_dirty = false;
#endif

#if defined(DEBUG_FLAG) && defined(OS_LINUX)
char g_exe_name[256] = {0};
#endif

