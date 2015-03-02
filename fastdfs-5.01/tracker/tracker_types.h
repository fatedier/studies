/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_types.h

#ifndef _TRACKER_TYPES_H_
#define _TRACKER_TYPES_H_

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include "fdfs_define.h"
#include "connection_pool.h"

#define FDFS_ONE_MB	(1024 * 1024)		/* 1MB对应 1024*1024字节 */

#define FDFS_GROUP_NAME_MAX_LEN		16		/* Group Name的最大长度 */
#define FDFS_MAX_SERVERS_EACH_GROUP	32		/* 每个group中storage的上限 */
#define FDFS_MAX_GROUPS		       512			/* 最大group数 */
#define FDFS_MAX_TRACKERS		16		/* tracker server最大数量 */

#define FDFS_MAX_META_NAME_LEN		 64
#define FDFS_MAX_META_VALUE_LEN		256

#define FDFS_FILE_PREFIX_MAX_LEN	16
#define FDFS_LOGIC_FILE_PATH_LEN	10
#define FDFS_TRUE_FILE_PATH_LEN		 6
#define FDFS_FILENAME_BASE64_LENGTH     27
#define FDFS_TRUNK_FILE_INFO_LEN  16
#define FDFS_MAX_SERVER_ID        ((1 << 24) - 1)	/* storage server id的最大值 */

#define FDFS_ID_TYPE_SERVER_ID    1		/* id格式为server id */
#define FDFS_ID_TYPE_IP_ADDRESS   2	/* id格式为ip地址 */

#define FDFS_NORMAL_LOGIC_FILENAME_LENGTH (FDFS_LOGIC_FILE_PATH_LEN + \
		FDFS_FILENAME_BASE64_LENGTH + FDFS_FILE_EXT_NAME_MAX_LEN + 1)

#define FDFS_TRUNK_FILENAME_LENGTH (FDFS_TRUE_FILE_PATH_LEN + \
		FDFS_FILENAME_BASE64_LENGTH + FDFS_TRUNK_FILE_INFO_LEN + \
		1 + FDFS_FILE_EXT_NAME_MAX_LEN)
#define FDFS_TRUNK_LOGIC_FILENAME_LENGTH  (FDFS_TRUNK_FILENAME_LENGTH + \
		(FDFS_LOGIC_FILE_PATH_LEN - FDFS_TRUE_FILE_PATH_LEN))

#define FDFS_VERSION_SIZE		6

/* storager所处状态 */
//status order is important!
#define FDFS_STORAGE_STATUS_INIT	  0
#define FDFS_STORAGE_STATUS_WAIT_SYNC	  1
#define FDFS_STORAGE_STATUS_SYNCING	  2
#define FDFS_STORAGE_STATUS_IP_CHANGED    3
#define FDFS_STORAGE_STATUS_DELETED	  4
#define FDFS_STORAGE_STATUS_OFFLINE	  5
#define FDFS_STORAGE_STATUS_ONLINE	  6
#define FDFS_STORAGE_STATUS_ACTIVE	  7
#define FDFS_STORAGE_STATUS_RECOVERY	  9
#define FDFS_STORAGE_STATUS_NONE	 99

/* 选择哪个组来上传文件的方式 */
//which group to upload file
#define FDFS_STORE_LOOKUP_ROUND_ROBIN	0  //round robin(循环)
#define FDFS_STORE_LOOKUP_SPEC_GROUP	1  //specify group(特定组)
#define FDFS_STORE_LOOKUP_LOAD_BALANCE	2  //load balance(均衡选择)

/* 上传文件时同一个组中选择哪一个storage的方式 */
//which server to upload file
#define FDFS_STORE_SERVER_ROUND_ROBIN	0  //round robin(循环)
#define FDFS_STORE_SERVER_FIRST_BY_IP	1  //the first server order by ip(根据ip排序的首个)
#define FDFS_STORE_SERVER_FIRST_BY_PRI	2  //the first server order by priority(根据优先级排序的首个)

/* 下载文件时选择哪一个storage的方式 */
//which server to download file
#define FDFS_DOWNLOAD_SERVER_ROUND_ROBIN	0  //round robin(循环)
#define FDFS_DOWNLOAD_SERVER_SOURCE_FIRST	1  //the source server(当初上传时的storage server)

/* 上传文件时选择哪一个目录的方式 */
//which path to upload file
#define FDFS_STORE_PATH_ROUND_ROBIN	0  //round robin(循环)
#define FDFS_STORE_PATH_LOAD_BALANCE	2  //load balance(均衡选择)

/* 
 * the mode of the files distributed to the data path
 * 文件在data目录下分散存储策略 
 */
#define FDFS_FILE_DIST_PATH_ROUND_ROBIN	0  //round robin
#define FDFS_FILE_DIST_PATH_RANDOM	1  //random(根据文件进行hash)

//http check alive type
#define FDFS_HTTP_CHECK_ALIVE_TYPE_TCP  0  //tcp
#define FDFS_HTTP_CHECK_ALIVE_TYPE_HTTP 1  //http

#define FDFS_DOWNLOAD_TYPE_TCP	0  //tcp
#define FDFS_DOWNLOAD_TYPE_HTTP	1  //http

/* 默认轮转存储方式的话每个目录存储多少文件后转到下一个目录 */
#define FDFS_FILE_DIST_DEFAULT_ROTATE_COUNT   100

#define FDFS_DOMAIN_NAME_MAX_SIZE	128

#define FDFS_STORAGE_STORE_PATH_PREFIX_CHAR  'M'		/* 文件名中store_path的前缀字符 */
#define FDFS_STORAGE_DATA_DIR_FORMAT         "%02X"
#define FDFS_STORAGE_META_FILE_EXT           "-m"

#define FDFS_APPENDER_FILE_SIZE  INFINITE_FILE_SIZE
#define FDFS_TRUNK_FILE_MARK_SIZE  (512 * 1024LL * 1024 * 1024 * 1024 * 1024LL)

#define FDFS_CHANGE_FLAG_TRACKER_LEADER	1  //tracker leader changed
#define FDFS_CHANGE_FLAG_TRUNK_SERVER	2  //trunk server changed
#define FDFS_CHANGE_FLAG_GROUP_SERVER	4  //group server changed

/* 根据file_size中的某一位判断是否是appender_file */
#define IS_APPENDER_FILE(file_size)   ((file_size & FDFS_APPENDER_FILE_SIZE)!=0)
/* 根据file_size中的某一位判断是否是trunk_file */
#define IS_TRUNK_FILE(file_size)     ((file_size&FDFS_TRUNK_FILE_MARK_SIZE)!=0)

#define IS_SLAVE_FILE(filename_len, file_size) \
	((filename_len > FDFS_TRUNK_LOGIC_FILENAME_LENGTH) || \
	(filename_len > FDFS_NORMAL_LOGIC_FILENAME_LENGTH && \
	 !IS_TRUNK_FILE(file_size)))

/* 获取trunk_file的真实文件大小，只取后32位，前面置为0 */
#define FDFS_TRUNK_FILE_TRUE_SIZE(file_size) \
	(file_size & 0xFFFFFFFF)

#define FDFS_STORAGE_ID_MAX_SIZE	16		/* storage server id最大长度 */

/* 每台storage server保留存储空间大小数字标志 */
#define TRACKER_STORAGE_RESERVED_SPACE_FLAG_MB		0	/* 具体数字 */
#define TRACKER_STORAGE_RESERVED_SPACE_FLAG_RATIO	1	/* 百分比 */

typedef struct
{
	char status;			/* 状态 */
	char port[4];			/* 通信端口 */
	char id[FDFS_STORAGE_ID_MAX_SIZE];	/* id */
	char ip_addr[IP_ADDRESS_SIZE];		/* ip地址 */
} FDFSStorageBrief;	/* storage server的简要信息结构 */

typedef struct
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 8];  //for 8 bytes alignment
	int64_t total_mb;  //total disk storage in MB
	int64_t free_mb;  //free disk storage in MB
	int64_t trunk_free_mb;  //trunk free disk storage in MB
	int count;        //server count
	int storage_port; //storage server port
	int storage_http_port; //storage server http port
	int active_count; //active server count
	int current_write_server; //current server index to upload file
	int store_path_count;  //store base path count of each storage server
	int subdir_count_per_path;
	int current_trunk_file_id;  //current trunk file id
} FDFSGroupStat;

typedef struct
{
	/* following count stat by source server,
           not including synced count
	*/
	int64_t total_upload_count;
	int64_t success_upload_count;
	int64_t total_append_count;
	int64_t success_append_count;
	int64_t total_modify_count;
	int64_t success_modify_count;
	int64_t total_truncate_count;
	int64_t success_truncate_count;
	int64_t total_set_meta_count;
	int64_t success_set_meta_count;
	int64_t total_delete_count;
	int64_t success_delete_count;
	int64_t total_download_count;
	int64_t success_download_count;
	int64_t total_get_meta_count;
	int64_t success_get_meta_count;
	int64_t total_create_link_count;
	int64_t success_create_link_count;
	int64_t total_delete_link_count;
	int64_t success_delete_link_count;
	int64_t total_upload_bytes;
	int64_t success_upload_bytes;
	int64_t total_append_bytes;
	int64_t success_append_bytes;
	int64_t total_modify_bytes;
	int64_t success_modify_bytes;
	int64_t total_download_bytes;
	int64_t success_download_bytes;
	int64_t total_sync_in_bytes;
	int64_t success_sync_in_bytes;
	int64_t total_sync_out_bytes;
	int64_t success_sync_out_bytes;
	int64_t total_file_open_count;
	int64_t success_file_open_count;
	int64_t total_file_read_count;
	int64_t success_file_read_count;
	int64_t total_file_write_count;
	int64_t success_file_write_count;

	/* last update timestamp as source server, 
           current server' timestamp
	*/
	time_t last_source_update;

	/* last update timestamp as dest server, 
           current server' timestamp
	*/
	time_t last_sync_update;

	/* 
	 * last syned timestamp, 
	 * source server's timestamp
	 * 最近一次全部同步完成的同步时间戳
	 */
	time_t last_synced_timestamp;

	/* last heart beat time (最近一次收到心跳包的时间) */
	time_t last_heart_beat_time;
} FDFSStorageStat;		/* storage server状态信息 */

/* struct for network transfering */
typedef struct
{
	char sz_total_upload_count[8];
	char sz_success_upload_count[8];
	char sz_total_append_count[8];
	char sz_success_append_count[8];
	char sz_total_modify_count[8];
	char sz_success_modify_count[8];
	char sz_total_truncate_count[8];
	char sz_success_truncate_count[8];
	char sz_total_set_meta_count[8];
	char sz_success_set_meta_count[8];
	char sz_total_delete_count[8];
	char sz_success_delete_count[8];
	char sz_total_download_count[8];
	char sz_success_download_count[8];
	char sz_total_get_meta_count[8];
	char sz_success_get_meta_count[8];
	char sz_total_create_link_count[8];
	char sz_success_create_link_count[8];
	char sz_total_delete_link_count[8];
	char sz_success_delete_link_count[8];
	char sz_total_upload_bytes[8];
	char sz_success_upload_bytes[8];
	char sz_total_append_bytes[8];
	char sz_success_append_bytes[8];
	char sz_total_modify_bytes[8];
	char sz_success_modify_bytes[8];
	char sz_total_download_bytes[8];
	char sz_success_download_bytes[8];
	char sz_total_sync_in_bytes[8];
	char sz_success_sync_in_bytes[8];
	char sz_total_sync_out_bytes[8];
	char sz_success_sync_out_bytes[8];
	char sz_total_file_open_count[8];
	char sz_success_file_open_count[8];
	char sz_total_file_read_count[8];
	char sz_success_file_read_count[8];
	char sz_total_file_write_count[8];
	char sz_success_file_write_count[8];
	char sz_last_source_update[8];
	char sz_last_sync_update[8];
	char sz_last_synced_timestamp[8];
	char sz_last_heart_beat_time[8];
} FDFSStorageStatBuff;	/* storage server状态信息字符串 */

typedef struct StructFDFSStorageDetail
{
	char status;
	char padding;  //just for padding
	char id[FDFS_STORAGE_ID_MAX_SIZE];
	char ip_addr[IP_ADDRESS_SIZE];
	char version[FDFS_VERSION_SIZE];
	char domain_name[FDFS_DOMAIN_NAME_MAX_SIZE];

	struct StructFDFSStorageDetail *psync_src_server;	/* 从哪台storage同步的文件 */
	int64_t *path_total_mbs; /* total disk storage in MB(每个路径下分别统计的总空间大小) */
	int64_t *path_free_mbs;  /* free disk storage in MB(每个路径下分别统计的空闲空间大小) */

	int64_t total_mb;  /* total disk storage in MB(累计总空间大小) */
	int64_t free_mb;  /* free disk storage in MB(累计空闲空间大小) */
	int64_t changelog_offset;  /* changelog file offset(changelog文件中的偏移量) */

	time_t sync_until_timestamp;		/* 同步时间戳 */
	time_t join_time;  //storage join timestamp (create timestamp)
	time_t up_time;    //startup timestamp

	int store_path_count;  /* store base path count of each storage server(base path的个数) */
	int subdir_count_per_path;	/* 二级目录的个数 */
	int upload_priority; /* storage upload priority(上传优先级) */

	int storage_port;   //storage server port
	int storage_http_port; //storage http server port

	int current_write_path; /* current write path index(当前使用的路径索引) */

	int chg_count;    /* current server changed counter(storage server信息改变次数) */
	int trunk_chg_count;   /* trunk server changed count(trunk server信息改变次数) */
	FDFSStorageStat stat;

#ifdef WITH_HTTPD
	int http_check_last_errno;
	int http_check_last_status;
	int http_check_fail_count;
	char http_check_error_info[256];
#endif
} FDFSStorageDetail;	/* storage server信息 */

typedef struct
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 8];   //for 8 bytes alignment
	int64_t total_mb;  //total disk storage in MB
	int64_t free_mb;  //free disk storage in MB
	int64_t trunk_free_mb;  //trunk free disk storage in MB
	int alloc_size;  //alloc storage count(已分配空间的storage server元素个数)
	int count;    //total server count(实际的storage server元素个数)
	int active_count; //active server count(活跃的storage server元素个数)
	int storage_port;  //storage server port(storage server用来与tracker server通讯的端口)
	int storage_http_port; //storage http server port
	int current_trunk_file_id;  /* current trunk file id report by storage(storage汇报的当前trunk_file_id) */
	FDFSStorageDetail **all_servers;   //all storage servers(存放所有storage server信息)
	FDFSStorageDetail **sorted_servers;  /* storages order by ip addr(存放根据ip排序的所有storage server信息) */
	FDFSStorageDetail **active_servers;  /* storages order by ip addr(存放根据ip排序的活跃的storage server信息) */
	FDFSStorageDetail *pStoreServer;  /* for upload priority mode(upload_priority值最小，优先级高的stroage) */
	FDFSStorageDetail *pTrunkServer;  /* point to the trunk server(指向trunk_server) */
	char last_trunk_server_id[FDFS_STORAGE_ID_MAX_SIZE];		/* 最后一次的trunk server id */

#ifdef WITH_HTTPD
	FDFSStorageDetail **http_servers;  //storages order by ip addr
	int http_server_count; //http server count
	int current_http_server; //current http server index
#endif

	int current_read_server;   /* current read storage server index(当前group下载storage的索引) */
	int current_write_server;  /* current write storage server index(当前group上传storage的索引) */

	int store_path_count;  /* store base path count of each storage server(每个storage的存储路径个数) */

	/* 
	 * subdir_count * subdir_count directories will be auto created
	 * under each store_path (disk) of the storage servers
	 * 二级目录的个数
	 */
	int subdir_count_per_path;

	/* 
	 * row for src storage, col for dest storage
	 * 从源storage server到目标storage server的最后一次同步时间戳
	 * 行代表源storage server，列代表目标storage server
	 */
	int **last_sync_timestamps;

	int chg_count;   /* current group changed count(此group的变换次数) */
	int trunk_chg_count;   /* trunk server changed count(此group的trunk_server的改变次数) */
	time_t last_source_update;  //last source update timestamp
	time_t last_sync_update;    //last synced update timestamp
} FDFSGroupInfo;	/* 单个group信息 */

typedef struct
{
	int alloc_size;   //alloc group count
	int count;  /* 所有正在使用的group的信息数组的元素个数 */
	FDFSGroupInfo **groups;	/* 所有正在使用的group的信息数组 */
	FDFSGroupInfo **sorted_groups; //groups order by group_name
	FDFSGroupInfo *pStoreGroup;  /* the group to store uploaded files(指定某个group上传文件) */
	int current_write_group;  /* current group index to upload file(当前用于上传文件的group在sorted_groups中的索引) */
	byte store_lookup;  //store to which group, from conf file(选择哪一个组上传来上传文件的方式)
	byte store_server;  //store to which storage server, from conf file(上传文件时同一个组中选择哪个storage的方式)
	byte download_server; //download from which storage server, from conf file(下载文件时选择哪个storage的方式)
	byte store_path;  //store to which path, from conf file(上传文件时选择哪一个目录的方式)
	char store_group[FDFS_GROUP_NAME_MAX_LEN + 8];  //for 8 bytes aliginment(如果指定特定的组来上传文件，此为组名)
} FDFSGroups;	/* 总体group信息 */

typedef struct
{
	FDFSGroupInfo *pGroup;		/* 当前连接的storage所在group信息 */
	FDFSStorageDetail *pStorage;	/* 当前连接的storage server */
	union {
		int tracker_leader;  //for notify storage servers
		int trunk_server;    //for notify other tracker servers
	} chg_count;
} TrackerClientInfo;	/* 当前连接的相关信息 */

typedef struct
{
	char name[FDFS_MAX_META_NAME_LEN + 1];  //key
	char value[FDFS_MAX_META_VALUE_LEN + 1]; //value
} FDFSMetaData;	/* 元数据的结构信息 */

typedef struct
{
	int storage_port;
	int storage_http_port;
	int store_path_count;
	int subdir_count_per_path;
	int upload_priority;
	int join_time; //storage join timestamp (create timestamp)
	int up_time;   //storage service started timestamp
        char version[FDFS_VERSION_SIZE];   //storage version
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];	/* storage所属group的name */
        char domain_name[FDFS_DOMAIN_NAME_MAX_SIZE];
        char init_flag;
	signed char status;
	int tracker_count;		/* 配置的tracker server的总数 */
	ConnectionInfo tracker_servers[FDFS_MAX_TRACKERS];	/* storage中配置的所有tracker的连接信息 */
} FDFSStorageJoinBody;	/* 新加入的storage的信息结构 */

typedef struct
{
	int server_count;		/* tracker server的数量 */
	int server_index;  //server index for roundrobin
	int leader_index;  /* leader server index(tracker leader所在位置索引) */
	ConnectionInfo *servers;	/* 其他tracker server的连接信息 */
} TrackerServerGroup;		/* 所有tracker_server的信息 */

typedef struct
{
	char *buffer;  //the buffer pointer
	char *current; /* pointer to current position(当前读取到的位置) */
	int length;    /* the content length(尚未读取内容的长度) */
	int version;   //for binlog pre-read, compare with binlog_write_version
} BinLogBuffer;

typedef struct
{
	char id[FDFS_STORAGE_ID_MAX_SIZE];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 8];  //for 8 bytes alignment
	char ip_addr[IP_ADDRESS_SIZE];
} FDFSStorageIdInfo;	/* storage server基本信息结构 */

typedef struct
{
	char id[FDFS_STORAGE_ID_MAX_SIZE];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char sync_src_id[FDFS_STORAGE_ID_MAX_SIZE];
} FDFSStorageSync;	/* storage同步信息结构 */

typedef struct {
	char flag;	/* 标志 1:指定大小(MB) 2:百分比 */
	union {
		int mb;
		double ratio;
	} rs;
} FDFSStorageReservedSpace;		/* 服务器保留存储空间大小 信息*/

typedef struct {
	int count;   /* store path count(存储目录个数) */
	char **paths; /* file store paths(存储目录路径数组) */
} FDFSStorePaths;	/* storage数据存储目录结构 */

#endif

