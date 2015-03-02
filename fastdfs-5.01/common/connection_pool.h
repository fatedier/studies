/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//connection_pool.h
/* 连接池 */
#ifndef _CONNECTION_POOL_H
#define _CONNECTION_POOL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "common_define.h"
#include "pthread_func.h"
#include "hash.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
	int sock;		/* socket描述符 */
	int port;		/* 端口号 */
	char ip_addr[IP_ADDRESS_SIZE];	/* ip地址 */
} ConnectionInfo;	/* 连接信息 */

struct tagConnectionManager;

typedef struct tagConnectionNode {
	ConnectionInfo *conn;		/* 连接信息 */
	struct tagConnectionManager *manager;		/* 连接管理对象 */
	struct tagConnectionNode *next;
	time_t atime;  /* last access time(最近一次连接时间) */
} ConnectionNode;		/* 连接对象节点 */

typedef struct tagConnectionManager {
	ConnectionNode *head;	/* 连接对象的链表 */
	int total_count;  /* total connections(创建节点的总数) */
	int free_count;   /* free connections(当前空闲节点的总数) */
	pthread_mutex_t lock;
} ConnectionManager;		/* 连接管理对象 */

typedef struct tagConnectionPool {
	HashArray hash_array;  /* key is ip:port, value is ConnectionManager */
	pthread_mutex_t lock;
	int connect_timeout;		/* 连接超时时间 */
	int max_count_per_entry;  /* 0 means no limit，每个hash桶的最大数量 */

	/*
	connections whose the idle time exceeds this time will be closed
	*/
	int max_idle_time;	/* 最大空闲时间 */
} ConnectionPool;	/* 连接池 */


/* 初始化连接池 */
int conn_pool_init(ConnectionPool *cp, int connect_timeout, \
	const int max_count_per_entry, const int max_idle_time);

/* 销毁连接池资源 */
void conn_pool_destroy(ConnectionPool *cp);

/* 
 * 获取一个指定ip:port的连接，返回对应的连接信息
 * 如果连接池中不存在，新建对应的ConnectionManager，并且连接指定ip的port
 */
ConnectionInfo *conn_pool_get_connection(ConnectionPool *cp, 
	const ConnectionInfo *conn, int *err_no);

/* 释放连接资源，将此节点加入空闲节点链表中，free_count加1 */
#define conn_pool_close_connection(cp, conn) \
	conn_pool_close_connection_ex(cp, conn, false)

/* 
 * 关闭和指定ip:port之间的连接 
 * bForce为true，则强制断开连接，否则free_count加1，并不真的断开socket连接
 */
int conn_pool_close_connection_ex(ConnectionPool *cp, ConnectionInfo *conn, 
	const bool bForce);

/* 断开和此ip:port的连接 */
void conn_pool_disconnect_server(ConnectionInfo *pConnection);

/* 建立pConnection所存储的ip和port的连接 */
int conn_pool_connect_server(ConnectionInfo *pConnection, \
		const int connect_timeout);

/* 获取连接池中的所有节点数 */
int conn_pool_get_connection_count(ConnectionPool *cp);

#ifdef __cplusplus
}
#endif

#endif

