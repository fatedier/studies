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
#include "sched_thread.h"
#include "connection_pool.h"

/* 
 * 初始化连接池
 * connect_timeout:连接超时时间
 * max_count_per_entry:0代表没有限制
 * max_idle_time:连接最大空闲时间
 */
int conn_pool_init(ConnectionPool *cp, int connect_timeout, \
	const int max_count_per_entry, const int max_idle_time)
{
	int result;

	/* 初始化连接池互斥锁 */
	if ((result=init_pthread_lock(&cp->lock)) != 0)
	{
		return result;
	}
	/* 设置超时时间 */
	cp->connect_timeout = connect_timeout;
	/* 设置每个哈希桶的最大容量 */
	cp->max_count_per_entry = max_count_per_entry;
	/* 设置每个连接的最大空闲时间 */
	cp->max_idle_time = max_idle_time;

	/* 初始化哈希表 */
	return hash_init(&(cp->hash_array), simple_hash, 1024, 0.75);
}

/* 销毁连接池资源 */
void conn_pool_destroy(ConnectionPool *cp)
{
	pthread_mutex_lock(&cp->lock);
	hash_destroy(&(cp->hash_array));
	pthread_mutex_unlock(&cp->lock);

	pthread_mutex_destroy(&cp->lock);
}

/* 断开和此ip:port的连接 */
void conn_pool_disconnect_server(ConnectionInfo *pConnection)
{
	if (pConnection->sock >= 0)
	{
		close(pConnection->sock);
		pConnection->sock = -1;
	}
}

/* 建立pConnection所存储的ip和port的连接 */
int conn_pool_connect_server(ConnectionInfo *pConnection, \
		const int connect_timeout)
{
	int result;

	if (pConnection->sock >= 0)
	{
		close(pConnection->sock);
	}

	pConnection->sock = socket(AF_INET, SOCK_STREAM, 0);
	if(pConnection->sock < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"socket create failed, errno: %d, " \
			"error info: %s", __LINE__, errno, STRERROR(errno));
		return errno != 0 ? errno : EPERM;
	}

	/* 设置套接口为非阻塞 */
	if ((result=tcpsetnonblockopt(pConnection->sock)) != 0)
	{
		close(pConnection->sock);
		pConnection->sock = -1;
		return result;
	}

	/* 建立非阻塞连接，超时时间为connect_timeout */
	if ((result=connectserverbyip_nb(pConnection->sock, \
		pConnection->ip_addr, pConnection->port, \
		connect_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"connect to %s:%d fail, errno: %d, " \
			"error info: %s", __LINE__, pConnection->ip_addr, \
			pConnection->port, result, STRERROR(result));

		close(pConnection->sock);
		pConnection->sock = -1;
		return result;
	}

	return 0;
}

/* 根据conn中存储的ip和port，计算出key值 */
static int conn_pool_get_key(const ConnectionInfo *conn, char *key, int *key_len)
{
	struct in_addr sin_addr;

	/* 将ip地址转换成32位整型 */
        if (inet_aton(conn->ip_addr, &sin_addr) == 0)
        {
		*key_len = 0;
                return EINVAL;
        }

	/* 将32整型转换成字符串，且唯一 */
	int2buff(sin_addr.s_addr, key);
	*key_len = 4 + sprintf(key + 4, "%d", conn->port);

	return 0;
}

/* 
 * 获取一个指定ip:port的连接，返回对应的连接信息
 * 如果连接池中不存在，新建对应的ConnectionManager，并且连接指定ip的port
 */
ConnectionInfo *conn_pool_get_connection(ConnectionPool *cp, 
	const ConnectionInfo *conn, int *err_no)
{
	char key[32];
	int key_len;
	int bytes;
	char *p;
	ConnectionManager *cm;
	ConnectionNode *node;
	ConnectionInfo *ci;
	time_t current_time;

	/* 根据conn中存储的ip和port，计算出存储进hash表时的key值 */
	*err_no = conn_pool_get_key(conn, key, &key_len);
	if (*err_no != 0)
	{
		return NULL;
	}

	pthread_mutex_lock(&cp->lock);
	/* 在hash表中查找这个连接对象是否存在，返回ConnectionManager对象指针 */
	cm = (ConnectionManager *)hash_find(&cp->hash_array, key, key_len);
	/* 如果这个ip:port对应的ConnectionManager不存在，新增这个ConnectionManager对象 */
	if (cm == NULL)
	{
		cm = (ConnectionManager *)malloc(sizeof(ConnectionManager));
		if (cm == NULL)
		{
			*err_no = errno != 0 ? errno : ENOMEM;
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, errno: %d, " \
				"error info: %s", __LINE__, \
				(int)sizeof(ConnectionManager), \
				*err_no, STRERROR(*err_no));
			pthread_mutex_unlock(&cp->lock);
			return NULL;
		}

		cm->head = NULL;
		cm->total_count = 0;
		cm->free_count = 0;
		if ((*err_no=init_pthread_lock(&cm->lock)) != 0)
		{
			pthread_mutex_unlock(&cp->lock);
			return NULL;
		}
		hash_insert(&cp->hash_array, key, key_len, cm);
	}
	pthread_mutex_unlock(&cp->lock);

	/* 获取当前时间 */
	current_time = get_current_time();
	pthread_mutex_lock(&cm->lock);
	while (1)
	{
		/* 如果这个ConnectionManager是新增的 */
		if (cm->head == NULL)
		{
			/* 如果超过哈希桶容量上限，报错 */
			if ((cp->max_count_per_entry > 0) && 
				(cm->total_count >= cp->max_count_per_entry))
			{
				*err_no = ENOSPC;
				logError("file: "__FILE__", line: %d, " \
					"connections: %d of server %s:%d " \
					"exceed limit: %d", __LINE__, \
					cm->total_count, conn->ip_addr, \
					conn->port, cp->max_count_per_entry);
				pthread_mutex_unlock(&cm->lock);
				return NULL;
			}

			/* 一次性分配ConnectionInfo和ConnectionNode两个对象的空间 */
			bytes = sizeof(ConnectionInfo) + sizeof(ConnectionNode);
			p = (char *)malloc(bytes);
			if (p == NULL)
			{
				*err_no = errno != 0 ? errno : ENOMEM;
				logError("file: "__FILE__", line: %d, " \
					"malloc %d bytes fail, errno: %d, " \
					"error info: %s", __LINE__, \
					bytes, *err_no, STRERROR(*err_no));
				pthread_mutex_unlock(&cm->lock);
				return NULL;
			}

			/* 新增一个ConnectionNode节点插入到所在ConnectionManager的头部 */
			node = (ConnectionNode *)(p + sizeof(ConnectionInfo));
			node->conn = (ConnectionInfo *)p;
			node->manager = cm;
			node->next = NULL;
			node->atime = 0;

			cm->total_count++;
			pthread_mutex_unlock(&cm->lock);

			memcpy(node->conn, conn, sizeof(ConnectionInfo));
			node->conn->sock = -1;

			/* 建立pConnection所存储的ip和port的连接 */
			*err_no = conn_pool_connect_server(node->conn, \
					cp->connect_timeout);
			if (*err_no != 0)
			{
				free(p);
				return NULL;
			}

			logDebug("file: "__FILE__", line: %d, " \
				"server %s:%d, new connection: %d, " \
				"total_count: %d, free_count: %d",   \
				__LINE__, conn->ip_addr, conn->port, \
				node->conn->sock, cm->total_count, \
				cm->free_count);
			return node->conn;
		}
		/* 如果之前就已经根据key值查找到所在ConnectionManager */
		else
		{
			/* 从连接池中获取一个可用的节点 */
			node = cm->head;
			ci = node->conn;
			cm->head = node->next;
			cm->free_count--;

			/* 如果超过了最大空闲时间 */
			if (current_time - node->atime > cp->max_idle_time)
			{
				cm->total_count--;

				logDebug("file: "__FILE__", line: %d, " \
					"server %s:%d, connection: %d idle " \
					"time: %d exceeds max idle time: %d, "\
					"total_count: %d, free_count: %d", \
					__LINE__, conn->ip_addr, conn->port, \
					ci->sock, \
					(int)(current_time - node->atime), \
					cp->max_idle_time, cm->total_count, \
					cm->free_count);

				/* 断开和server的连接 */
				conn_pool_disconnect_server(ci);
				free(ci);
				continue;
			}

			pthread_mutex_unlock(&cm->lock);
			logDebug("file: "__FILE__", line: %d, " \
				"server %s:%d, reuse connection: %d, " \
				"total_count: %d, free_count: %d", 
				__LINE__, conn->ip_addr, conn->port, 
				ci->sock, cm->total_count, cm->free_count);
			return ci;
		}
	}
}

/* 
 * 关闭和指定ip:port之间的连接 
 * bForce为true，则强制断开连接，否则free_count加1，并不真的断开socket连接
 */
int conn_pool_close_connection_ex(ConnectionPool *cp, ConnectionInfo *conn, 
	const bool bForce)
{
	char key[32];
	int result;
	int key_len;
	ConnectionManager *cm;
	ConnectionNode *node;

	/* 计算生成的hash表的key值 */
	result = conn_pool_get_key(conn, key, &key_len);
	if (result != 0)
	{
		return result;
	}

	pthread_mutex_lock(&cp->lock);
	/* 查找该key值对应的ConnectionManager对象是否存在 */
	cm = (ConnectionManager *)hash_find(&cp->hash_array, key, key_len);
	pthread_mutex_unlock(&cp->lock);
	if (cm == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"hash entry of server %s:%d not exist", __LINE__, \
			conn->ip_addr, conn->port);
		return ENOENT;
	}

	/* 获取该ConnectionInfo所在ConnectionNode是否对应之前计算出的ConnectionManager对象 */
	node = (ConnectionNode *)(((char *)conn) + sizeof(ConnectionInfo));
	if (node->manager != cm)
	{
		logError("file: "__FILE__", line: %d, " \
			"manager of server entry %s:%d is invalid!", \
			__LINE__, conn->ip_addr, conn->port);
		return EINVAL;
	}

	pthread_mutex_lock(&cm->lock);
	/* 直接断开连接 */
	if (bForce)
	{
		cm->total_count--;

		logDebug("file: "__FILE__", line: %d, " \
			"server %s:%d, release connection: %d, " \
			"total_count: %d, free_count: %d", 
			__LINE__, conn->ip_addr, conn->port, 
			conn->sock, cm->total_count, cm->free_count);

		/* 断开和此ip:port的连接 */
		conn_pool_disconnect_server(conn);
		free(conn);
	}
	else
	{
		/* 将此节点加入到空闲节点的链表中，并且free_count加1 */
		node->atime = get_current_time();
		node->next = cm->head;
		cm->head = node;
		cm->free_count++;

		logDebug("file: "__FILE__", line: %d, " \
			"server %s:%d, free connection: %d, " \
			"total_count: %d, free_count: %d", 
			__LINE__, conn->ip_addr, conn->port, 
			conn->sock, cm->total_count, cm->free_count);
	}
	pthread_mutex_unlock(&cm->lock);

	return 0;
}

/* 计算一个链表中的节点数 */
static int _conn_count_walk(const int index, const HashData *data, void *args)
{
	int *count;
	ConnectionManager *cm;
	ConnectionNode *node;

	count = (int *)args;
	cm = (ConnectionManager *)data->value;
	node = cm->head;
	while (node != NULL)
	{
		(*count)++;
		node = node->next;
	}

	return 0;
}

/* 获取连接池中的所有节点数 */
int conn_pool_get_connection_count(ConnectionPool *cp)
{
	int count;
	count = 0;
	hash_walk(&cp->hash_array, _conn_count_walk, &count);
	return count;
}

