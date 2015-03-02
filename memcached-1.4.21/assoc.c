/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 *
 * The rest of the file is licensed under the BSD license.  See LICENSE.
 */

#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;


typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;   /* unsigned 1-byte quantities */

/* how many powers of 2's worth of buckets we use */
unsigned int hashpower = HASHPOWER_DEFAULT;		/* hash桶个数的2的指数值 */

#define hashsize(n) ((ub4)1<<(n))			/* hash桶的总个数 */
#define hashmask(n) (hashsize(n)-1)		/* hash桶数量的掩码 */

/* Main hash table. This is where we look except during expansion. */
static item** primary_hashtable = 0;	/* hash表结构 */

/*
 * Previous hash table. During expansion, we look here for keys that haven't
 * been moved over to the primary yet.
 */
static item** old_hashtable = 0;	/* hash扩展时旧的hash表数据 */

/* Number of items in the hash table. */
static unsigned int hash_items = 0;		/* hash表中总元素个数 */

/* Flag: Are we in the middle of expanding now? */
static bool expanding = false;			/* hash表是否正在进行扩展 */
static bool started_expanding = false;

/*
 * During expansion we migrate values with bucket granularity; this is how
 * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
 * 扩充过程中，桶迁移到新hash表的进度，值的范围是0 .. hashsize(hashpower - 1) - 1
 */
static unsigned int expand_bucket = 0;

/* 初始化hash表 */
void assoc_init(const int hashtable_init) {
    if (hashtable_init) {
        hashpower = hashtable_init;
    }
    /* 分配空间并全部初始化为0 */
    primary_hashtable = calloc(hashsize(hashpower), sizeof(void *));
    if (! primary_hashtable) {
        fprintf(stderr, "Failed to init hashtable.\n");
        exit(EXIT_FAILURE);
    }
    STATS_LOCK();
    stats.hash_power_level = hashpower;
    stats.hash_bytes = hashsize(hashpower) * sizeof(void *);
    STATS_UNLOCK();
}

/* 在hash表中查找key对应的item */
item *assoc_find(const char *key, const size_t nkey, const uint32_t hv) {
    item *it;
    unsigned int oldbucket;

    /* 
     * 如果正在扩展中，判断要查找的内容在旧的hash表还是新的中 
     * 查找是在哪一个hash桶中
     */
    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it = old_hashtable[oldbucket];
    } else {
        it = primary_hashtable[hv & hashmask(hashpower)];
    }

    item *ret = NULL;
    int depth = 0;
    while (it) {
	 /* 遍历这个hash桶，查找key值相同的item */
        if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0)) {
            ret = it;
            break;
        }
        it = it->h_next;
        ++depth;
    }
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return ret;
}

/* 
 * returns the address of the item pointer before the key.  if *item == 0,
 * the item wasn't found 
 * 返回指向指定key值对应item指针，不存在返回NULL
 */
static item** _hashitem_before (const char *key, const size_t nkey, const uint32_t hv) {
    item **pos;
    unsigned int oldbucket;

    /* 寻找所在hash桶 */
    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        pos = &old_hashtable[oldbucket];
    } else {
        pos = &primary_hashtable[hv & hashmask(hashpower)];
    }

    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

/* grows the hashtable to the next power of 2. */
/* 对hash表进行扩展，2的指数级扩展 */
static void assoc_expand(void) {
    old_hashtable = primary_hashtable;

    primary_hashtable = calloc(hashsize(hashpower + 1), sizeof(void *));
    if (primary_hashtable) {
        if (settings.verbose > 1)
            fprintf(stderr, "Hash table expansion starting\n");
        hashpower++;
        expanding = true;
        expand_bucket = 0;
        STATS_LOCK();
        stats.hash_power_level = hashpower;
        stats.hash_bytes += hashsize(hashpower) * sizeof(void *);
        stats.hash_is_expanding = 1;
        STATS_UNLOCK();
    } else {
        primary_hashtable = old_hashtable;
        /* Bad news, but we can keep running. */
    }
}

/* 开始进行hash表扩展，唤醒maintenace线程进行扩展 */
static void assoc_start_expand(void) {
    if (started_expanding)
        return;
    started_expanding = true;
    pthread_cond_signal(&maintenance_cond);
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
/* 向hash表中插入一个item，如果超过桶数的1.5倍，进行扩展  */
int assoc_insert(item *it, const uint32_t hv) {
    unsigned int oldbucket;

//    assert(assoc_find(ITEM_key(it), it->nkey) == 0);  /* shouldn't have duplicately named things defined */

    /* 寻找此item的插入位置 */
    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it->h_next = old_hashtable[oldbucket];
        old_hashtable[oldbucket] = it;
    } else {
        it->h_next = primary_hashtable[hv & hashmask(hashpower)];
        primary_hashtable[hv & hashmask(hashpower)] = it;
    }

    hash_items++;
    /* 如果不在扩展中，并且总数超过hash桶数的1.5倍，开始进行扩展 */
    if (! expanding && hash_items > (hashsize(hashpower) * 3) / 2) {
        assoc_start_expand();
    }

    MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey, hash_items);
    return 1;
}

/* 在hash表中删除指定key值对应的item */
void assoc_delete(const char *key, const size_t nkey, const uint32_t hv) {
    /* 找到key所在hash桶的链表中的位置 */
    item **before = _hashitem_before(key, nkey, hv);

    if (*before) {
        item *nxt;
        hash_items--;
        /* The DTrace probe cannot be triggered as the last instruction
         * due to possible tail-optimization by the compiler
         */
        MEMCACHED_ASSOC_DELETE(key, nkey, hash_items);
	 /* 在链表中去除该节点 */
        nxt = (*before)->h_next;
        (*before)->h_next = 0;   /* probably pointless, but whatever. */
        *before = nxt;
        return;
    }
    /* Note:  we never actually get here.  the callers don't delete things
       they can't find. */
    assert(*before != 0);
}


static volatile int do_run_maintenance_thread = 1;

#define DEFAULT_HASH_BULK_MOVE 1
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;	/* 每次迁移多少个hash桶，这一过程中会加锁 */

/* hash表扩展线程主函数 */
static void *assoc_maintenance_thread(void *arg) {

    while (do_run_maintenance_thread) {
        int ii = 0;

        /* Lock the cache, and bulk move multiple buckets to the new
         * hash table. */
        item_lock_global();
        mutex_lock(&cache_lock);

        for (ii = 0; ii < hash_bulk_move && expanding; ++ii) {
            item *it, *next;
            int bucket;

	     /* 迁移一个hash桶的所有节点 */
            for (it = old_hashtable[expand_bucket]; NULL != it; it = next) {
                next = it->h_next;

		  /* 计算旧的节点在新的hash桶中的位置 */
                bucket = hash(ITEM_key(it), it->nkey) & hashmask(hashpower);
		  /* 将old_hashtable中此hash桶中的节点加入到primary_hashtable中 */
                it->h_next = primary_hashtable[bucket];
                primary_hashtable[bucket] = it;
            }

	     /* 旧的hash桶清空 */
            old_hashtable[expand_bucket] = NULL;

	     /* 准备迁移下一个hash桶 */
            expand_bucket++;
	     /* 如果迁移完成，expand_bucket个数等于旧的hash表的桶的数量 */
            if (expand_bucket == hashsize(hashpower - 1)) {
                expanding = false;
                free(old_hashtable);
		  /* 修改hash表相关的状态信息 */
                STATS_LOCK();
                stats.hash_bytes -= hashsize(hashpower - 1) * sizeof(void *);		/* 减去旧的hash表占用的空间 */
                stats.hash_is_expanding = 0;
                STATS_UNLOCK();
                if (settings.verbose > 1)
                    fprintf(stderr, "Hash table expansion done\n");
            }
        }

        mutex_unlock(&cache_lock);
        item_unlock_global();

	 /* 扩展完成 */
        if (!expanding) {
            /* 
              * finished expanding. tell all threads to use fine-grained locks 
		* 切换锁的类型，通过管道发送到各个工作线程
		*/
            switch_item_lock_type(ITEM_LOCK_GRANULAR);
	     /* 恢复slab平衡线程 */
            slabs_rebalancer_resume();
		 
            /* We are done expanding.. just wait for next invocation */
            mutex_lock(&cache_lock);
            started_expanding = false;
	
	     /* 等待在条件变量上，接收到信号后进行下一次扩展 */
            pthread_cond_wait(&maintenance_cond, &cache_lock);
            /* Before doing anything, tell threads to use a global lock */
            mutex_unlock(&cache_lock);

	     /* 暂停slab平衡线程 */
            slabs_rebalancer_pause();
	     /* 切换到全局锁 */
            switch_item_lock_type(ITEM_LOCK_GLOBAL);

	     /* 对hash表进行扩展，2的指数级增长 */
            mutex_lock(&cache_lock);
            assoc_expand();
            mutex_unlock(&cache_lock);
        }
    }
    return NULL;
}

static pthread_t maintenance_tid;	/* hash表扩展线程tid */

/* 启动hash表扩展线程 */
int start_assoc_maintenance_thread() {
    int ret;
    char *env = getenv("MEMCACHED_HASH_BULK_MOVE");
    if (env != NULL) {
        hash_bulk_move = atoi(env);
        if (hash_bulk_move == 0) {
            hash_bulk_move = DEFAULT_HASH_BULK_MOVE;
        }
    }
    if ((ret = pthread_create(&maintenance_tid, NULL,
                              assoc_maintenance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        return -1;
    }
    return 0;
}

/* 停止hash表扩展线程 */
void stop_assoc_maintenance_thread() {
    mutex_lock(&cache_lock);
    do_run_maintenance_thread = 0;
    pthread_cond_signal(&maintenance_cond);
    mutex_unlock(&cache_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(maintenance_tid, NULL);
}


