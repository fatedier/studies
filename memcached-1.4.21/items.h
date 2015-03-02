/* See items.c */

/* 获得下一个cas_id，加1 */
uint64_t get_cas_id(void);

/*@null@*/
item *do_item_alloc(char *key, const size_t nkey, const int flags, const rel_time_t exptime, const int nbytes, const uint32_t cur_hv);

/* 释放一个item */
void item_free(item *it);

/* 检查指定的item元素总计的大小是否符合要求，没有超过slab页中最大的chunk_size */
bool item_size_ok(const size_t nkey, const int flags, const int nbytes);

/* 将item放入hashtable和LRU队列中 */
int  do_item_link(item *it, const uint32_t hv);     /** may fail if transgresses limits */
void do_item_unlink(item *it, const uint32_t hv);
void do_item_unlink_nolock(item *it, const uint32_t hv);

/* 删除一个item，直到引用计数为0才真正释放该item */
void do_item_remove(item *it);
void do_item_update(item *it);   /** update LRU time to current and reposition */
void do_item_update_nolock(item *it);
int  do_item_replace(item *it, item *new_it, const uint32_t hv);

/*@null@*/
/* 获取指定classid的所有item的相关信息，共获取limit条记录 */
char *do_item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes);
void do_item_stats(ADD_STAT add_stats, void *c);
void do_item_stats_totals(ADD_STAT add_stats, void *c);
/*@null@*/
void do_item_stats_sizes(ADD_STAT add_stats, void *c);
void do_item_flush_expired(void);

/* 根据key值查找item，需要检查是否失效 */
item *do_item_get(const char *key, const size_t nkey, const uint32_t hv);

/* 修改指定key值的item的过期时间 */
item *do_item_touch(const char *key, const size_t nkey, uint32_t exptime, const uint32_t hv);

/* 重置item统计数据 */
void item_stats_reset(void);
extern pthread_mutex_t cache_lock;
void item_stats_evictions(uint64_t *evicted);

enum crawler_result_type {
    CRAWLER_OK=0, CRAWLER_RUNNING, CRAWLER_BADCLASS
};

int start_item_crawler_thread(void);
int stop_item_crawler_thread(void);
int init_lru_crawler(void);
enum crawler_result_type lru_crawler_crawl(char *slabs);
