/* associative array */

/* 初始化hash表 */
void assoc_init(const int hashpower_init);

/* 在hash表中查找key对应的item */
item *assoc_find(const char *key, const size_t nkey, const uint32_t hv);

/* 向hash表中插入一个item，如果超过桶数的1.5倍，进行扩展  */
int assoc_insert(item *item, const uint32_t hv);

/* 在hash表中删除指定key值对应的item */
void assoc_delete(const char *key, const size_t nkey, const uint32_t hv);

void do_assoc_move_next_bucket(void);

/* 启动hash表扩展线程 */
int start_assoc_maintenance_thread(void);

/* 停止hash表扩展线程 */
void stop_assoc_maintenance_thread(void);

/* hash桶个数的2的指数值 */
extern unsigned int hashpower;
