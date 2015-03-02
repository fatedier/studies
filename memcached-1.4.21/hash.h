#ifndef HASH_H
#define    HASH_H

/* hash函数指针声明 */
typedef uint32_t (*hash_func)(const void *key, size_t length);
hash_func hash;

enum hashfunc_type {
    JENKINS_HASH=0, MURMUR3_HASH
};	/* hash函数的类型 */

/* hash初始化，指定使用的hash函数 */
int hash_init(enum hashfunc_type type);

#endif    /* HASH_H */

