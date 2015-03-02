
#ifndef AVL_TREE_H
#define AVL_TREE_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "common_define.h"

typedef struct tagAVLTreeNode {
	void *data;					/* 数据 */
	struct tagAVLTreeNode *left;	/* 左子树 */
	struct tagAVLTreeNode *right;	/* 右子树 */
	byte balance;				/* 平衡因子，右子树深度减去左子树深度的值 */
} AVLTreeNode;	/* 平衡二叉树节点 */

typedef int (*DataOpFunc) (void *data, void *args);

typedef struct tagAVLTreeInfo {
	AVLTreeNode *root;			/* 根节点 */
	FreeDataFunc free_data_func;	/* 释放某个节点的函数指针 */
	CompareFunc compare_func;	/* 比较元素大小的函数指针 */
} AVLTreeInfo;		/* 平衡二叉树信息 */

#ifdef __cplusplus
extern "C" {
#endif

/* 初始化AVL子树 */
int avl_tree_init(AVLTreeInfo *tree, FreeDataFunc free_data_func, \
	CompareFunc compare_func);
/* 递归销毁平衡二叉树 */
void avl_tree_destroy(AVLTreeInfo *tree);

/* 向平衡二叉树中插入一个数据 */
int avl_tree_insert(AVLTreeInfo *tree, void *data);
int avl_tree_replace(AVLTreeInfo *tree, void *data);
/* 在AVL树中删除一个节点 */
int avl_tree_delete(AVLTreeInfo *tree, void *data);
/* 在AVL树中查找某个节点 */
void *avl_tree_find(AVLTreeInfo *tree, void *target_data);
/* 递归查找和target_data一样的值，如果没有，就返回第一个稍大一些的值 */
void *avl_tree_find_ge(AVLTreeInfo *tree, void *target_data);
/* 中序遍历avl树的每一个节点，执行data_op_func回调函数 */
int avl_tree_walk(AVLTreeInfo *tree, DataOpFunc data_op_func, void *args);
/* 返回AVL树的节点个数 */
int avl_tree_count(AVLTreeInfo *tree);
/*计算AVL树的深度 */
int avl_tree_depth(AVLTreeInfo *tree);
//void avl_tree_print(AVLTreeInfo *tree);

#ifdef __cplusplus
}
#endif

#endif
