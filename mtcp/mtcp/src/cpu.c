#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <numa.h>
#include <sched.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <assert.h>
#include "mtcp_api.h"

#define MAX_FILE_NAME 1024

/*----------------------------------------------------------------------------*/
// 获取当前的cpu总数
int 
GetNumCPUs() 
{
	// 获取当前的cpu总数
	return sysconf(_SC_NPROCESSORS_ONLN);
}
/*----------------------------------------------------------------------------*/
// 获取当前线程的tid
pid_t 
Gettid()
{
	// 获取当前线程的tid
	return syscall(__NR_gettid);
}
/*----------------------------------------------------------------------------*/
// 绑定当前线程到指定id的cpu
int 
mtcp_core_affinitize(int cpu)
{
	cpu_set_t cpus;	//是一个掩码数组，一共有1024位，每一位都可以对应一个cpu核心  
	struct bitmask *bmask;
	FILE *fp;
	char sysfname[MAX_FILE_NAME];
	int phy_id;
	size_t n;
	int ret;
	int unused;

	n = GetNumCPUs();

	if (cpu < 0 || cpu >= (int) n) {
		errno = -EINVAL;
		return -1;
	}

	// 所有位置0
	CPU_ZERO(&cpus);
	// 设置需要绑定的cpu id
	CPU_SET((unsigned)cpu, &cpus);

	// 绑定当前线程tid到指定的cpu
	ret = sched_setaffinity(Gettid(), sizeof(cpus), &cpus);

	// 返回当前系统中的numa node个数，如果没有，直接返回
	if (numa_max_node() == 0)
		return ret;

	bmask = numa_bitmask_alloc(n);
	assert(bmask);

	/* read physical id of the core from sys information */
	snprintf(sysfname, MAX_FILE_NAME - 1, 
			"/sys/devices/system/cpu/cpu%d/topology/physical_package_id", cpu);
	fp = fopen(sysfname, "r");
	if (!fp) {
		perror(sysfname);
		errno = EFAULT;
		return -1;
	}
	unused = fscanf(fp, "%d", &phy_id);

	// 设置memory的亲和性
	numa_bitmask_setbit(bmask, phy_id);
	numa_set_membind(bmask);
	numa_bitmask_free(bmask);

	fclose(fp);

	UNUSED(unused);
	return ret;
}
