#ifndef __CONFIG_H_
#define __CONFIG_H_

#include "ps.h"

int num_cpus;		// 可用的cpu核数
int num_queues;		// 网卡队列数
int num_devices;	// 网卡端口数

int num_devices_attached;			// 绑定的网卡端口数
int devices_attached[MAX_DEVICES];	// 绑定的网卡端口序号

int 
LoadConfiguration(char *fname);

/* set configurations from the setted 
   interface information */
int
SetInterfaceInfo();

/* set configurations from the files */
int 
SetRoutingTable();

int 
LoadARPTable();

/* print setted configuration */
void 
PrintConfiguration();

void 
PrintInterfaceInfo();

void 
PrintRoutingTable();

/* set socket modes */
int
SetSocketMode(int8_t socket_mode);

/* fetch mask from prefix */
uint32_t 
MaskFromPrefix(int prefix);

void
ParseMACAddress(unsigned char *haddr, char *haddr_str);

int 
ParseIPAddress(uint32_t *ip_addr, char *ip_str);

#endif /* __CONFIG_H_ */
