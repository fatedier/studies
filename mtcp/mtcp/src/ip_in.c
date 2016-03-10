#include <string.h>
#include <netinet/ip.h>

#include "ip_in.h"
#include "tcp_in.h"
#include "mtcp_api.h"
#include "ps.h"
#include "debug.h"
#include "icmp.h"

#define ETH_P_IP_FRAG   0xF800
#define ETH_P_IPV6_FRAG 0xF6DD

/*----------------------------------------------------------------------------*/
// 处理ipv4数据包
// cur_ts: 当前时间
// ifidx: 网卡端口序号
// pkt_data: 数据包mbuf地址
// len: 数据包总长度
inline int 
ProcessIPv4Packet(mtcp_manager_t mtcp, uint32_t cur_ts, 
				  const int ifidx, unsigned char* pkt_data, int len)
{
	/* check and process IPv4 packets */
	// 解析出ip包的首部，pkt_data最开始是以太网帧首部
	struct iphdr* iph = (struct iphdr *)(pkt_data + sizeof(struct ethhdr));
	int ip_len = ntohs(iph->tot_len);

	/* drop the packet shorter than ip header */
	if (ip_len < sizeof(struct iphdr))
		return ERROR;

	// ip数据包校验
	if (ip_fast_csum(iph, iph->ihl))
		return ERROR;

#if !PROMISCUOUS_MODE
	/* if not promiscuous mode, drop if the destination is not myself */
	if (iph->daddr != CONFIG.eths[ifidx].ip_addr)
		//DumpIPPacketToFile(stderr, iph, ip_len);
		return TRUE;
#endif

	// see if the version is correct
	// 检查是否是ipv4包
	if (iph->version != 0x4 ) {
		mtcp->iom->release_pkt(mtcp->ctx, ifidx, pkt_data, len);
		return FALSE;
	}
	
	switch (iph->protocol) {
		case IPPROTO_TCP:
			return ProcessTCPPacket(mtcp, cur_ts, iph, ip_len);
		case IPPROTO_ICMP:
			return ProcessICMPPacket(mtcp, iph, ip_len);
		default:
			/* currently drop other protocols */
			return FALSE;
	}
	return FALSE;
}
/*----------------------------------------------------------------------------*/
