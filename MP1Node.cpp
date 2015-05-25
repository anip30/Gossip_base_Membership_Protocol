/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

int K = 3;

bool compare_func_for_sort_memberlist(const MemberListEntry &a, const MemberListEntry &b)
{
	if(a.id<b.id)
	{
		return true;
	}
	else if(a.id==b.id)
	{
		return a.port < b.port;
	}
	else
		return false;
}

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
	this->pos_in_memberlist=0;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */

//	int id = *(int*)(&memberNode->addr.addr);
//	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = 0;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	char *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

	// if the itroducer itself then just add to membership list
    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
    	MemberListEntry member_entry(1,0,memberNode->heartbeat,par->getcurrtime());

        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->memberList.push_back(member_entry);
        memberNode->inGroup = true;

    }
    else {
		
    	int msg_type=JOINREQ;
    	size_t msgsize = sizeof(int) + sizeof(long) + sizeof(joinaddr->addr);
    	msg=(char *)malloc(msgsize*sizeof(char));

    	memset(msg,0x00,msgsize);
    	memcpy(msg,&msg_type,sizeof(int));
    	memcpy(msg+sizeof(int),memberNode->addr.addr,sizeof(memberNode->addr.addr));
    	memcpy(msg+sizeof(int)+sizeof(memberNode->addr.addr),&(memberNode->heartbeat),sizeof(long));
//    	printAddress(&memberNode->addr);

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        free(msg);
    }
    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

	//update current timestamp of current node and increment heartbeat counter
    memberNode->memberList[pos_in_memberlist].timestamp=par->getcurrtime();
    memberNode->memberList[pos_in_memberlist].heartbeat++;

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/*
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */

	int message_type=0,resp_msg_size=0,i,j;
	int total_member;
	char *resp_msg;
	Address temp_addr;
	Address joinaddr;
	joinaddr = getJoinAddress();
	long cnt=0;
	bool flag=false;
	std::vector<MemberListEntry>::iterator it;
	std::vector<MemberListEntry> heartbeat_member;

	std::list<int> member_to_add;
	std::list<int>::iterator it2;
	MemberListEntry new_member;
	int id;
	short port;

	memcpy((char *)&message_type,data,sizeof(int));
	switch (message_type)
	{
	case JOINREQ:
		// if join req then just send whole memberlist to reqeusted node
		memcpy(temp_addr.addr,data+sizeof(int),sizeof(temp_addr.addr));
		memcpy((char *)&cnt,data+sizeof(int)+sizeof(temp_addr.addr),sizeof(long));
		id = *(int*)(&temp_addr.addr);
		 port = *(short*)(&temp_addr.addr[4]);
//		 cout<<" heartbeat count "<<cnt<<endl;
//		 cout<<"id "<<id<<" port "<<port;
		 new_member.setid(id);
		 new_member.setport(port);
		 new_member.setheartbeat(cnt);
		 new_member.settimestamp(par->getcurrtime());
			log->logNodeAdd(&joinaddr,&temp_addr);
		 memberNode->memberList.push_back(new_member);
		 std::sort(memberNode->memberList.begin(),memberNode->memberList.end(),compare_func_for_sort_memberlist);
//		  std::cout << "\n\n ====== vector size is "<<memberNode->memberList.size()<<endl;
//		  for (it=memberNode->memberList.begin(); it<memberNode->memberList.end(); it++)
//		  {
//			  std::cout << " id is  " << (MemberListEntry*)it->id<< " port : "<<(MemberListEntry*)it->port <<" hearbeatcout  "<<(MemberListEntry*)it->heartbeat <<" timepstamp  "<< (MemberListEntry*)it->timestamp<<endl;
//
		 resp_msg_size=sizeof(int) + sizeof(MemberListEntry)*memberNode->memberList.size();
		 resp_msg=(char *)malloc(sizeof(char)*resp_msg_size);
		 memset(resp_msg,0x00,resp_msg_size);
		 message_type=1;
		 memcpy(resp_msg,&message_type,sizeof(int));
		 memcpy(resp_msg+sizeof(int),memberNode->memberList.data(),resp_msg_size);
		 emulNet->ENsend(&joinaddr,&temp_addr,resp_msg,resp_msg_size);
		 free(resp_msg);
		 break;
	case JOINREP:
	
		//Membership list received so update own membership list.
	
		id = *(int*)(memberNode->addr.addr);
		port = *(short*)(&memberNode->addr.addr[4]);
//		cout<<"size is" << size<<endl;
		total_member=(size-sizeof(int))/sizeof(MemberListEntry);


		if(memberNode->memberList.size()==0)
		{
		for(int i=0;i<total_member;i++)
		{

			memcpy(&new_member,data+sizeof(int)+i*sizeof(MemberListEntry),sizeof(MemberListEntry));
			if(id==new_member.id)
				pos_in_memberlist=i;
//			std::cout << " id is  " << new_member.id<< " port : "<<new_member.port <<" hearbeatcout  "<<new_member.heartbeat <<" timepstamp  "<< new_member.timestamp<<endl;
			memberNode->memberList.push_back(new_member);
			memcpy(&temp_addr.addr[0],&new_member.id,sizeof(int));
			memcpy(&temp_addr.addr[4],&new_member.port,sizeof(short));
			log->logNodeAdd(&memberNode->addr,&temp_addr);

		}
		}
		else
		{
			for(int i=0;i<total_member;i++)
			{
				memcpy(&new_member,data+sizeof(int)+i*sizeof(MemberListEntry),sizeof(MemberListEntry));
	//			std::cout << " id is  " << new_member.id<< " port : "<<new_member.port <<" hearbeatcout  "<<new_member.heartbeat <<" timepstamp  "<< new_member.timestamp<<endl;
				heartbeat_member.push_back(new_member);
			}
			std::sort(heartbeat_member.begin(),heartbeat_member.end(),compare_func_for_sort_memberlist);

			for(i=0;i<total_member;i++)
			{
				for(j=0;j<memberNode->memberList.size();j++)
				{
					if(heartbeat_member[i].id == memberNode->memberList[j].id)
					{
						if( (memberNode->memberList[j].timestamp>=0) &&  (heartbeat_member[i].timestamp>=0) && (heartbeat_member[i].heartbeat > memberNode->memberList[j].heartbeat))
						{
							memberNode->memberList[j].heartbeat=heartbeat_member[i].heartbeat;
							memberNode->memberList[j].timestamp=par->getcurrtime();
						}
						flag=true;
					}
				}
				if(!flag && (heartbeat_member[i].timestamp>=0))
					member_to_add.push_back(i);
				flag=false;
			}
			for(it2=member_to_add.begin();it2!=member_to_add.end();it2++)
			{
				memcpy(&temp_addr.addr[0],&heartbeat_member[*it2].id,sizeof(int));
				memcpy(&temp_addr.addr[4],&heartbeat_member[*it2].port,sizeof(short));
				log->logNodeAdd(&memberNode->addr,&temp_addr);

					heartbeat_member[*it2].timestamp=par->getcurrtime();
					memberNode->memberList.push_back(heartbeat_member[*it2]);
			}
			std::sort(memberNode->memberList.begin(),memberNode->memberList.end(),compare_func_for_sort_memberlist);
			for(j=0;j<memberNode->memberList.size();j++)
			{
				if(id==memberNode->memberList[j].id)
					pos_in_memberlist=j;
			}
		}

//		for (it=memberNode->memberList.begin(); it!=memberNode->memberList.end(); it++)
//		{
//			std::cout << " id is  " << (MemberListEntry*)it->id<< " port : "<<(MemberListEntry*)it->port <<" hearbeatcout  "<<(MemberListEntry*)it->heartbeat <<" timepstamp  "<< (MemberListEntry*)it->timestamp<<endl;
//		}

		memberNode->inGroup = true;
		break;
	case HEARTBEAT:
	
		//received heartbeat so update current membership list
	
		id = *(int*)(memberNode->addr.addr);
		 port = *(short*)(&memberNode->addr.addr[4]);

		total_member=(size-sizeof(int))/sizeof(MemberListEntry);

		for(int i=0;i<total_member;i++)
		{
			memcpy(&new_member,data+sizeof(int)+i*sizeof(MemberListEntry),sizeof(MemberListEntry));
//		std::cout << "**** id is  " << new_member.id<< " port : "<<new_member.port <<" hearbeatcout  "<<new_member.heartbeat <<" timepstamp  "<< new_member.timestamp<<endl;
			heartbeat_member.push_back(new_member);
		}
		std::sort(heartbeat_member.begin(),heartbeat_member.end(),compare_func_for_sort_memberlist);

		for(i=0;i<total_member;i++)
		{
			for(j=0;j<memberNode->memberList.size();j++)
			{
				if(heartbeat_member[i].id == memberNode->memberList[j].id)
				{

					if( (memberNode->memberList[j].timestamp>=0) &&  (heartbeat_member[i].timestamp>=0) && (heartbeat_member[i].heartbeat > memberNode->memberList[j].heartbeat))
					{
						memberNode->memberList[j].heartbeat=heartbeat_member[i].heartbeat;
						memberNode->memberList[j].timestamp=par->getcurrtime();
					}
					flag=true;
				}
			}
			if(!flag && (heartbeat_member[i].timestamp>=0))
				member_to_add.push_back(i);
			flag=false;
		}

		for(it2=member_to_add.begin();it2!=member_to_add.end();it2++)
		{
				memcpy(&temp_addr.addr[0],&heartbeat_member[*it2].id,sizeof(int));
				memcpy(&temp_addr.addr[4],&heartbeat_member[*it2].port,sizeof(short));
				log->logNodeAdd(&memberNode->addr,&temp_addr);
				heartbeat_member[*it2].timestamp=par->getcurrtime();
				memberNode->memberList.push_back(heartbeat_member[*it2]);
		}
		std::sort(memberNode->memberList.begin(),memberNode->memberList.end(),compare_func_for_sort_memberlist);


		for(j=0;j<memberNode->memberList.size();j++)
		{
			if(id==memberNode->memberList[j].id)
				pos_in_memberlist=j;
		}

//		cout<<" after heartbeat at ";
//		printAddress(&memberNode->addr);
//
//				for (it=memberNode->memberList.begin(); it!=memberNode->memberList.end(); it++)
//				{
//					std::cout << " id is  " << (MemberListEntry*)it->id<< " port : "<<(MemberListEntry*)it->port <<" hearbeatcout  "<<(MemberListEntry*)it->heartbeat <<" timepstamp  "<< (MemberListEntry*)it->timestamp<<endl;
//				}

		break;
	default:
		cout<<"default case";
	}
	return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	std::set<int> random_gossip_target;
	std::set<int>::iterator it;
	std::list<int> node_to_delete;
	std::list<int>::iterator delete_list_itr;
	int current_time=par->getcurrtime();
	char *heartbeat_msg;
	int i,heartbeat_msg_size=0,message_type;
	int rand_member=0,temp_num=0;
	Address dest_addr,dead_addr;

	int id = *(int*)(memberNode->addr.addr);


	//first check timestamp of all nodes in membershiplist. mark nodes with tfail.

	node_to_delete.clear();
	for (i=0; i<memberNode->memberList.size(); i++)
	{
		if (memberNode->memberList[i].timestamp > 0)
		{
			if(current_time - memberNode->memberList[i].timestamp  >=TFAIL)
			{
				memberNode->memberList[i].timestamp = - memberNode->memberList[i].timestamp;
			}
		}
		else //if greater then tfail+tremove then node is dead so remove the node from membership list
		{
			if(current_time - abs(memberNode->memberList[i].timestamp) >=TFAIL+TREMOVE)
			{
				node_to_delete.push_back(i);
			}
		}
//		std::cout << " id is  " << (MemberListEntry*)it->id<< " port : "<<(MemberListEntry*)it->port <<" hearbeatcout  "<<(MemberListEntry*)it->heartbeat <<" timepstamp  "<< (MemberListEntry*)it->timestamp<<endl;
	}

//		cout<<"\n\n ===== total node to delete "<<node_to_delete.size()<<endl;


		for(delete_list_itr=node_to_delete.begin();delete_list_itr!=node_to_delete.end();delete_list_itr++)
		{
			memcpy(&dead_addr.addr[0],&memberNode->memberList[*delete_list_itr- temp_num].id,sizeof(int));
			memcpy(&dead_addr.addr[4],&memberNode->memberList[*delete_list_itr- temp_num].port,sizeof(short));
//			cout<<" going to delete node "<<memberNode->memberList[*delete_list_itr- temp_num].id << " timestamp "<< memberNode->memberList[*delete_list_itr- temp_num].timestamp << "current timestamp "<< par->getcurrtime() <<" at node ";
//			printAddress(&memberNode->addr);
			log->logNodeRemove(&memberNode->addr,&dead_addr);

			memberNode->memberList.erase(memberNode->memberList.begin()+ *delete_list_itr - temp_num);
			temp_num++;
		}


		// update current position of node in its own membership list.
		for(i=0;i<memberNode->memberList.size();i++)
		{
			if(id==memberNode->memberList[i].id)
				pos_in_memberlist=i;
		}


		if(memberNode->pingCounter != GOSSIPPERIOD)
		{
			memberNode->pingCounter++;
			return;
		}
		
		// after gossip period send updated membership list to K random node
		
		memberNode->pingCounter=0;
//send the node detail
	int total_member=memberNode->memberList.size();

//    memberNode->memberList[pos_in_memberlist].timestamp=par->getcurrtime();
//    memberNode->memberList[pos_in_memberlist].heartbeat++;

	 heartbeat_msg_size=sizeof(int) + sizeof(MemberListEntry)*total_member;
	 heartbeat_msg=(char *)malloc(sizeof(char)*heartbeat_msg_size);
	 memset(heartbeat_msg,0x00,heartbeat_msg_size);
	 message_type=HEARTBEAT;
	 memcpy(heartbeat_msg,&message_type,sizeof(int));
	 memcpy(heartbeat_msg+sizeof(int),memberNode->memberList.data(),heartbeat_msg_size);

	 random_gossip_target.clear();
	 K=total_member/3;

	 if(K==1)
		 K++;

	 for(i=0;i<K;i++)
	 {
		 rand_member=rand()%total_member;
		 if(rand_member==pos_in_memberlist)
		 {
			 i--;
			 continue;
		 }

		 it=random_gossip_target.find(rand_member);
		 if(it != random_gossip_target.end())
		 {
			 i--;
			 continue;
		 }

		 random_gossip_target.insert(rand_member);
		 *(int *)(dest_addr.addr) = memberNode->memberList[rand_member].id;
		 *(short *)(&dest_addr.addr[4]) = memberNode->memberList[rand_member].port;
		 emulNet->ENsend(&(memberNode->addr),&dest_addr,heartbeat_msg,heartbeat_msg_size); //send membership list to random node.
	 }
	 free(heartbeat_msg);
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;
    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}
