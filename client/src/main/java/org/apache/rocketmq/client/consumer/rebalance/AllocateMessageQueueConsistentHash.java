/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.rocketmq.common.consistenthash.ConsistentHashRouter;
import org.apache.rocketmq.common.consistenthash.HashFunction;
import org.apache.rocketmq.common.consistenthash.Node;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Consistent Hashing queue algorithm
 */
public class AllocateMessageQueueConsistentHash extends AbstractAllocateMessageQueueStrategy {

    /**
     * 虚拟节点数量，默认为10
     */
    private final int virtualNodeCnt;
    private final HashFunction customHashFunction;

    public AllocateMessageQueueConsistentHash() {
        this(10);
    }

    public AllocateMessageQueueConsistentHash(int virtualNodeCnt) {
        this(virtualNodeCnt, null);
    }

    public AllocateMessageQueueConsistentHash(int virtualNodeCnt, HashFunction customHashFunction) {
        if (virtualNodeCnt < 0) {
            throw new IllegalArgumentException("illegal virtualNodeCnt :" + virtualNodeCnt);
        }
        this.virtualNodeCnt = virtualNodeCnt;
        this.customHashFunction = customHashFunction;
    }


    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<>();
        // 如果当前消费者不在所有消费者id列表里，直接返回空MQ列表。
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }
        // 添加所有客户端节点
        Collection<ClientNode> cidNodes = new ArrayList<>();
        for (String cid : cidAll) {
            cidNodes.add(new ClientNode(cid));
        }
        // 创建一个虚拟节点数量默认为10的一致性哈希路由器
        final ConsistentHashRouter<ClientNode> router; //for building hash ring
        if (customHashFunction != null) {
            router = new ConsistentHashRouter<>(cidNodes, virtualNodeCnt, customHashFunction);
        } else {
            router = new ConsistentHashRouter<>(cidNodes, virtualNodeCnt);
        }

        List<MessageQueue> results = new ArrayList<>();
        for (MessageQueue mq : mqAll) {
            // 为MQ路由一个客户端节点，如果路由到当前客户端，就把MQ放到结果里
            ClientNode clientNode = router.routeNode(mq.toString());
            if (clientNode != null && currentCID.equals(clientNode.getKey())) {
                results.add(mq);
            }
        }

        return results;

    }

    @Override
    public String getName() {
        return "CONSISTENT_HASH";
    }

    private static class ClientNode implements Node {
        private final String clientID;

        public ClientNode(String clientID) {
            this.clientID = clientID;
        }

        @Override
        public String getKey() {
            return clientID;
        }
    }

}
