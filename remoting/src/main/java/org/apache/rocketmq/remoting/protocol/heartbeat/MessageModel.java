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

/**
 * $Id: MessageModel.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.remoting.protocol.heartbeat;

/**
 * Message model
 * 消息推送及消费的机制（负载均衡或集群）<br>
 * 参考资料：<a href="https://baijiahao.baidu.com/s?id=1777919323929104700">深入理解RocketMQ 广播消费</a>
 */
public enum MessageModel {
    /**
     * broadcast
     * 广播模式
     * 同一Topic下的一条消息会推送到同一消费组中的所有消费者，也就是消息至少被消费一次（消费组内的每一个消费者都会消费全量消息）
     */
    BROADCASTING("BROADCASTING"),
    /**
     * clustering
     * 集群模式（Push消费者默认为集群模式）
     * 同一Topic下的一条消息只会被同一消费组中的一个消费者消费，也就是把消息负载均衡到多个消费者了（同一个消费组内的消费者分担消费）
     */
    CLUSTERING("CLUSTERING");

    private String modeCN;

    MessageModel(String modeCN) {
        this.modeCN = modeCN;
    }

    public String getModeCN() {
        return modeCN;
    }
}
