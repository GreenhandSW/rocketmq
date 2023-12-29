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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public abstract class RebalanceImpl {
    protected static final Logger log = LoggerFactory.getLogger(RebalanceImpl.class);

    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<>(64);
    protected final ConcurrentMap<MessageQueue, PopProcessQueue> popProcessQueueTable = new ConcurrentHashMap<>(64);

    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
        new ConcurrentHashMap<>();
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
        new ConcurrentHashMap<>();
    protected String consumerGroup;
    protected MessageModel messageModel;
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mQClientFactory;
    private static final int TIMEOUT_CHECK_TIMES = 3;
    private static final int QUERY_ASSIGNMENT_TIMEOUT = 3000;

    private Map<String, String> topicBrokerRebalance = new ConcurrentHashMap<>();
    private Map<String, String> topicClientRebalance = new ConcurrentHashMap<>();

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty()) {
                continue;
            }

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<>();

        for (Map.Entry<MessageQueue, ProcessQueue> entry : this.processQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (pq.isDropped()) {
                continue;
            }

            String destBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(mq);
            Set<MessageQueue> mqs = result.get(destBrokerName);
            if (null == mqs) {
                mqs = new HashSet<>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    public boolean lock(final MessageQueue mq) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOK = lockedMq.contains(mq);
                log.info("message queue lock {}, {} {}", lockOK ? "OK" : "Failed", this.consumerGroup, mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    public void lockAll() {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty()) {
                continue;
            }

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    Set<MessageQueue> lockOKMQSet =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (lockOKMQSet.contains(mq)) {
                                if (!processQueue.isLocked()) {
                                    log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                                }
                                processQueue.setLocked(true);
                                processQueue.setLastLockTimestamp(System.currentTimeMillis());
                            } else {
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    /**
     * 是否由客户端进行负载均衡
     * @param topic 主题
     * @return 是否由客户端负载均衡
     */
    public boolean clientRebalance(String topic) {
        return true;
    }

    /**
     * 执行负载均衡
     * @param isOrder 是否顺序消费
     * @return 均衡结果
     */
    public boolean doRebalance(final boolean isOrder) {
        boolean balanced = true;
        // 获取消费者和订阅信息（主要是主题）之间的订阅关系map
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            // 对每个消费者的订阅信息，获取其主题
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    // 广播模式没有负载均衡，拉取方式也是自由拉取不必负载均衡，所以对于消费者，只有集群模式的推送方式下才需要执行负载均衡
                    // 如果这个主题要求的是生产者负载均衡，并且就直接从broker获取均衡结果（实际上是请求在生产者侧进行负载均衡）
                    if (!clientRebalance(topic) && tryQueryAssignment(topic)) {
                        balanced = this.getRebalanceResultFromBroker(topic, isOrder);
                    } else {
                        // 只有对于集群模式下的推送方式，才执行消费者负载均衡
                        balanced = this.rebalanceByTopic(topic, isOrder);
                    }
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalance Exception", e);
                        balanced = false;
                    }
                }
            }
        }

        // 更新重平衡后的processQueueTable、popProcessQueueTable、topicClientRebalance、topicBrokerRebalance
        this.truncateMessageQueueNotMyTopic();

        return balanced;
    }

    /**
     * 检查主题是否设置了broker负载均衡（让Broker负载均衡实际上是让生产者轮询把消息发到队列里）
     * @param topic 检查的主题
     * @return      true: 已经为主题分配了broker负载均衡; false: 分配了客户端负载均衡、broker分配超时（或者连接不到）或超过3次
     */
    private boolean tryQueryAssignment(String topic) {
        // 如果是在客户端负载均衡，并且客户端负载均衡表里有这个主题表示已经分配过了，就返回false
        if (topicClientRebalance.containsKey(topic)) {
            return false;
        }
        // 如果是在broker负载均衡，并且broker负载均衡表里有这个主题表示已经分配过了，就返回true
        if (topicBrokerRebalance.containsKey(topic)) {
            return true;
        }
        String strategyName = allocateMessageQueueStrategy != null ? allocateMessageQueueStrategy.getName() : null;
        int retryTimes = 0;
        // 默认尝试Broker负载均衡，如果3次都失败就更新到消费者负载均衡表，后面会让消费者做负载均衡
        while (retryTimes++ < TIMEOUT_CHECK_TIMES) {
            try {
                // 进行分配，然后返回true
                Set<MessageQueueAssignment> resultSet = mQClientFactory.queryAssignment(topic, consumerGroup,
                    strategyName, messageModel, QUERY_ASSIGNMENT_TIMEOUT / TIMEOUT_CHECK_TIMES * retryTimes);
                // 更新Broker负载均衡表
                topicBrokerRebalance.put(topic, topic);
                return true;
            } catch (Throwable t) {
                if (!(t instanceof RemotingTimeoutException)) {
                    log.error("tryQueryAssignment error.", t);
                    topicClientRebalance.put(topic, topic);
                    return false;
                }
            }
        }
        // 尝试达到3次分配都失败后，返回false
        if (retryTimes >= TIMEOUT_CHECK_TIMES) {
            // if never success before and timeout exceed TIMEOUT_CHECK_TIMES, force client rebalance
            topicClientRebalance.put(topic, topic);
            return false;
        }
        return true;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    /**
     * 客户端负载均衡
     * @param topic 主题
     * @param isOrder 是否顺序消费
     * @return 均衡结果
     */
    private boolean rebalanceByTopic(final String topic, final boolean isOrder) {
        boolean balanced = true;
        switch (messageModel) {
            // 如果是广播模式
            case BROADCASTING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    // 更新消费快照表
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    // 如果更新的时候改动了快照表，就通知Broker
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}", consumerGroup, topic, mqSet, mqSet);
                    }

                    // 对于广播模式，如果当前工作的消息队列和订阅该主题的消息队列一致，说明均衡成功
                    balanced = mqSet.equals(getWorkingMessageQueue(topic));
                } else {
                    // 当前订阅表为空，显然是有所改动，因此通知Broker。
                    this.messageQueueChanged(topic, Collections.<MessageQueue>emptySet(), Collections.<MessageQueue>emptySet());
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            // 如果是集群模式
            case CLUSTERING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                // 如果没有消息队列订阅该主题
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        this.messageQueueChanged(topic, Collections.<MessageQueue>emptySet(), Collections.<MessageQueue>emptySet());
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                // 有消息队列、并且有消费者，才能开始负载均衡
                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<>();
                    mqAll.addAll(mqSet);

                    Collections.sort(mqAll);
                    Collections.sort(cidAll);

                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    List<MessageQueue> allocateResult = null;
                    try {
                        // 做负载均衡
                        allocateResult = strategy.allocate(
                            this.consumerGroup,
                            this.mQClientFactory.getClientId(),
                            mqAll,
                            cidAll);
                    } catch (Throwable e) {
                        log.error("allocate message queue exception. strategy name: {}, ex: {}", strategy.getName(), e);
                        return false;
                    }

                    Set<MessageQueue> allocateResultSet = new HashSet<>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }
                    // 更新消费快照表
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if (changed) {
                        log.info(
                            "client rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                            allocateResultSet.size(), allocateResultSet);
                        // 如果更新的时候改动了快照表，就通知Broker
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }

                    // 对于集群模式，如果均衡后的消息队列和当前工作消息队列一致，说明均衡成功
                    balanced = allocateResultSet.equals(getWorkingMessageQueue(topic));
                }
                break;
            }
            default:
                break;
        }

        return balanced;
    }

    private boolean getRebalanceResultFromBroker(final String topic, final boolean isOrder) {
        // 根据分配策略，要求broker分配负载均衡结果
        String strategyName = this.allocateMessageQueueStrategy.getName();
        Set<MessageQueueAssignment> messageQueueAssignments;
        try {
            messageQueueAssignments = this.mQClientFactory.queryAssignment(topic, consumerGroup,
                strategyName, messageModel, QUERY_ASSIGNMENT_TIMEOUT);
        } catch (Exception e) {
            log.error("allocate message queue exception. strategy name: {}, ex: {}", strategyName, e);
            return false;
        }

        // null means invalid result, we should skip the update logic
        if (messageQueueAssignments == null) {
            return false;
        }
        Set<MessageQueue> mqSet = new HashSet<>();
        for (MessageQueueAssignment messageQueueAssignment : messageQueueAssignments) {
            if (messageQueueAssignment.getMessageQueue() != null) {
                mqSet.add(messageQueueAssignment.getMessageQueue());
            }
        }
        Set<MessageQueue> mqAll = null;
        boolean changed = this.updateMessageQueueAssignment(topic, messageQueueAssignments, isOrder);
        if (changed) {
            log.info("broker rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, assignmentSet={}",
                strategyName, consumerGroup, topic, this.mQClientFactory.getClientId(), messageQueueAssignments);
            this.messageQueueChanged(topic, mqAll, mqSet);
        }

        return mqSet.equals(getWorkingMessageQueue(topic));
    }

    private Set<MessageQueue> getWorkingMessageQueue(String topic) {
        Set<MessageQueue> queueSet = new HashSet<>();
        for (Entry<MessageQueue, ProcessQueue> entry : this.processQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (mq.getTopic().equals(topic) && !pq.isDropped()) {
                queueSet.add(mq);
            }
        }

        for (Entry<MessageQueue, PopProcessQueue> entry : this.popProcessQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            PopProcessQueue pq = entry.getValue();

            if (mq.getTopic().equals(topic) && !pq.isDropped()) {
                queueSet.add(mq);
            }
        }

        return queueSet;
    }

    /**
     * 更新重平衡后的processQueueTable、popProcessQueueTable、topicClientRebalance、topicBrokerRebalance
     */
    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        // 删除订阅主题不存在的快照
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            // 如果订阅表不存在订阅的主题
            if (!subTable.containsKey(mq.getTopic())) {
                // 就删除快照，将其设置为丢弃状态
                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }

        // 删除订阅主题不存在的pop快照
        for (MessageQueue mq : this.popProcessQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                PopProcessQueue pq = this.popProcessQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary pop mq, {}", consumerGroup, mq);
                }
            }
        }

        Iterator<Map.Entry<String, String>> clientIter = topicClientRebalance.entrySet().iterator();
        while (clientIter.hasNext()) {
            if (!subTable.containsKey(clientIter.next().getKey())) {
                clientIter.remove();
            }
        }

        Iterator<Map.Entry<String, String>> brokerIter = topicBrokerRebalance.entrySet().iterator();
        while (brokerIter.hasNext()) {
            if (!subTable.containsKey(brokerIter.next().getKey())) {
                brokerIter.remove();
            }
        }
    }

    /**
     * 更新消费快照表
     * @param topic 等待更新的主题
     * @param mqSet 当前的消息队列
     * @param isOrder 是否顺序消费
     * @return 是否改动了消费快照表
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
        final boolean isOrder) {
        // 是否丢弃、移除过消息
        boolean changed = false;

        // drop process queues no longer belong me
        HashMap<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<>(this.processQueueTable.size());
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            if (mq.getTopic().equals(topic)) {
                // 消息队列里已经不存在这个消息了，就把它的状态设置为丢弃，放到待丢弃的map中
                if (!mqSet.contains(mq)) {
                    pq.setDropped(true);
                    removeQueueMap.put(mq, pq);
                // 或者还存在这个消息，
                    // 但主动拉取间歇过长，也就是说由于长时间没有拉取，说明消费者已经不再使用这个队列了
                    // 并且消费者是被动消费（推送模式），那就表示这个消息也应该被丢弃了。
                    // 总结一下，大致意思就是：消费者是推送模式，但是这个消息2分钟都没推送出去，说明消费者不再使用这个队列了，所以这些消息也没啥用了，应该被丢掉
                } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                    pq.setDropped(true);
                    removeQueueMap.put(mq, pq);
                    log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                        consumerGroup, mq);
                }
            }
        }

        // remove message queues no longer belong me
        // 移除已经消费过的消息
        // 主要是在Broker端把负载均衡的锁解开，然后在这里把解开的消息队列对应的消费快照清理掉
        // 这样快照map里所有的消息队列都是锁上的状态
        for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                this.processQueueTable.remove(mq);
                changed = true;
                log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
            }
        }

        // add new message queue
        boolean allMQLocked = true;
        List<PullRequest> pullRequestList = new ArrayList<>();
        for (MessageQueue mq : mqSet) {
            if (!this.processQueueTable.containsKey(mq)) {
                // 如果顺序消费，并且这个消息队列没有被负载均衡上锁，那说明现在暂时无法做负载均衡
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    allMQLocked = false;
                    continue;
                }

                this.removeDirtyOffset(mq);
                // 创建一个新的消费快照并且上锁
                ProcessQueue pq = createProcessQueue(topic);
                pq.setLocked(true);
                // 如果消费位置有效（实际上必然是有效的，因为<0的位置会直接触发异常），就把快照放到map里
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    // 如果这个消费快照本来就存在，不做处理
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    // 如果这个消费快照本来不存在，然后他现在已经被放到快照map里等待消费，就继续发起拉取请求
                    } else {
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }

        }

        // 消息队列没有都上锁，只能等500ms再做负载均衡
        if (!allMQLocked) {
            mQClientFactory.rebalanceLater(500);
        }

        // 把拉取请求放到计时任务队列里
        this.dispatchPullRequest(pullRequestList, 500);

        return changed;
    }

    private boolean updateMessageQueueAssignment(final String topic, final Set<MessageQueueAssignment> assignments,
        final boolean isOrder) {
        boolean changed = false;

        Map<MessageQueue, MessageQueueAssignment> mq2PushAssignment = new HashMap<>();
        Map<MessageQueue, MessageQueueAssignment> mq2PopAssignment = new HashMap<>();
        for (MessageQueueAssignment assignment : assignments) {
            MessageQueue messageQueue = assignment.getMessageQueue();
            if (messageQueue == null) {
                continue;
            }
            if (MessageRequestMode.POP == assignment.getMode()) {
                mq2PopAssignment.put(messageQueue, assignment);
            } else {
                mq2PushAssignment.put(messageQueue, assignment);
            }
        }

        if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            if (mq2PopAssignment.isEmpty() && !mq2PushAssignment.isEmpty()) {
                //pop switch to push
                //subscribe pop retry topic
                try {
                    final String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                    getSubscriptionInner().put(retryTopic, subscriptionData);
                } catch (Exception ignored) {
                }

            } else if (!mq2PopAssignment.isEmpty() && mq2PushAssignment.isEmpty()) {
                //push switch to pop
                //unsubscribe pop retry topic
                try {
                    final String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
                    getSubscriptionInner().remove(retryTopic);
                } catch (Exception ignored) {
                }

            }
        }

        {
            // drop process queues no longer belong me
            HashMap<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<>(this.processQueueTable.size());
            Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, ProcessQueue> next = it.next();
                MessageQueue mq = next.getKey();
                ProcessQueue pq = next.getValue();

                if (mq.getTopic().equals(topic)) {
                    if (!mq2PushAssignment.containsKey(mq)) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                    } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                        log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                            consumerGroup, mq);
                    }
                }
            }
            // remove message queues no longer belong me
            for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
                MessageQueue mq = entry.getKey();
                ProcessQueue pq = entry.getValue();

                if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                    this.processQueueTable.remove(mq);
                    changed = true;
                    log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }

        {
            HashMap<MessageQueue, PopProcessQueue> removeQueueMap = new HashMap<>(this.popProcessQueueTable.size());
            Iterator<Entry<MessageQueue, PopProcessQueue>> it = this.popProcessQueueTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, PopProcessQueue> next = it.next();
                MessageQueue mq = next.getKey();
                PopProcessQueue pq = next.getValue();

                if (mq.getTopic().equals(topic)) {
                    if (!mq2PopAssignment.containsKey(mq)) {
                        //the queue is no longer your assignment
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                    } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                        log.error("[BUG]doRebalance, {}, try remove unnecessary pop mq, {}, because pop is pause, so try to fixed it",
                            consumerGroup, mq);
                    }
                }
            }
            // remove message queues no longer belong me
            for (Entry<MessageQueue, PopProcessQueue> entry : removeQueueMap.entrySet()) {
                MessageQueue mq = entry.getKey();
                PopProcessQueue pq = entry.getValue();

                if (this.removeUnnecessaryPopMessageQueue(mq, pq)) {
                    this.popProcessQueueTable.remove(mq);
                    changed = true;
                    log.info("doRebalance, {}, remove unnecessary pop mq, {}", consumerGroup, mq);
                }
            }
        }

        {
            // add new message queue
            boolean allMQLocked = true;
            List<PullRequest> pullRequestList = new ArrayList<>();
            for (MessageQueue mq : mq2PushAssignment.keySet()) {
                if (!this.processQueueTable.containsKey(mq)) {
                    if (isOrder && !this.lock(mq)) {
                        log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                        allMQLocked = false;
                        continue;
                    }

                    this.removeDirtyOffset(mq);
                    ProcessQueue pq = createProcessQueue();
                    pq.setLocked(true);
                    long nextOffset = -1L;
                    try {
                        nextOffset = this.computePullFromWhereWithException(mq);
                    } catch (Exception e) {
                        log.info("doRebalance, {}, compute offset failed, {}", consumerGroup, mq);
                        continue;
                    }

                    if (nextOffset >= 0) {
                        ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                        if (pre != null) {
                            log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                        } else {
                            log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                            PullRequest pullRequest = new PullRequest();
                            pullRequest.setConsumerGroup(consumerGroup);
                            pullRequest.setNextOffset(nextOffset);
                            pullRequest.setMessageQueue(mq);
                            pullRequest.setProcessQueue(pq);
                            pullRequestList.add(pullRequest);
                            changed = true;
                        }
                    } else {
                        log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                    }
                }
            }

            if (!allMQLocked) {
                mQClientFactory.rebalanceLater(500);
            }
            this.dispatchPullRequest(pullRequestList, 500);
        }

        {
            // add new message queue
            List<PopRequest> popRequestList = new ArrayList<>();
            for (MessageQueue mq : mq2PopAssignment.keySet()) {
                if (!this.popProcessQueueTable.containsKey(mq)) {
                    PopProcessQueue pq = createPopProcessQueue();
                    PopProcessQueue pre = this.popProcessQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq pop already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new pop mq, {}", consumerGroup, mq);
                        PopRequest popRequest = new PopRequest();
                        popRequest.setTopic(topic);
                        popRequest.setConsumerGroup(consumerGroup);
                        popRequest.setMessageQueue(mq);
                        popRequest.setPopProcessQueue(pq);
                        popRequest.setInitMode(getConsumeInitMode());
                        popRequestList.add(popRequest);
                        changed = true;
                    }
                }
            }

            this.dispatchPopPullRequest(popRequestList, 500);
        }

        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public boolean removeUnnecessaryPopMessageQueue(final MessageQueue mq, final PopProcessQueue pq) {
        return true;
    }

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    /**
     * When the network is unstable, using this interface may return wrong offset.
     * It is recommended to use computePullFromWhereWithException instead.
     * @param mq
     * @return offset
     */
    @Deprecated
    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract long computePullFromWhereWithException(final MessageQueue mq) throws MQClientException;

    public abstract int getConsumeInitMode();

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList, final long delay);

    public abstract void dispatchPopPullRequest(final List<PopRequest> pullRequestList, final long delay);

    public abstract ProcessQueue createProcessQueue();

    public abstract PopProcessQueue createPopProcessQueue();

    public abstract ProcessQueue createProcessQueue(String topicName);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<MessageQueue, PopProcessQueue> getPopProcessQueueTable() {
        return popProcessQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();

        Iterator<Entry<MessageQueue, PopProcessQueue>> popIt = this.popProcessQueueTable.entrySet().iterator();
        while (popIt.hasNext()) {
            Entry<MessageQueue, PopProcessQueue> next = popIt.next();
            next.getValue().setDropped(true);
        }
        this.popProcessQueueTable.clear();
    }

}
