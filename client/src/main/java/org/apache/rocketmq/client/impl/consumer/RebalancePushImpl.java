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

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class RebalancePushImpl extends RebalanceImpl {
    private final static long UNLOCK_DELAY_TIME_MILLS = Long.parseLong(System.getProperty("rocketmq.client.unlockDelayTimeMills", "20000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;


    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }

    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory, DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }

    /**
     * 如果负载均衡结果显示有所改变，就更新拉取消息数量阈值并且通知Broker
     * @param topic 主题
     * @param mqAll 所有消息
     * @param mqDivided 不知道，暂时没啥用
     */
    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        /**
         * When rebalance result changed, should update subscription's version to notify broker.
         * Fix: inconsistency subscription may lead to consumer miss messages.
         */
        SubscriptionData subscriptionData = this.subscriptionInner.get(topic);
        long newVersion = System.currentTimeMillis();
        log.info("{} Rebalance changed, also update version: {}, {}", topic, subscriptionData.getSubVersion(), newVersion);
        subscriptionData.setSubVersion(newVersion);

        int currentQueueCount = this.processQueueTable.size();
        // 更新拉取消息数量阈值
        if (currentQueueCount != 0) {
            int pullThresholdForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForTopic();
            if (pullThresholdForTopic != -1) {
                int newVal = Math.max(1, pullThresholdForTopic / currentQueueCount);
                log.info("The pullThresholdForQueue is changed from {} to {}",
                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdForQueue(newVal);
            }

            int pullThresholdSizeForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForTopic();
            if (pullThresholdSizeForTopic != -1) {
                int newVal = Math.max(1, pullThresholdSizeForTopic / currentQueueCount);
                log.info("The pullThresholdSizeForQueue is changed from {} to {}",
                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(newVal);
            }
        }

        // notify broker
        // 通知broker
        this.getmQClientFactory().sendHeartbeatToAllBrokerWithLockV2(true);
    }

    /**
     * 对于已经消费的消息，首先向Broker同步消费进度更新Offset，然后解开Broker里这些消息的锁
     * @param mq
     * @param pq
     * @return
     */
    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
        if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
            && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            try {
                if (pq.getConsumeLock().tryLock(1000, TimeUnit.MILLISECONDS)) {
                    try {
                        return this.unlockDelay(mq, pq);
                    } finally {
                        pq.getConsumeLock().unlock();
                    }
                } else {
                    log.warn("[WRONG]mq is consuming, so can not unlock it, {}. maybe hanged for a while, {}",
                        mq,
                        pq.getTryUnlockTimes());

                    pq.incTryUnlockTimes();
                }
            } catch (Exception e) {
                log.error("removeUnnecessaryMessageQueue Exception", e);
            }

            return false;
        }
        return true;
    }

    /**
     * 如果设置的是默认的推送消费者的客户端负载均衡，或者是有序消费的
     * 或者如果消费机制是广播，就返回true
     * 否则返回false
     * 也就是说如果负载均衡是要在客户端实现的，那就返回true，否则返回false。
     * 总之，这个方法是判断是否在客户端负载均衡
     * @param topic 主题，但是没啥用
     * @return  是否负载均衡
     */
    @Override
    public boolean clientRebalance(String topic) {
        // POPTODO order pop consume not implement yet
        return defaultMQPushConsumerImpl.getDefaultMQPushConsumer().isClientRebalance() || defaultMQPushConsumerImpl.isConsumeOrderly() || MessageModel.BROADCASTING.equals(messageModel);
    }

    public boolean removeUnnecessaryPopMessageQueue(final MessageQueue mq, final PopProcessQueue pq) {
        return true;
    }

    private boolean unlockDelay(final MessageQueue mq, final ProcessQueue pq) {

        if (pq.hasTempMessage()) {
            log.info("[{}]unlockDelay, begin {} ", mq.hashCode(), mq);
            this.defaultMQPushConsumerImpl.getmQClientFactory().getScheduledExecutorService().schedule(new Runnable() {
                @Override
                public void run() {
                    log.info("[{}]unlockDelay, execute at once {}", mq.hashCode(), mq);
                    RebalancePushImpl.this.unlock(mq, true);
                }
            }, UNLOCK_DELAY_TIME_MILLS, TimeUnit.MILLISECONDS);
        } else {
            this.unlock(mq, true);
        }
        return true;
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    @Deprecated
    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1L;
        try {
            result = computePullFromWhereWithException(mq);
        } catch (MQClientException e) {
            log.warn("Compute consume offset exception, mq={}", mq);
        }
        return result;
    }

    /**
     * 计算拉取消息的offset
     * @param mq 消息队列
     * @return 拉取offset
     * @throws MQClientException 客户端异常
     */
    @Override
    public long computePullFromWhereWithException(MessageQueue mq) throws MQClientException {
        long result = -1;
        final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
            case CONSUME_FROM_MIN_OFFSET:
            case CONSUME_FROM_MAX_OFFSET:
            case CONSUME_FROM_LAST_OFFSET: {
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                // First start,no offset
                else if (-1 == lastOffset) {
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        result = 0L;
                    } else {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    }
                } else {
                    throw new MQClientException(ResponseCode.QUERY_NOT_FOUND, "Failed to query consume offset from " +
                            "offset store");
                }
                break;
            }
            case CONSUME_FROM_FIRST_OFFSET: {
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    //the offset will be fixed by the OFFSET_ILLEGAL process
                    result = 0L;
                } else {
                    throw new MQClientException(ResponseCode.QUERY_NOT_FOUND, "Failed to query offset from offset " +
                            "store");
                }
                break;
            }
            case CONSUME_FROM_TIMESTAMP: {
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    } else {
                        try {
                            long timestamp = UtilAll.parseDate(this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeTimestamp(),
                                UtilAll.YYYYMMDDHHMMSS).getTime();
                            result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    }
                } else {
                    throw new MQClientException(ResponseCode.QUERY_NOT_FOUND, "Failed to query offset from offset " +
                            "store");
                }
                break;
            }

            default:
                break;
        }

        if (result < 0) {
            throw new MQClientException(ResponseCode.SYSTEM_ERROR, "Found unexpected result " + result);
        }

        return result;
    }

    @Override
    public int getConsumeInitMode() {
        final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        if (ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET == consumeFromWhere) {
            return ConsumeInitMode.MIN;
        } else {
            return ConsumeInitMode.MAX;
        }
    }

    @Override
    public void dispatchPullRequest(final List<PullRequest> pullRequestList, final long delay) {
        for (PullRequest pullRequest : pullRequestList) {
            if (delay <= 0) {
                this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            } else {
                this.defaultMQPushConsumerImpl.executePullRequestLater(pullRequest, delay);
            }
        }
    }

    @Override
    public void dispatchPopPullRequest(final List<PopRequest> pullRequestList, final long delay) {
        for (PopRequest pullRequest : pullRequestList) {
            if (delay <= 0) {
                this.defaultMQPushConsumerImpl.executePopPullRequestImmediately(pullRequest);
            } else {
                this.defaultMQPushConsumerImpl.executePopPullRequestLater(pullRequest, delay);
            }
        }
    }

    @Override
    public ProcessQueue createProcessQueue() {
        return new ProcessQueue();
    }

    @Override
    public ProcessQueue createProcessQueue(String topicName) {
        return createProcessQueue();
    }

    @Override
    public PopProcessQueue createPopProcessQueue() {
        return new PopProcessQueue();
    }
}
