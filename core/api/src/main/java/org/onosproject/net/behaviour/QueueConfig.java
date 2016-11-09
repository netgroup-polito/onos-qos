/*
 * Copyright 2015-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onosproject.net.behaviour;

import org.onosproject.net.driver.HandlerBehaviour;

import java.util.List;


/**
 * Means to alter a device's dataplane queues.
 */
public interface QueueConfig extends HandlerBehaviour {

    boolean addQosProfile(QosProfileDescription qosProfileDesc);

    boolean removeQosProfile(String qosProfileName);

    List<QosProfileDescription> getQosProfiles();

    boolean addQueueProfile(String qosProfileName, QueueProfileDescription queueProfileDescr);

    boolean removeQueueProfile(String queueName);

    List<QueueProfileDescription> getQueueProfile();
    List<QueueProfileDescription> getQueueProfile(QosProfileDescription qosProfileDesc);

    boolean setQosProfile(String ifaceName, String qosProfileName);

    boolean clearQosProfile(String ifaceName);

    long getOfQueue(String qosProfileName, String queueProfileName);

}
