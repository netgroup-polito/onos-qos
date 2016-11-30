/*
 * Copyright 2016-present Open Networking Laboratory
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
package org.qos.app;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.onlab.packet.Ethernet;
import org.onlab.packet.IpPrefix;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.PortNumber;
import org.onosproject.net.behaviour.BridgeConfig;
import org.onosproject.net.behaviour.BridgeDescription;
import org.onosproject.net.behaviour.DefaultQosProfileDescription;
import org.onosproject.net.behaviour.DefaultQueueProfileDescription;
import org.onosproject.net.behaviour.InterfaceConfig;
import org.onosproject.net.behaviour.QosProfileDescription;
import org.onosproject.net.behaviour.QueueConfig;
import org.onosproject.net.behaviour.QueueProfileDescription;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.driver.DriverService;
import org.onosproject.net.flow.DefaultFlowRule;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;


/**
 * Skeletal ONOS application component.
 */
@Component(immediate = true)
public class AppComponent {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ComponentConfigService cfgService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected FlowRuleService flowRuleService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DriverService driverService;

    private ApplicationId appId;

    @Activate
    protected void activate() {
        //cfgService.registerProperties(getClass()); //???
        appId = coreService.registerApplication("org.qos.app");

        log.info("Starting..."+appId);

        for( Device d : deviceService.getAvailableDevices())
        {
            log.info("Found device: "+ d.id() +" ["+d.manufacturer()+"]");

            if(d.is(QueueConfig.class)) {
                if ( d.is(BridgeConfig.class)) {
                    if (d.id().toString().startsWith("ovsdb:")) {
                        QueueConfig qg = d.as(QueueConfig.class);
                        BridgeConfig bg = d.as(BridgeConfig.class);

                        BridgeDescription bridge = bg.getBridges().iterator().next();

                        DeviceId idOF = bridge.deviceId().get();

                        log.info("====>>> OpenFlow Datapath ID:" + idOF.uri());

                        log.info("Configured Qos Profile:");

                        /** List qos profiles configured on device d **/
                        List<QosProfileDescription> qoses = qg.getQosProfiles();
                        for (QosProfileDescription qos : qoses) {
                            log.info("Qos Name: " + qos.name() + " => " + qos.toString());

                            List<QueueProfileDescription> queues = qg.getQueueProfile(qos.name());

                            for (QueueProfileDescription queue : queues) {
                                log.info("Queue Name: " + queue.name() + " => " + queue.toString());
                            }
                        }
                        /** Add new qos profile named QosProfile_VoIPDati , type LINUX_HTB and max rate 10Mbps **/
                        boolean ret = qg.addQosProfile(new DefaultQosProfileDescription("QosProfile_VoIPDati",
                                                                                        QosProfileDescription.Type.LINUX_HTB,
                                                                                        Optional.empty(), //Min-rate
                                                                                        Optional.of(new Long(10000000))  //Max-rate 10Mbps
                        ));

                        /** Check configuration result **/
                        if (ret)
                            log.info("QosProfile_VoIPDati added correctly!");
                        else log.error("QosProfile_VoIPDati add error =(!");

                        /** Add new queue profile inside QosProfile_VoIPDati, with min-rate=max-rate=2Mbps and priority 0 (max) **/
                        ret = qg.addQueueProfile("QosProfile_VoIPDati", new DefaultQueueProfileDescription("QueueProfile_VoIP",
                                                                                                           QueueProfileDescription.Type.FULL,
                                                                                                           Optional.of(new Long(2000000)), //Min-rate 2Mbps
                                                                                                           Optional.of(new Long(2000000)), //Max-rate 2Mbps
                                                                                                           Optional.empty(),
                                                                                                           Optional.of(new Long(0)))); //Massima priorita'

                        /** Check configuration result **/
                        if (ret) log.info("QueueProfile_VoIP added correctly!");
                        else log.error("QueueProfile_VoIP add error =(!");

                        /** Add new queue profile inside QosProfile_VoIPDati, with min-rate=0, max-rate=10Mbps and priority 1 **/
                        ret = qg.addQueueProfile("QosProfile_VoIPDati", new DefaultQueueProfileDescription("QueueProfile_Dati",
                                                                                                           QueueProfileDescription.Type.FULL,
                                                                                                           Optional.of(new Long(0)), //Min-rate 0bps
                                                                                                           Optional.of(new Long(10000000)), //Max-rate 10Mbps
                                                                                                           Optional.empty(),
                                                                                                           Optional.of(new Long(1)))); //Minore priorita'

                        /** Check the configuration result **/
                        if (ret) log.info("QueueProfile_Dati added correctly!");
                        else log.error("QueueProfile_Dati add error =(!");

                        /** Apply the qos profile QosProfile_VoIPDati on interface eth1 **/
                        ret = qg.setQosProfile("eth1", "QosProfile_VoIPDati");

                        /** Check configuration result **/
                        if (ret) log.info("set qos ok!");
                        else log.error("set qos error =(!");

                        /** Get the OpenFlowID of the two queues **/
                        long voipQueueOFID = qg.getOfQueue("QosProfile_VoIPDati", "QueueProfile_VoIP");
                        long datiQueueOFID = qg.getOfQueue("QosProfile_VoIPDati", "QueueProfile_Dati");


                        log.info("OpenFlow Queue ID of QosProfile_VoIPDati/QueueProfile_VoIP : " + voipQueueOFID);
                        log.info("OpenFlow Queue ID of QosProfile_VoIPDati/QueueProfile_Dati : " + datiQueueOFID);

                        TrafficSelector.Builder trafficSelectorBuilderFromHost1ToServer = DefaultTrafficSelector.builder();

                        trafficSelectorBuilderFromHost1ToServer.matchInPort(PortNumber.portNumber(2));
                        trafficSelectorBuilderFromHost1ToServer.matchEthType(Ethernet.TYPE_IPV4);
                        trafficSelectorBuilderFromHost1ToServer.matchIPDst(IpPrefix.valueOf("192.168.90.1/32"));

                        TrafficTreatment.Builder trafficTreatmentBuilderSetQueue0 = DefaultTrafficTreatment.builder();

                        /** setQueue generate the OpenFlow action SetQueue(queueID) (not enqueue(queueID,portID) ) **/
                        trafficTreatmentBuilderSetQueue0.setQueue(datiQueueOFID, PortNumber.portNumber(1));
                        trafficTreatmentBuilderSetQueue0.setOutput(PortNumber.portNumber(1));

                        //trafficTreatmentBuilderSetQueue0.setQueue(0); -> non supportato da OVS?

                        FlowRule flowRuleFromHost1ToServer = new DefaultFlowRule(idOF, trafficSelectorBuilderFromHost1ToServer.build(), trafficTreatmentBuilderSetQueue0.build(), 60010, appId, 0, true, null);

                        TrafficSelector.Builder trafficSelectorBuilderFromHost2ToServer = DefaultTrafficSelector.builder();

                        trafficSelectorBuilderFromHost2ToServer.matchInPort(PortNumber.portNumber(3));
                        trafficSelectorBuilderFromHost2ToServer.matchEthType(Ethernet.TYPE_IPV4);
                        trafficSelectorBuilderFromHost2ToServer.matchIPDst(IpPrefix.valueOf("192.168.90.1/32"));

                        TrafficTreatment.Builder trafficTreatmentSetQueue1 = DefaultTrafficTreatment.builder();

                        /** setQueue generate the OpenFlow action SetQueue(queueID) (not enqueue(queueID,portID) ) **/
                        trafficTreatmentSetQueue1.setQueue(voipQueueOFID, PortNumber.portNumber(1));
                        trafficTreatmentSetQueue1.setOutput(PortNumber.portNumber(1));

                        FlowRule flowRuleFromHost2ToServer = new DefaultFlowRule(idOF, trafficSelectorBuilderFromHost2ToServer.build(), trafficTreatmentSetQueue1.build(), 60010, appId, 0, true, null);

                        //ovs-ofctl add-flow ovs-br0 actions=normal
                        TrafficTreatment.Builder trafficTreatmentNormal = DefaultTrafficTreatment.builder();

                        trafficTreatmentNormal.setOutput(PortNumber.NORMAL);

                        FlowRule flowRuleNormal = new DefaultFlowRule(idOF, DefaultTrafficSelector.emptySelector(), trafficTreatmentNormal.build(), 60000, appId, 0, true, null);

                        flowRuleService.applyFlowRules(flowRuleFromHost1ToServer);
                        flowRuleService.applyFlowRules(flowRuleFromHost2ToServer);
                        flowRuleService.applyFlowRules(flowRuleNormal);
                    }
                    else
                    {
                        log.info("Device is not 'ovsdb:' (necessary for BridgeConfig) ");
                    }
                }
                else
                {
                    log.info("Device without BridgeConfig =(");
                }
            }
            else
            {
                log.info("Device without QueueConfig =( ");
            }
            if (!d.is(InterfaceConfig.class))
            {
                log.info("Device without InterfaceConfig =(");
            }
        }
        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        flowRuleService.removeFlowRulesById(appId);
        log.info("Stopped");
    }
}