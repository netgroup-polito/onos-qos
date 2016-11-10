package org.onosproject.drivers.ovsdb;

import org.onlab.packet.IpAddress;
import org.onlab.packet.TpPort;
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.DeviceId;
import org.onosproject.net.behaviour.DefaultQosProfileDescription;
import org.onosproject.net.behaviour.DefaultQueueProfileDescription;
import org.onosproject.net.behaviour.QosProfileDescription;
import org.onosproject.net.behaviour.QueueConfig;
import org.onosproject.net.behaviour.QueueProfileDescription;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.driver.AbstractHandlerBehaviour;
import org.onosproject.net.driver.DriverHandler;
import org.onosproject.ovsdb.controller.OvsdbBridge;
import org.onosproject.ovsdb.controller.OvsdbClientService;
import org.onosproject.ovsdb.controller.OvsdbConstant;
import org.onosproject.ovsdb.controller.OvsdbController;
import org.onosproject.ovsdb.controller.OvsdbNodeId;
import org.onosproject.ovsdb.controller.OvsdbQosProfile;
import org.onosproject.ovsdb.controller.OvsdbQueueProfile;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.onlab.util.Tools.delay;
import static org.slf4j.LoggerFactory.getLogger;

public class OvsdbQueueConfig extends AbstractHandlerBehaviour implements QueueConfig {
    private final Logger log = getLogger(getClass());

    @Override
    public boolean addQosProfile(QosProfileDescription qosProfileDescr) {
        OvsdbQosProfile ovsdbQosProfile = OvsdbQosProfile.builder(qosProfileDescr).build();
        OvsdbClientService ovsdbClient = getOvsdbClient(handler());

        return ovsdbClient.createQosProfile(ovsdbQosProfile);
    }

    @Override
    public boolean removeQosProfile(String qosProfilename) {
        OvsdbClientService ovsdbClient = getOvsdbClient(handler());
        return ovsdbClient.dropQosProfile(qosProfilename);
    }

    @Override
    public List<QosProfileDescription> getQosProfiles() {
        List<QosProfileDescription> qosProfilesDescr = new ArrayList<>();

        DriverHandler handler = handler();
        OvsdbClientService client = getOvsdbClient(handler);
        Set<OvsdbQosProfile> ovsdbSet = client.getQosProfiles();
        if (ovsdbSet != null) {
            ovsdbSet.forEach(o -> {
                qosProfilesDescr.add(new DefaultQosProfileDescription(o.name(), o.type(), o.minRate(), o.maxRate()));
            });
            return qosProfilesDescr;
        }
        return null;
    }

    @Override
    public boolean addQueueProfile(String qosProfileName, QueueProfileDescription queueProfileDescr) {
        OvsdbQueueProfile ovsdbQosProfile = OvsdbQueueProfile.builder(queueProfileDescr).build();
        OvsdbClientService ovsdbClient = getOvsdbClient(handler());

        return ovsdbClient.createQueueProfile(qosProfileName, ovsdbQosProfile);
    }

    @Override
    public boolean removeQueueProfile(String queueProfileName) {
        OvsdbClientService ovsdbClient = getOvsdbClient(handler());
        return ovsdbClient.dropQueueProfile(queueProfileName);
    }

    @Override
    public List<QueueProfileDescription> getQueueProfile() {
        return getQueueProfile(null);
    }

    @Override
    public List<QueueProfileDescription> getQueueProfile(String queueProfileName) {
        List<QueueProfileDescription> queueProfilesDescr = new ArrayList<>();

        DriverHandler handler = handler();
        OvsdbClientService client = getOvsdbClient(handler);
        Set<OvsdbQueueProfile> ovsdbSet = (queueProfileName == null) ?
                client.getQueueProfiles() : client.getQueueProfiles(queueProfileName);
        if (ovsdbSet != null) {
            ovsdbSet.forEach(o -> {
                queueProfilesDescr.add(new DefaultQueueProfileDescription(o.name(), o.type(), o.minRate(), o.maxRate(),
                                                                          o.burst(), o.priority()));
            });
            return queueProfilesDescr;
        }
        return null;
    }

    @Override
    public boolean setQosProfile(String iface, String qosProfileName) {
        OvsdbClientService ovsdbClient = getOvsdbClient(handler());

        return ovsdbClient.setQueueProfile(iface, qosProfileName);
    }

    @Override
    public boolean clearQosProfile(String ifaceName) {
        OvsdbClientService ovsdbClient = getOvsdbClient(handler());

        return ovsdbClient.clearQosProfile(ifaceName);
    }

    @Override
    public long getOfQueue(String qosProfileName, String queueProfileName) {
        OvsdbClientService ovsdbClient = getOvsdbClient(handler());

        return ovsdbClient.getOfQueue(qosProfileName, queueProfileName);
    }


    private OvsdbClientService getOvsdbClient(DriverHandler handler) {
        OvsdbController ovsController = handler.get(OvsdbController.class);
        DeviceService deviceService = handler.get(DeviceService.class);
        DeviceId deviceId = handler.data().deviceId();

        String[] splits = deviceId.toString().split(":");
        if (splits == null || splits.length < 1) {
            log.warn("Wrong deviceId format");
            return null;
        }

        /**
         * Each type of device has to be managed in a different way.
         */
        switch (splits[0]) {
            case "ovsdb":
                OvsdbNodeId nodeId = changeDeviceIdToNodeId(deviceId);
                return ovsController.getOvsdbClient(nodeId);
            case "of":
                String[] mgmtAddress = deviceService.getDevice(deviceId)
                        .annotations().value(AnnotationKeys.MANAGEMENT_ADDRESS).split(":");
                String targetIp = mgmtAddress[0];
                TpPort targetPort = null;
                if (mgmtAddress.length > 1) {
                    targetPort = TpPort.tpPort(Integer.parseInt(mgmtAddress[1]));
                }
                List<OvsdbNodeId> nodeIds = ovsController.getNodeIds().stream()
                        .filter(nodeID -> nodeID.getIpAddress().equals(targetIp))
                        .collect(Collectors.toList());
                if (nodeIds.size() == 0) {
                    //TODO decide what port?
                    ovsController.connect(IpAddress.valueOf(targetIp),
                                          targetPort == null ? TpPort.tpPort(OvsdbConstant.OVSDBPORT) : targetPort);
                    delay(1000); //FIXME... connect is async
                }
                List<OvsdbClientService> clientServices = ovsController.getNodeIds().stream()
                        .filter(nodeID -> nodeID.getIpAddress().equals(targetIp))
                        .map(ovsController::getOvsdbClient)
                        .filter(cs -> cs.getBridges().stream().anyMatch(b -> dpidMatches(b, deviceId)))
                        .collect(Collectors.toList());
                checkState(clientServices.size() > 0, "No clientServices found");
                //FIXME add connection to management address if null --> done ?
                return clientServices.size() > 0 ? clientServices.get(0) : null;
            default:
                log.warn("Unmanaged device type");
        }
        return null;
    }

    private static boolean dpidMatches(OvsdbBridge bridge, DeviceId deviceId) {
        checkArgument(bridge.datapathId().isPresent());

        String bridgeDpid = "of:" + bridge.datapathId().get();
        String ofDpid = deviceId.toString();
        return bridgeDpid.equals(ofDpid);
    }

    /**
     * OvsdbNodeId(IP) is used in the adaptor while DeviceId(ovsdb:IP)
     * is used in the core. So DeviceId need be changed to OvsdbNodeId.
     *
     * @param deviceId the device id in ovsdb:ip format
     * @return the ovsdb node id
     */
    private OvsdbNodeId changeDeviceIdToNodeId(DeviceId deviceId) {
        String[] splits = deviceId.toString().split(":");
        if (splits == null || splits.length < 1) {
            return null;
        }
        IpAddress ipAddress = IpAddress.valueOf(splits[1]);
        return new OvsdbNodeId(ipAddress, 0);
    }
}
