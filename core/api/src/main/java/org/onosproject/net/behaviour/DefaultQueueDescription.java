package org.onosproject.net.behaviour;

import com.google.common.primitives.UnsignedInteger;
import org.onosproject.net.device.DeviceInterfaceDescription;

import java.util.Objects;


/**
 * Created by name29 on 27/10/16.
 */
public class DefaultQueueDescription implements QueueDescription {

    UnsignedInteger queueNumber;
    DeviceInterfaceDescription iface;
    QueueProfileDescription queueProfileDesc;


    public DefaultQueueDescription(UnsignedInteger queueNumber, DeviceInterfaceDescription iface,
                                   QueueProfileDescription queueProfileDesc) {
        this.queueNumber = queueNumber;
        this.iface = iface;
        this.queueProfileDesc = queueProfileDesc;
    }
    @Override
    public DeviceInterfaceDescription iface() {
        return iface;
    }

    @Override
    public UnsignedInteger queueNumber() {
        return queueNumber;
    }

    @Override
    public QueueProfileDescription queueProfile() {
        return queueProfileDesc;
    }


    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DefaultQueueDescription)) {
            return false;
        }

        DefaultQueueDescription otherQueueProfile =
                (DefaultQueueDescription) other;

        return Objects.equals(queueNumber, otherQueueProfile.queueNumber) &&
                Objects.equals(iface, otherQueueProfile.iface) &&
                Objects.equals(queueProfileDesc, otherQueueProfile.queueProfileDesc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queueNumber, iface, queueProfileDesc);
    }
}
