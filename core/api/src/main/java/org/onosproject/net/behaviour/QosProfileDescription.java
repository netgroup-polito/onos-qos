package org.onosproject.net.behaviour;

import java.util.Optional;

/**
 * Created by Emanuele Fia on 27/10/16.
 */
public interface QosProfileDescription {
    enum Type {
        LINUX_HTB
    }

    String name();
    Type type();
    Optional<Long> minRate();
    Optional<Long> maxRate();

}
