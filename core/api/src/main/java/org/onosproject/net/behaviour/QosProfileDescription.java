package org.onosproject.net.behaviour;

import java.util.Optional;

public interface QosProfileDescription {
    enum Type {
        LINUX_HTB
    }

    String name();
    Type type();
    Optional<Long> minRate();
    Optional<Long> maxRate();

}
