package org.onosproject.net.behaviour;

import java.util.Optional;


/*
*The description of an Queue Profile used for legacy devices.
*/
public interface QueueProfileDescription {
    enum Type {
        /**
         * Supports burst and priority as well as min and max rates.
         */
        FULL,

        /**
         * Only support min and max rates.
         */
        MINMAX
    }

    String name();
    Type type();
    Optional<Long> minRate();
    Optional<Long> maxRate();
    Optional<Long> burst();
    Optional<Long> priority();

}
