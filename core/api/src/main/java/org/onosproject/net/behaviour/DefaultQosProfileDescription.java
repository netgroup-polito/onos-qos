package org.onosproject.net.behaviour;

import java.util.Objects;
import java.util.Optional;

/**
 * Created by name29 on 27/10/16.
 */
public class DefaultQosProfileDescription implements QosProfileDescription {

    private final String name;
    private final Type type;
    private final Optional<Long> minRate;
    private final Optional<Long> maxRate;


    public DefaultQosProfileDescription(String name, Type type, Optional<Long> minRate,
                                          Optional<Long> maxRate) {
        this.name = name;
        this.type = type;
        this.minRate = minRate;
        this.maxRate = maxRate;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Type type() {
        return type;
    }


    @Override
    public Optional<Long> minRate() {
        return minRate;
    }

    @Override
    public Optional<Long> maxRate() {
        return maxRate;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DefaultQosProfileDescription)) {
            return false;
        }

        DefaultQosProfileDescription otherQosProfileDescription =
                (DefaultQosProfileDescription) other;

        return Objects.equals(name, otherQosProfileDescription.name) &&
                Objects.equals(minRate, otherQosProfileDescription.minRate) &&
                Objects.equals(maxRate, otherQosProfileDescription.maxRate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, minRate, maxRate);
    }
}
