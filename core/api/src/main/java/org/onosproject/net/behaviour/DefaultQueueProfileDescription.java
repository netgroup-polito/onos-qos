package org.onosproject.net.behaviour;

import java.util.Objects;
import java.util.Optional;

public class DefaultQueueProfileDescription implements QueueProfileDescription {

    private final String name;
    private final Type type;
    private final Optional<Long> minRate;
    private final Optional<Long> maxRate;
    private final Optional<Long> burst;
    private final Optional<Long> priority;

    public DefaultQueueProfileDescription(String name, Type type, Optional<Long> minRate,
                                          Optional<Long> maxRate, Optional<Long> burst, Optional<Long> priority) {
        this.name = name;
        this.type = type;
        this.minRate = minRate;
        this.maxRate = maxRate;
        this.burst = burst;
        this.priority = priority;
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
    public Optional<Long> burst() {
        return burst;
    }

    @Override
    public Optional<Long> priority() {
        return priority;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DefaultQueueProfileDescription)) {
            return false;
        }

        DefaultQueueProfileDescription otherQueueProfile =
                (DefaultQueueProfileDescription) other;

        return Objects.equals(name, otherQueueProfile.name) &&
                Objects.equals(minRate, otherQueueProfile.minRate) &&
                Objects.equals(maxRate, otherQueueProfile.maxRate) &&
                Objects.equals(burst, otherQueueProfile.burst) &&
                Objects.equals(priority, otherQueueProfile.priority);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, minRate, maxRate, burst, priority);
    }
}
