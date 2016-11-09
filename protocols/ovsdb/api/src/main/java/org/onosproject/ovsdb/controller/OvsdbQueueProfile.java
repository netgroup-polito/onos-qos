package org.onosproject.ovsdb.controller;

import org.onosproject.net.behaviour.QueueProfileDescription;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class OvsdbQueueProfile {
    private final String name;
    private final QueueProfileDescription.Type type;
    private final Optional<Long> minRate;
    private final Optional<Long> maxRate;
    private final Optional<Long> burst;
    private final Optional<Long> priority;

    private OvsdbQueueProfile(String name, QueueProfileDescription.Type type, Optional<Long> minRate,
                              Optional<Long> maxRate, Optional<Long> burst, Optional<Long> priority) {
        this.name = name;
        this.type = type;
        this.minRate = minRate;
        this.maxRate = maxRate;
        this.burst = burst;
        this.priority = priority;
    }

    public String name() {
        return name;
    };
    public QueueProfileDescription.Type type() {
        return type;
    };
    public Optional<Long> minRate() {
        return minRate;
    };
    public Optional<Long> maxRate() {
        return maxRate;
    };
    public Optional<Long> burst() {
        return burst;
    };
    public Optional<Long> priority() {
        return priority;
    };

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof OvsdbQueueProfile) {
            final OvsdbQueueProfile ovsQueueProfile = (OvsdbQueueProfile) obj;
            return Objects.equals(this.name, ovsQueueProfile.name);
        }
        return false;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("minrate", minRate)
                .add("maxrate", maxRate)
                .add("burst", burst)
                .add("priority", priority)
                .toString();
    }

    public static OvsdbQueueProfile.Builder builder(QueueProfileDescription queueProfileDescr) {
        return new OvsdbQueueProfile.Builder(queueProfileDescr);
    }

    public static final class Builder {
        private String name;
        private QueueProfileDescription.Type type;
        private Optional<Long> minRate;
        private Optional<Long> maxRate;
        private Optional<Long> burst;
        private Optional<Long> priority;

        private Builder(QueueProfileDescription queueProfileDescr) {
            this.name = queueProfileDescr.name();
            this.type = queueProfileDescr.type();
            this.minRate = queueProfileDescr.minRate();
            this.maxRate = queueProfileDescr.maxRate();
            this.burst = queueProfileDescr.burst();
            this.priority = queueProfileDescr.priority();

        }

        public OvsdbQueueProfile.Builder name(String name) {
            this.name = name;
            return this;
        }

        public OvsdbQueueProfile.Builder type(QueueProfileDescription.Type type) {
            this.type = type;
            return this;
        }

        public OvsdbQueueProfile.Builder minRate(Long minRate) {
            this.minRate = Optional.ofNullable(minRate);
            return this;
        }
        public OvsdbQueueProfile.Builder maxRate(Long maxRate) {
            this.maxRate = Optional.ofNullable(maxRate);
            return this;
        }
        public OvsdbQueueProfile.Builder priority(Long priority) {
            this.priority = Optional.ofNullable(priority);
            return this;
        }
        public OvsdbQueueProfile.Builder burst(Long burst) {
            this.burst = Optional.ofNullable(burst);
            return this;
        }

        public OvsdbQueueProfile build() {
            return new OvsdbQueueProfile(name, type, minRate, maxRate, burst, priority);
        }
    }

}
