package org.onosproject.ovsdb.controller;

import org.onosproject.net.behaviour.QosProfileDescription;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Created by name29 on 27/10/16.
 */
public final class OvsdbQosProfile {

    private final String name;
    private final QosProfileDescription.Type type;
    private final Optional<Long> minRate;
    private final Optional<Long> maxRate;


    private OvsdbQosProfile(String name, QosProfileDescription.Type type, Optional<Long> minRate,
                            Optional<Long> maxRate) {
        this.name = name;
        this.type = type;
        this.minRate = minRate;
        this.maxRate = maxRate;
    }

    public String name() {
        return name;
    };

    public QosProfileDescription.Type type() {
        return type;
    };

    public Optional<Long> minRate() {
        return minRate;
    };

    public Optional<Long> maxRate() {
        return maxRate;
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
        if (obj instanceof OvsdbQosProfile) {
            final OvsdbQosProfile ovsQosProfile = (OvsdbQosProfile) obj;
            return Objects.equals(this.name, ovsQosProfile.name);
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
                .toString();
    }

    public static OvsdbQosProfile.Builder builder(QosProfileDescription qosProfileDescr) {
        return new OvsdbQosProfile.Builder(qosProfileDescr);
    }

    public String typeToString() {
        switch (type) {
            case LINUX_HTB:
                return "linux-htb";
            default:
                return "invalid";
        }
    }

    public static final class Builder {
        private String name;
        private QosProfileDescription.Type type;
        private Optional<Long> minRate;
        private Optional<Long> maxRate;

        public Builder(QosProfileDescription qosProfileDescr) {
            this.name = qosProfileDescr.name();
            this.type = qosProfileDescr.type();
            this.minRate = qosProfileDescr.minRate();
            this.maxRate = qosProfileDescr.maxRate();

        }

        public OvsdbQosProfile.Builder name(String name) {
            this.name = name;
            return this;
        }

        public OvsdbQosProfile.Builder type(QosProfileDescription.Type type) {
            this.type = type;
            return this;
        }

        public OvsdbQosProfile.Builder minRate(Long minRate) {
            this.minRate = Optional.ofNullable(minRate);
            return this;
        }
        public OvsdbQosProfile.Builder maxRate(Long maxRate) {
            this.maxRate = Optional.ofNullable(maxRate);
            return this;
        }

        public OvsdbQosProfile build() {
            return new OvsdbQosProfile(name, type, minRate, maxRate);
        }
    }

}
