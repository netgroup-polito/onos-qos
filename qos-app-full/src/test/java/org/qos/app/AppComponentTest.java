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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.onosproject.cfg.ComponentConfigAdapter;
import org.onosproject.core.CoreServiceAdapter;
import org.onosproject.net.device.DeviceServiceAdapter;
import org.onosproject.net.driver.DriverServiceAdapter;
import org.onosproject.net.flow.FlowRuleServiceAdapter;

/**
 * Set of tests of the ONOS application component.
 */
public class AppComponentTest {

    private AppComponent component;

    @Before
    public void setUp() {
        component = new AppComponent();
        component.cfgService = new ComponentConfigAdapter();
        component.coreService =  new CoreServiceAdapter();
        component.deviceService = new DeviceServiceAdapter();
        component.flowRuleService = new FlowRuleServiceAdapter();
        component.driverService = new DriverServiceAdapter();
        component.activate();

    }

    @After
    public void tearDown() {
        component.deactivate();
    }

    @Test
    public void basics() {

    }

}
