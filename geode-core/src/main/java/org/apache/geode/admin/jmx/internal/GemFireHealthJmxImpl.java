/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.admin.jmx.internal;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.modelmbean.ModelMBean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.DistributedSystemHealthConfig;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.admin.RuntimeAdminException;
import org.apache.geode.admin.internal.GemFireHealthImpl;
import org.apache.geode.internal.admin.GfManagerAgent;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * The JMX "managed resource" that represents the health of GemFire. Basically, it provides the
 * behavior of <code>GemFireHealthImpl</code>, but does some JMX stuff like registering beans with
 * the agent.
 *
 * @see AdminDistributedSystemJmxImpl#createGemFireHealth
 *
 *
 * @since GemFire 3.5
 */
public class GemFireHealthJmxImpl extends GemFireHealthImpl implements ManagedResource {

  private static final Logger logger = LogService.getLogger();

  /** The name of the MBean that will manage this resource */
  private final String mbeanName;

  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;

  /** The object name of the MBean created for this managed resource */
  private final ObjectName objectName;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>GemFireHealthJmxImpl</code> that monitors the health of the given
   * distributed system and uses the given JMX agent.
   */
  GemFireHealthJmxImpl(GfManagerAgent agent, AdminDistributedSystemJmxImpl system)
      throws AdminException {

    super(agent, system);
    mbeanName = MBEAN_NAME_PREFIX + "GemFireHealth,id="
        + MBeanUtils.makeCompliantMBeanNameProperty(system.getId());
    objectName = MBeanUtils.createMBean(this);
  }

  ////////////////////// Instance Methods //////////////////////

  public String getHealthStatus() {
    return getHealth().toString();
  }

  public ObjectName manageGemFireHealthConfig(String hostName) throws MalformedObjectNameException {
    try {
      GemFireHealthConfig config = getGemFireHealthConfig(hostName);
      GemFireHealthConfigJmxImpl jmx = (GemFireHealthConfigJmxImpl) config;
      return new ObjectName(jmx.getMBeanName());
    } catch (RuntimeException e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }

  /**
   * Creates a new {@link DistributedSystemHealthConfigJmxImpl}
   */
  @Override
  protected DistributedSystemHealthConfig createDistributedSystemHealthConfig() {

    try {
      return new DistributedSystemHealthConfigJmxImpl(this);

    } catch (AdminException ex) {
      throw new RuntimeAdminException(
          "While getting the DistributedSystemHealthConfig",
          ex);
    }
  }

  /**
   * Creates a new {@link GemFireHealthConfigJmxImpl}
   */
  @Override
  protected GemFireHealthConfig createGemFireHealthConfig(String hostName) {

    try {
      return new GemFireHealthConfigJmxImpl(this, hostName);

    } catch (AdminException ex) {
      throw new RuntimeAdminException(
          "While getting the GemFireHealthConfig",
          ex);
    }
  }

  /**
   * Ensures that the three primary Health MBeans are registered and returns their ObjectNames.
   */
  protected void ensureMBeansAreRegistered() {
    MBeanUtils.ensureMBeanIsRegistered(this);
    MBeanUtils.ensureMBeanIsRegistered((ManagedResource) defaultConfig);
    MBeanUtils.ensureMBeanIsRegistered((ManagedResource) dsHealthConfig);
  }

  @Override
  public String getMBeanName() {
    return mbeanName;
  }

  @Override
  public ModelMBean getModelMBean() {
    return modelMBean;
  }

  @Override
  public void setModelMBean(ModelMBean modelMBean) {
    this.modelMBean = modelMBean;
  }

  @Override
  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.GEMFIRE_HEALTH;
  }

  @Override
  public ObjectName getObjectName() {
    return objectName;
  }

  @Override
  public void cleanupResource() {
    close();
  }

}
