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
package org.apache.geode.admin.internal;

import static java.lang.System.lineSeparator;
import static org.apache.geode.admin.internal.InetAddressUtils.toHostString;
import static org.apache.geode.admin.internal.InetAddressUtilsWithLogging.validateHost;
import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_JMX;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_TCP;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.TCP_PORT;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.admin.AdminXmlException;
import org.apache.geode.admin.CacheServerConfig;
import org.apache.geode.admin.CacheVmConfig;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.admin.DistributionLocator;
import org.apache.geode.admin.DistributionLocatorConfig;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.internal.statistics.StatisticsConfig;
import org.apache.geode.logging.internal.log4j.LogLevel;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * An implementation of the configuration object for an <code>AdminDistributedSystem</code>. After a
 * config has been used to create an <code>AdminDistributedSystem</code> most of the configuration
 * attributes cannot be changed. However, some operations (such as getting information about GemFire
 * managers and distribution locators) are "passed through" to the
 * <code>AdminDistributedSystem</code> associated with this configuration object.
 *
 * @since GemFire 3.5
 */
public class DistributedSystemConfigImpl implements DistributedSystemConfig {

  private static final Logger logger = LogService.getLogger();

  private String entityConfigXMLFile = DEFAULT_ENTITY_CONFIG_XML_FILE;
  private String systemId = DEFAULT_SYSTEM_ID;
  private String mcastAddress = DEFAULT_MCAST_ADDRESS;
  private int mcastPort = DEFAULT_MCAST_PORT;
  private int ackWaitThreshold = DEFAULT_ACK_WAIT_THRESHOLD;
  private int ackSevereAlertThreshold = DEFAULT_ACK_SEVERE_ALERT_THRESHOLD;
  private String locators = DEFAULT_LOCATORS;
  private String bindAddress = DEFAULT_BIND_ADDRESS;
  private String serverBindAddress = DEFAULT_BIND_ADDRESS;
  private String remoteCommand = DEFAULT_REMOTE_COMMAND;
  private boolean disableTcp = DEFAULT_DISABLE_TCP;
  private boolean disableJmx = DEFAULT_DISABLE_JMX;
  private boolean enableNetworkPartitionDetection = DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION;
  private boolean disableAutoReconnect = DEFAULT_DISABLE_AUTO_RECONNECT;
  private int memberTimeout = DEFAULT_MEMBER_TIMEOUT;
  private String membershipPortRange = getMembershipPortRangeString(DEFAULT_MEMBERSHIP_PORT_RANGE);
  private int tcpPort = DEFAULT_TCP_PORT;

  private String logFile = DEFAULT_LOG_FILE;
  private String logLevel = DEFAULT_LOG_LEVEL;
  private int logDiskSpaceLimit = DEFAULT_LOG_DISK_SPACE_LIMIT;
  private int logFileSizeLimit = DEFAULT_LOG_FILE_SIZE_LIMIT;
  private int refreshInterval = DEFAULT_REFRESH_INTERVAL;
  private Properties gfSecurityProperties = new Properties();

  /**
   * Listeners to notify when this DistributedSystemConfig changes
   */
  private final Set listeners = new HashSet();

  /**
   * Configs for CacheServers that this system config is aware of
   */
  private Set cacheServerConfigs = new HashSet();

  /**
   * Configs for the managed distribution locators in the distributed system
   */
  private Set locatorConfigs = new HashSet();

  /**
   * The display name of this distributed system
   */
  private String systemName = DEFAULT_NAME;

  /**
   * The admin distributed system object that is configured by this config object.
   *
   * @since GemFire 4.0
   */
  private AdminDistributedSystemImpl system;

  /**
   * The GemFire log writer used by the distributed system
   */
  private InternalLogWriter logWriter;

  /////////////////////// Static Methods ///////////////////////

  /**
   * Filters out all properties that are unique to the admin <code>DistributedSystemConfig</code>
   * that are not present in the internal <code>DistributionConfig</code>.
   *
   * @since GemFire 4.0
   */
  private static Properties filterOutAdminProperties(Properties props) {

    Properties props2 = new Properties();
    for (Enumeration names = props.propertyNames(); names.hasMoreElements();) {
      String name = (String) names.nextElement();
      if (!(ENTITY_CONFIG_XML_FILE_NAME.equals(name) || REFRESH_INTERVAL_NAME.equals(name)
          || REMOTE_COMMAND_NAME.equals(name))) {
        String value = props.getProperty(name);
        if ((name != null) && (value != null)) {
          props2.setProperty(name, value);
        }
      }
    }

    return props2;
  }

  //////////////////////// Constructors ////////////////////////

  /**
   * Creates a new <code>DistributedSystemConfigImpl</code> based on the configuration stored in a
   * <code>DistributedSystem</code>'s <code>DistributionConfig</code>.
   */
  public DistributedSystemConfigImpl(DistributionConfig distConfig, String remoteCommand) {
    if (distConfig == null) {
      throw new IllegalArgumentException(
          "DistributionConfig must not be null.");
    }

    mcastAddress = toHostString(distConfig.getMcastAddress());
    mcastPort = distConfig.getMcastPort();
    locators = distConfig.getLocators();
    membershipPortRange = getMembershipPortRangeString(distConfig.getMembershipPortRange());

    systemName = distConfig.getName();

    sslEnabled = distConfig.getClusterSSLEnabled();
    sslCiphers = distConfig.getClusterSSLCiphers();
    sslProtocols = distConfig.getClusterSSLProtocols();
    sslAuthenticationRequired = distConfig.getClusterSSLRequireAuthentication();

    logFile = distConfig.getLogFile().getPath();
    logLevel = LogWriterImpl.levelToString(distConfig.getLogLevel());
    logDiskSpaceLimit = distConfig.getLogDiskSpaceLimit();
    logFileSizeLimit = distConfig.getLogFileSizeLimit();

    basicSetBindAddress(distConfig.getBindAddress());
    tcpPort = distConfig.getTcpPort();

    disableTcp = distConfig.getDisableTcp();

    this.remoteCommand = remoteCommand;
    serverBindAddress = distConfig.getServerBindAddress();
    enableNetworkPartitionDetection = distConfig.getEnableNetworkPartitionDetection();
    memberTimeout = distConfig.getMemberTimeout();
    refreshInterval = DistributedSystemConfig.DEFAULT_REFRESH_INTERVAL;
    gfSecurityProperties = (Properties) distConfig.getSSLProperties().clone();
  }

  /**
   * Zero-argument constructor to be used only by subclasses.
   *
   * @since GemFire 4.0
   */
  protected DistributedSystemConfigImpl() {

  }

  /**
   * Creates a new <code>DistributedSystemConifgImpl</code> whose configuration is specified by the
   * given <code>Properties</code> object.
   */
  protected DistributedSystemConfigImpl(Properties props) {
    this(props, false);
  }

  /**
   * Creates a new <code>DistributedSystemConifgImpl</code> whose configuration is specified by the
   * given <code>Properties</code> object.
   *
   * @param props The configuration properties specified by the caller
   * @param ignoreGemFirePropsFile whether to skip loading distributed system properties from
   *        gemfire.properties file
   *
   * @since GemFire 6.5
   */
  protected DistributedSystemConfigImpl(Properties props, boolean ignoreGemFirePropsFile) {
    this(new DistributionConfigImpl(filterOutAdminProperties(props), ignoreGemFirePropsFile),
        DEFAULT_REMOTE_COMMAND);
    String remoteCommand = props.getProperty(REMOTE_COMMAND_NAME);
    if (remoteCommand != null) {
      this.remoteCommand = remoteCommand;
    }

    String entityConfigXMLFile = props.getProperty(ENTITY_CONFIG_XML_FILE_NAME);
    if (entityConfigXMLFile != null) {
      this.entityConfigXMLFile = entityConfigXMLFile;
    }

    String refreshInterval = props.getProperty(REFRESH_INTERVAL_NAME);
    if (refreshInterval != null) {
      try {
        this.refreshInterval = Integer.parseInt(refreshInterval);
      } catch (NumberFormatException nfEx) {
        throw new IllegalArgumentException(
            String.format("%s is not a valid integer for %s",
                refreshInterval, REFRESH_INTERVAL_NAME));
      }
    }
  }

  ////////////////////// Instance Methods //////////////////////

  /**
   * Returns the <code>LogWriterI18n</code> to be used when administering the distributed system.
   * Returns null if nothing has been provided via <code>setInternalLogWriter</code>.
   *
   * @since GemFire 4.0
   */
  public InternalLogWriter getInternalLogWriter() {
    // LOG: used only for sharing between IDS, AdminDSImpl and AgentImpl -- to prevent multiple
    // banners, etc.
    synchronized (this) {
      return logWriter;
    }
  }

  /**
   * Sets the <code>LogWriterI18n</code> to be used when administering the distributed system.
   */
  public void setInternalLogWriter(InternalLogWriter logWriter) {
    // LOG: used only for sharing between IDS, AdminDSImpl and AgentImpl -- to prevent multiple
    // banners, etc.
    synchronized (this) {
      this.logWriter = logWriter;
    }
  }

  public LogConfig createLogConfig() {
    return new LogConfig() {
      @Override
      public int getLogLevel() {
        return LogLevel.getLogWriterLevel(DistributedSystemConfigImpl.this.getLogLevel());
      }

      @Override
      public File getLogFile() {
        return new File(DistributedSystemConfigImpl.this.getLogFile());
      }

      @Override
      public File getSecurityLogFile() {
        return null;
      }

      @Override
      public int getSecurityLogLevel() {
        return LogLevel.getLogWriterLevel(DistributedSystemConfigImpl.this.getLogLevel());
      }

      @Override
      public int getLogFileSizeLimit() {
        return DistributedSystemConfigImpl.this.getLogFileSizeLimit();
      }

      @Override
      public int getLogDiskSpaceLimit() {
        return DistributedSystemConfigImpl.this.getLogDiskSpaceLimit();
      }

      @Override
      public String getName() {
        return getSystemName();
      }

      @Override
      public String toLoggerString() {
        return DistributedSystemConfigImpl.this.toString();
      }

      @Override
      public boolean isLoner() {
        return false;
      }
    };
  }

  public StatisticsConfig createStatisticsConfig() {
    return new StatisticsConfig() {

      @Override
      public File getStatisticArchiveFile() {
        return null;
      }

      @Override
      public int getArchiveFileSizeLimit() {
        return 0;
      }

      @Override
      public int getArchiveDiskSpaceLimit() {
        return 0;
      }

      @Override
      public int getStatisticSampleRate() {
        return 0;
      }

      @Override
      public boolean getStatisticSamplingEnabled() {
        return false;
      }
    };
  }

  /**
   * Marks this config object as "read only". Attempts to modify a config object will result in a
   * {@link IllegalStateException} being thrown.
   *
   * @since GemFire 4.0
   */
  void setDistributedSystem(AdminDistributedSystemImpl system) {
    this.system = system;
  }

  /**
   * Checks to see if this config object is "read only". If it is, then an
   * {@link IllegalStateException} is thrown.
   *
   * @since GemFire 4.0
   */
  protected void checkReadOnly() {
    if (system != null) {
      throw new IllegalStateException(
          "A DistributedSystemConfig object cannot be modified after it has been used to create an AdminDistributedSystem.");
    }
  }

  @Override
  public String getEntityConfigXMLFile() {
    return entityConfigXMLFile;
  }

  @Override
  public void setEntityConfigXMLFile(String xmlFile) {
    checkReadOnly();
    entityConfigXMLFile = xmlFile;
    configChanged();
  }

  /**
   * Parses the XML configuration file that describes managed entities.
   *
   * @throws AdminXmlException If a problem is encountered while parsing the XML file.
   */
  private void parseEntityConfigXMLFile() {
    String fileName = entityConfigXMLFile;
    File xmlFile = new File(fileName);
    if (!xmlFile.exists()) {
      if (DEFAULT_ENTITY_CONFIG_XML_FILE.equals(fileName)) {
        // Default doesn't exist, no big deal
        return;
      } else {
        throw new AdminXmlException(
            String.format("Entity configuration XML file %s does not exist",
                fileName));
      }
    }

    try {
      InputStream is = new FileInputStream(xmlFile);
      try {
        ManagedEntityConfigXmlParser.parse(is, this);
      } finally {
        is.close();
      }
    } catch (IOException ex) {
      throw new AdminXmlException(
          String.format("While parsing %s", fileName),
          ex);
    }
  }

  @Override
  public String getSystemId() {
    return systemId;
  }

  @Override
  public void setSystemId(String systemId) {
    checkReadOnly();
    this.systemId = systemId;
    configChanged();
  }

  /**
   * Returns the multicast address for the system
   */
  @Override
  public String getMcastAddress() {
    return mcastAddress;
  }

  @Override
  public void setMcastAddress(String mcastAddress) {
    checkReadOnly();
    this.mcastAddress = mcastAddress;
    configChanged();
  }

  /**
   * Returns the multicast port for the system
   */
  @Override
  public int getMcastPort() {
    return mcastPort;
  }

  @Override
  public void setMcastPort(int mcastPort) {
    checkReadOnly();
    this.mcastPort = mcastPort;
    configChanged();
  }

  @Override
  public int getAckWaitThreshold() {
    return ackWaitThreshold;
  }

  @Override
  public void setAckWaitThreshold(int seconds) {
    checkReadOnly();
    ackWaitThreshold = seconds;
    configChanged();
  }

  @Override
  public int getAckSevereAlertThreshold() {
    return ackSevereAlertThreshold;
  }

  @Override
  public void setAckSevereAlertThreshold(int seconds) {
    checkReadOnly();
    ackSevereAlertThreshold = seconds;
    configChanged();
  }

  /**
   * Returns the comma-delimited list of locators for the system
   */
  @Override
  public String getLocators() {
    return locators;
  }

  @Override
  public void setLocators(String locators) {
    checkReadOnly();
    if (locators == null) {
      this.locators = "";
    } else {
      this.locators = locators;
    }
    configChanged();
  }

  /**
   * Returns the value for membership-port-range
   *
   * @return the value for the Distributed System property membership-port-range
   */
  @Override
  public String getMembershipPortRange() {
    return membershipPortRange;
  }

  /**
   * Sets the Distributed System property membership-port-range
   *
   * @param membershipPortRangeStr the value for membership-port-range given as two numbers
   *        separated by a minus sign.
   */
  @Override
  public void setMembershipPortRange(String membershipPortRangeStr) {
    /*
     * FIXME: Setting attributes in DistributedSystemConfig has no effect on DistributionConfig
     * which is actually used for connection with DS. This is true for all such attributes. Should
     * be addressed in the Admin Revamp if we want these 'set' calls to affect anything. Then we can
     * use the validation code in DistributionConfigImpl code.
     */
    checkReadOnly();
    if (membershipPortRangeStr == null) {
      membershipPortRange = getMembershipPortRangeString(DEFAULT_MEMBERSHIP_PORT_RANGE);
    } else {
      try {
        if (validateMembershipRange(membershipPortRangeStr)) {
          membershipPortRange = membershipPortRangeStr;
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "The value specified %s is invalid for the property : %s. This range should be specified as min-max.",

                  membershipPortRangeStr, MEMBERSHIP_PORT_RANGE_NAME));
        }
      } catch (Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug(e.getMessage(), e);
        }
      }
    }
  }

  @Override
  public void setTcpPort(int port) {
    checkReadOnly();
    tcpPort = port;
    configChanged();
  }

  @Override
  public int getTcpPort() {
    return tcpPort;
  }

  /**
   * Validates the given string - which is expected in the format as two numbers separated by a
   * minus sign - in to an integer array of length 2 with first element as lower end & second
   * element as upper end of the range.
   *
   * @param membershipPortRange membership-port-range given as two numbers separated by a minus
   *        sign.
   * @return true if the membership-port-range string is valid, false otherwise
   */
  private boolean validateMembershipRange(String membershipPortRange) {
    int[] range = null;
    if (membershipPortRange != null && membershipPortRange.trim().length() > 0) {
      String[] splitted = membershipPortRange.split("-");
      range = new int[2];
      range[0] = Integer.parseInt(splitted[0].trim());
      range[1] = Integer.parseInt(splitted[1].trim());
      // NumberFormatException if any could be thrown

      if (range[0] < 0 || range[0] >= range[1] || range[1] < 0 || range[1] > 65535) {
        range = null;
      }
    }
    return range != null;
  }

  /**
   * @return the String representation of membershipPortRange with lower & upper limits of the port
   *         range separated by '-' e.g. 1-65535
   */
  private static String getMembershipPortRangeString(int[] membershipPortRange) {
    String membershipPortRangeString = "";
    if (membershipPortRange != null && membershipPortRange.length == 2) {
      membershipPortRangeString = membershipPortRange[0] + "-" + membershipPortRange[1];
    }

    return membershipPortRangeString;
  }

  @Override
  public String getBindAddress() {
    return bindAddress;
  }

  @Override
  public void setBindAddress(String bindAddress) {
    checkReadOnly();
    basicSetBindAddress(bindAddress);
    configChanged();
  }

  @Override
  public String getServerBindAddress() {
    return serverBindAddress;
  }

  @Override
  public void setServerBindAddress(String bindAddress) {
    checkReadOnly();
    basicSetServerBindAddress(bindAddress);
    configChanged();
  }

  @Override
  public boolean getDisableTcp() {
    return disableTcp;
  }

  @Override
  public void setDisableTcp(boolean flag) {
    checkReadOnly();
    disableTcp = flag;
    configChanged();
  }

  @Override
  public boolean getDisableJmx() {
    return disableJmx;
  }

  @Override
  public void setDisableJmx(boolean flag) {
    checkReadOnly();
    disableJmx = flag;
    configChanged();
  }

  @Override
  public void setEnableNetworkPartitionDetection(boolean newValue) {
    checkReadOnly();
    enableNetworkPartitionDetection = newValue;
    configChanged();
  }

  @Override
  public boolean getEnableNetworkPartitionDetection() {
    return enableNetworkPartitionDetection;
  }

  @Override
  public void setDisableAutoReconnect(boolean newValue) {
    checkReadOnly();
    disableAutoReconnect = newValue;
    configChanged();
  }

  @Override
  public boolean getDisableAutoReconnect() {
    return disableAutoReconnect;
  }

  @Override
  public int getMemberTimeout() {
    return memberTimeout;
  }

  @Override
  public void setMemberTimeout(int value) {
    checkReadOnly();
    memberTimeout = value;
    configChanged();
  }

  private void basicSetBindAddress(String bindAddress) {
    if (!validateBindAddress(bindAddress)) {
      throw new IllegalArgumentException(
          String.format("Invalid bind address: %s",
              bindAddress));
    }
    this.bindAddress = bindAddress;
  }

  private void basicSetServerBindAddress(String bindAddress) {
    if (!validateBindAddress(bindAddress)) {
      throw new IllegalArgumentException(
          String.format("Invalid bind address: %s",
              bindAddress));
    }
    serverBindAddress = bindAddress;
  }

  /**
   * Returns the remote command setting to use for remote administration
   */
  @Override
  public String getRemoteCommand() {
    return remoteCommand;
  }

  /**
   * Sets the remote command for this config object. This attribute may be modified after this
   * config object has been used to create an admin distributed system.
   */
  @Override
  public void setRemoteCommand(String remoteCommand) {
    if (!ALLOW_ALL_REMOTE_COMMANDS) {
      checkRemoteCommand(remoteCommand);
    }
    this.remoteCommand = remoteCommand;
    configChanged();
  }

  private static final boolean ALLOW_ALL_REMOTE_COMMANDS =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "admin.ALLOW_ALL_REMOTE_COMMANDS");
  private static final String[] LEGAL_REMOTE_COMMANDS = {"rsh", "ssh"};
  private static final String ILLEGAL_REMOTE_COMMAND_RSH_OR_SSH =
      "Allowed remote commands include \"rsh {HOST} {CMD}\" or \"ssh {HOST} {CMD}\" with valid rsh or ssh switches. Invalid: ";

  private void checkRemoteCommand(final String remoteCommand) {
    if (remoteCommand == null || remoteCommand.isEmpty()) {
      return;
    }
    final String command = remoteCommand.toLowerCase().trim();
    if (!command.contains("{host}") || !command.contains("{cmd}")) {
      throw new IllegalArgumentException(ILLEGAL_REMOTE_COMMAND_RSH_OR_SSH + remoteCommand);
    }

    final StringTokenizer tokenizer = new StringTokenizer(command, " ");
    final ArrayList<String> array = new ArrayList<>();
    for (int i = 0; tokenizer.hasMoreTokens(); i++) {
      String string = tokenizer.nextToken();
      if (i == 0) {
        // first element must be rsh or ssh
        boolean found = false;
        for (final String legalRemoteCommand : LEGAL_REMOTE_COMMANDS) {
          if (string.contains(legalRemoteCommand)) {
            // verify command is at end of string
            if (!(string.endsWith(legalRemoteCommand)
                || string.endsWith(legalRemoteCommand + ".exe"))) {
              throw new IllegalArgumentException(ILLEGAL_REMOTE_COMMAND_RSH_OR_SSH + remoteCommand);
            }
            found = true;
          }
        }
        if (!found) {
          throw new IllegalArgumentException(ILLEGAL_REMOTE_COMMAND_RSH_OR_SSH + remoteCommand);
        }
      } else {
        final boolean isSwitch = string.startsWith("-");
        final boolean isHostOrCmd = string.equals("{host}") || string.equals("{cmd}");

        // additional elements must be switches or values-for-switches or {host} or user@{host} or
        // {cmd}
        if (!isSwitch && !isHostOrCmd) {
          final String previous =
              array.isEmpty() ? null : array.get(array.size() - 1);
          final boolean isValueForSwitch = previous != null && previous.startsWith("-");
          final boolean isHostWithUser = string.contains("@") && string.endsWith("{host}");

          if (!(isValueForSwitch || isHostWithUser)) {
            throw new IllegalArgumentException(ILLEGAL_REMOTE_COMMAND_RSH_OR_SSH + remoteCommand);
          }
        }
      }
      array.add(string);
    }
  }

  @Override
  public String getSystemName() {
    return systemName;
  }

  @Override
  public void setSystemName(final String systemName) {
    checkReadOnly();
    this.systemName = systemName;
    configChanged();
  }

  /**
   * Returns an array of configurations for statically known CacheServers
   *
   * @since GemFire 4.0
   */
  @Override
  public CacheServerConfig[] getCacheServerConfigs() {
    return (CacheServerConfig[]) cacheServerConfigs
        .toArray(new CacheServerConfig[0]);
  }

  @Override
  public CacheVmConfig[] getCacheVmConfigs() {
    return (CacheVmConfig[]) cacheServerConfigs
        .toArray(new CacheVmConfig[0]);
  }

  /**
   * Creates the configuration for a CacheServer
   *
   * @since GemFire 4.0
   */
  @Override
  public CacheServerConfig createCacheServerConfig() {
    CacheServerConfig config = new CacheServerConfigImpl();
    addCacheServerConfig(config);
    return config;
  }

  @Override
  public CacheVmConfig createCacheVmConfig() {
    return createCacheServerConfig();
  }

  /**
   * Adds the configuration for a CacheServer
   *
   * @since GemFire 4.0
   */
  private void addCacheServerConfig(CacheServerConfig managerConfig) {
    checkReadOnly();

    if (managerConfig == null) {
      return;
    }
    for (final Object cacheServerConfig : cacheServerConfigs) {
      CacheServerConfigImpl impl = (CacheServerConfigImpl) cacheServerConfig;
      if (impl.equals(managerConfig)) {
        return;
      }
    }
    cacheServerConfigs.add(managerConfig);
    configChanged();
  }

  /**
   * Removes the configuration for a CacheServer
   *
   * @since GemFire 4.0
   */
  @Override
  public void removeCacheServerConfig(CacheServerConfig managerConfig) {
    removeCacheVmConfig(managerConfig);
  }

  @Override
  public void removeCacheVmConfig(CacheVmConfig managerConfig) {
    checkReadOnly();
    cacheServerConfigs.remove(managerConfig);
    configChanged();
  }

  /**
   * Returns the configurations of all managed distribution locators
   */
  @Override
  public DistributionLocatorConfig[] getDistributionLocatorConfigs() {
    if (system != null) {
      DistributionLocator[] locators = system.getDistributionLocators();
      DistributionLocatorConfig[] configs = new DistributionLocatorConfig[locators.length];
      for (int i = 0; i < locators.length; i++) {
        configs[i] = locators[i].getConfig();
      }
      return configs;

    } else {
      Object[] array = new DistributionLocatorConfig[locatorConfigs.size()];
      return (DistributionLocatorConfig[]) locatorConfigs.toArray(array);
    }
  }

  /**
   * Creates the configuration for a DistributionLocator
   */
  @Override
  public DistributionLocatorConfig createDistributionLocatorConfig() {
    checkReadOnly();
    DistributionLocatorConfig config = new DistributionLocatorConfigImpl();
    addDistributionLocatorConfig(config);
    return config;
  }

  /**
   * Adds the configuration for a DistributionLocator
   */
  private void addDistributionLocatorConfig(DistributionLocatorConfig config) {
    checkReadOnly();
    locatorConfigs.add(config);
    configChanged();
  }

  /**
   * Removes the configuration for a DistributionLocator
   */
  @Override
  public void removeDistributionLocatorConfig(DistributionLocatorConfig config) {
    checkReadOnly();
    locatorConfigs.remove(config);
    configChanged();
  }

  /**
   * Validates the bind address. The address may be a host name or IP address, but it must not be
   * empty and must be usable for creating an InetAddress. Cannot have a leading '/' (which
   * InetAddress.toString() produces).
   *
   * @param bindAddress host name or IP address to validate
   */
  public static boolean validateBindAddress(String bindAddress) {
    if (bindAddress == null || bindAddress.length() == 0) {
      return true;
    }
    return validateHost(bindAddress) != null;
  }

  public synchronized void configChanged() {
    ConfigListener[] clients = null;
    synchronized (listeners) {
      clients = (ConfigListener[]) listeners.toArray(new ConfigListener[listeners.size()]);
    }
    for (final ConfigListener client : clients) {
      try {
        client.configChanged(this);
      } catch (Exception e) {
        logger.warn(e.getMessage(), e);
      }
    }
  }

  /**
   * Registers listener for notification of changes in this config.
   */
  @Override
  public void addListener(ConfigListener listener) {
    synchronized (listeners) {
      listeners.add(listener);
    }
  }

  /**
   * Removes previously registered listener of this config.
   */
  @Override
  public void removeListener(ConfigListener listener) {
    synchronized (listeners) {
      listeners.remove(listener);
    }
  }

  // -------------------------------------------------------------------------
  // SSL support...
  // -------------------------------------------------------------------------
  private boolean sslEnabled = DistributionConfig.DEFAULT_SSL_ENABLED;
  private String sslProtocols = DistributionConfig.DEFAULT_SSL_PROTOCOLS;
  private String sslCiphers = DistributionConfig.DEFAULT_SSL_CIPHERS;
  private boolean sslAuthenticationRequired = DistributionConfig.DEFAULT_SSL_REQUIRE_AUTHENTICATION;
  private Properties sslProperties = new Properties();

  @Override
  public boolean isSSLEnabled() {
    return sslEnabled;
  }

  @Override
  public void setSSLEnabled(boolean enabled) {
    checkReadOnly();
    sslEnabled = enabled;
    configChanged();
  }

  @Override
  public String getSSLProtocols() {
    return sslProtocols;
  }

  @Override
  public void setSSLProtocols(String protocols) {
    checkReadOnly();
    sslProtocols = protocols;
    configChanged();
  }

  @Override
  public String getSSLCiphers() {
    return sslCiphers;
  }

  @Override
  public void setSSLCiphers(String ciphers) {
    checkReadOnly();
    sslCiphers = ciphers;
    configChanged();
  }

  @Override
  public boolean isSSLAuthenticationRequired() {
    return sslAuthenticationRequired;
  }

  @Override
  public void setSSLAuthenticationRequired(boolean authRequired) {
    checkReadOnly();
    sslAuthenticationRequired = authRequired;
    configChanged();
  }

  @Override
  public Properties getSSLProperties() {
    return sslProperties;
  }

  @Override
  public void setSSLProperties(Properties sslProperties) {
    checkReadOnly();
    this.sslProperties = sslProperties;
    if (this.sslProperties == null) {
      this.sslProperties = new Properties();
    }
    configChanged();
  }

  @Override
  public void addSSLProperty(String key, String value) {
    checkReadOnly();
    sslProperties.put(key, value);
    configChanged();
  }

  @Override
  public void removeSSLProperty(String key) {
    checkReadOnly();
    sslProperties.remove(key);
    configChanged();
  }

  /**
   * @return the gfSecurityProperties
   * @since GemFire 6.6.3
   */
  public Properties getGfSecurityProperties() {
    return gfSecurityProperties;
  }

  @Override
  public String getLogFile() {
    return logFile;
  }

  @Override
  public void setLogFile(String logFile) {
    checkReadOnly();
    this.logFile = logFile;
    configChanged();
  }

  @Override
  public String getLogLevel() {
    return logLevel;
  }

  @Override
  public void setLogLevel(String logLevel) {
    checkReadOnly();
    this.logLevel = logLevel;
    configChanged();
  }

  @Override
  public int getLogDiskSpaceLimit() {
    return logDiskSpaceLimit;
  }

  @Override
  public void setLogDiskSpaceLimit(int limit) {
    checkReadOnly();
    logDiskSpaceLimit = limit;
    configChanged();
  }

  @Override
  public int getLogFileSizeLimit() {
    return logFileSizeLimit;
  }

  @Override
  public void setLogFileSizeLimit(int limit) {
    checkReadOnly();
    logFileSizeLimit = limit;
    configChanged();
  }

  /**
   * Returns the refreshInterval in seconds
   */
  @Override
  public int getRefreshInterval() {
    return refreshInterval;
  }

  /**
   * Sets the refreshInterval in seconds
   */
  @Override
  public void setRefreshInterval(int timeInSecs) {
    checkReadOnly();
    refreshInterval = timeInSecs;
    configChanged();
  }

  /**
   * Makes sure that the mcast port and locators are correct and consistent.
   *
   * @throws IllegalArgumentException If configuration is not valid
   */
  @Override
  public void validate() {
    if (getMcastPort() < MIN_MCAST_PORT || getMcastPort() > MAX_MCAST_PORT) {
      throw new IllegalArgumentException(
          String.format("mcastPort must be an integer inclusively between %s and %s",

              MIN_MCAST_PORT, MAX_MCAST_PORT));
    }

    LogLevel.getLogWriterLevel(logLevel);

    if (logFileSizeLimit < MIN_LOG_FILE_SIZE_LIMIT
        || logFileSizeLimit > MAX_LOG_FILE_SIZE_LIMIT) {
      throw new IllegalArgumentException(
          String.format("LogFileSizeLimit must be an integer between %s and %s",
              MIN_LOG_FILE_SIZE_LIMIT,
              MAX_LOG_FILE_SIZE_LIMIT));
    }

    if (logDiskSpaceLimit < MIN_LOG_DISK_SPACE_LIMIT
        || logDiskSpaceLimit > MAX_LOG_DISK_SPACE_LIMIT) {
      throw new IllegalArgumentException(
          String.format("LogDiskSpaceLimit must be an integer between %s and %s",
              MIN_LOG_DISK_SPACE_LIMIT,
              MAX_LOG_DISK_SPACE_LIMIT));
    }

    parseEntityConfigXMLFile();
  }

  /**
   * Makes a deep copy of this config object.
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    DistributedSystemConfigImpl other = (DistributedSystemConfigImpl) super.clone();
    other.system = null;
    other.cacheServerConfigs = new HashSet();
    other.locatorConfigs = new HashSet();

    DistributionLocatorConfig[] myLocators = getDistributionLocatorConfigs();
    for (DistributionLocatorConfig locator : myLocators) {
      other.addDistributionLocatorConfig((DistributionLocatorConfig) locator.clone());
    }

    CacheServerConfig[] myCacheServers = getCacheServerConfigs();
    for (CacheServerConfig locator : myCacheServers) {
      other.addCacheServerConfig((CacheServerConfig) locator.clone());
    }

    return other;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder(1000);
    String lf = lineSeparator();
    if (lf == null) {
      lf = ",";
    }

    buf.append("DistributedSystemConfig(");
    buf.append(lf);
    buf.append("  system-name=");
    buf.append(systemName);
    buf.append(lf);
    buf.append("  " + MCAST_ADDRESS + "=");
    buf.append(mcastAddress);
    buf.append(lf);
    buf.append("  " + MCAST_PORT + "=");
    buf.append(mcastPort);
    buf.append(lf);
    buf.append("  " + LOCATORS + "=");
    buf.append(locators);
    buf.append(lf);
    buf.append("  " + MEMBERSHIP_PORT_RANGE_NAME + "=");
    buf.append(getMembershipPortRange());
    buf.append(lf);
    buf.append("  " + BIND_ADDRESS + "=");
    buf.append(bindAddress);
    buf.append(lf);
    buf.append("  " + TCP_PORT + "=" + tcpPort);
    buf.append(lf);
    buf.append("  " + DISABLE_TCP + "=");
    buf.append(disableTcp);
    buf.append(lf);
    buf.append("  " + DISABLE_JMX + "=");
    buf.append(disableJmx);
    buf.append(lf);
    buf.append("  " + DISABLE_AUTO_RECONNECT + "=");
    buf.append(disableAutoReconnect);
    buf.append(lf);
    buf.append("  " + REMOTE_COMMAND_NAME + "=");
    buf.append(remoteCommand);
    buf.append(lf);
    buf.append("  " + CLUSTER_SSL_ENABLED + "=");
    buf.append(sslEnabled);
    buf.append(lf);
    buf.append("  " + CLUSTER_SSL_CIPHERS + "=");
    buf.append(sslCiphers);
    buf.append(lf);
    buf.append("  " + CLUSTER_SSL_PROTOCOLS + "=");
    buf.append(sslProtocols);
    buf.append(lf);
    buf.append("  " + CLUSTER_SSL_REQUIRE_AUTHENTICATION + "=");
    buf.append(sslAuthenticationRequired);
    buf.append(lf);
    buf.append("  " + LOG_FILE_NAME + "=");
    buf.append(logFile);
    buf.append(lf);
    buf.append("  " + LOG_LEVEL_NAME + "=");
    buf.append(logLevel);
    buf.append(lf);
    buf.append("  " + LOG_DISK_SPACE_LIMIT_NAME + "=");
    buf.append(logDiskSpaceLimit);
    buf.append(lf);
    buf.append("  " + LOG_FILE_SIZE_LIMIT_NAME + "=");
    buf.append(logFileSizeLimit);
    buf.append(lf);
    buf.append("  " + REFRESH_INTERVAL_NAME + "=");
    buf.append(refreshInterval);
    buf.append(")");
    return buf.toString();
  }
}
