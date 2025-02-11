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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Used to share code with BridgeServerAdvisor and ControllerAdvisor
 */
public abstract class GridAdvisor extends DistributionAdvisor {

  /** Creates a new instance of GridAdvisor */
  protected GridAdvisor(DistributionAdvisee server) {
    super(server);
  }

  private final Object cacheLock = new Object();

  private volatile List/* <BridgeServerProfile> */ cachedBridgeServerProfiles;

  private volatile List/* <ControllerProfile> */ cachedControllerProfiles;

  private volatile Set/* <DistributedMember> */ cachedBridgeServerAdvise;

  private volatile Set/* <DistributedMember> */ cachedControllerAdvise;

  @Immutable
  private static final Filter CONTROLLER_FILTER =
      profile -> profile instanceof ControllerAdvisor.ControllerProfile;

  @Immutable
  private static final Filter BRIDGE_SERVER_FILTER =
      profile -> profile instanceof CacheServerAdvisor.CacheServerProfile;

  /**
   * Return an unmodifiable Set<DistributedMember> of the cnx controllers in this system.
   */
  public Set adviseControllers() {
    Set/* <DistributedMember> */ result = cachedControllerAdvise;
    if (result == null) {
      synchronized (cacheLock) {
        result = cachedControllerAdvise;
        if (result == null) {
          result = Collections.unmodifiableSet(adviseFilter(CONTROLLER_FILTER));
          cachedControllerAdvise = result;
        }
      }
    }
    return result;
  }

  /**
   * Return an unmodifiable Set<DistributedMember> of the cache servers in this system.
   */
  public Set adviseBridgeServers() {
    Set/* <DistributedMember> */ result = cachedBridgeServerAdvise;
    if (result == null) {
      synchronized (cacheLock) {
        result = cachedBridgeServerAdvise;
        if (result == null) {
          result = Collections.unmodifiableSet(adviseFilter(BRIDGE_SERVER_FILTER));
          cachedBridgeServerAdvise = result;
        }
      }
    }
    return result;
  }

  /**
   * Returns an unmodifiable {@code List} of the {@code BridgeServerProfile}s for all known bridge
   * servers.
   */
  public List/* <BridgeServerProfile> */ fetchBridgeServers() {
    List/* <BridgeServerProfile> */ result = null;
    // TODO: remove double-checking
    if (result == null) {
      synchronized (cacheLock) {
        // result = this.cachedBridgeServerProfiles;
        if (result == null) {
          result = fetchProfiles(BRIDGE_SERVER_FILTER);
          cachedBridgeServerProfiles = result;
        }
      }
    }
    return result;
  }

  /**
   * Returns an unmodifiable {@code List} of the {@code ControllerProfile}s for all known cnx
   * controllers.
   */
  public List/* <ControllerProfile> */ fetchControllers() {
    List/* <ControllerProfile> */ result = cachedControllerProfiles;
    if (result == null) {
      synchronized (cacheLock) {
        result = cachedControllerProfiles;
        if (result == null) {
          result = fetchProfiles(CONTROLLER_FILTER);
          cachedControllerProfiles = result;
        }
      }
    }
    return result;
  }

  public int getBridgeServerCount() {
    List/* <BridgeServerProfile> */ l = cachedBridgeServerProfiles;
    if (l == null) {
      l = fetchProfiles(BRIDGE_SERVER_FILTER);
    }
    return l.size();
  }

  public int getControllerCount() {
    List/* <ControllerProfile> */ l = cachedControllerProfiles;
    if (l == null) {
      l = fetchProfiles(CONTROLLER_FILTER);
    }
    return l.size();
  }

  /**
   * Need ALL others (both normal members and admin members).
   */
  @Override
  public boolean useAdminMembersForDefault() {
    return true;
  }

  /**
   * Just always return true. This method could use getAllOtherMembers but it would cause us to make
   * lots of copies of the member set. The problem with our super.isCurrentMember is it ignores
   * admin members.
   */
  @Override
  protected boolean isCurrentMember(Profile p) {
    return true;
  }

  @Override
  protected void profileCreated(Profile profile) {
    profilesChanged();
  }

  @Override
  protected void profileUpdated(Profile profile) {
    profilesChanged();
  }

  @Override
  protected void profileRemoved(Profile profile) {
    profilesChanged();
  }

  /**
   * Used to drop any cached profile views we have since the master list of profiles changed. The
   * next time someone asks for a view it will be recomputed.
   */
  protected void profilesChanged() {
    if (pollIsInitialized()) {
      // no need to synchronize here since all cached* fields are volatile
      cachedBridgeServerProfiles = null;
      cachedControllerProfiles = null;
      cachedBridgeServerAdvise = null;
      cachedControllerAdvise = null;
    }
  }

  /**
   * Tell everyone else who we are and find out who they are.
   */
  public void handshake() {
    if (initializationGate()) {
      // Exchange with any local servers or controllers.
      List<Profile> otherProfiles = new ArrayList<>();
      GridProfile profile = (GridProfile) createProfile();
      profile.tellLocalBridgeServers(getDistributionManager().getCache(), false, true,
          otherProfiles);
      profile.tellLocalControllers(false, true, otherProfiles);
      for (Profile otherProfile : otherProfiles) {
        if (!otherProfile.equals(profile)) {
          putProfile(otherProfile);
        }
      }
    }
    profilesChanged();
  }

  @Override
  public void close() {
    try {
      new UpdateAttributesProcessor(getAdvisee(), true/* removeProfile */).distribute();

      // Notify any local cache servers or controllers
      // that we are closing.
      GridProfile profile = (GridProfile) createProfile();
      profile.tellLocalBridgeServers(getDistributionManager().getCache(), true, false, null);
      profile.tellLocalControllers(true, false, null);
      super.close();
    } catch (DistributedSystemDisconnectedException ignore) {
      // we are closing so ignore a shutdown exception.
    }
    profilesChanged();
  }

  @Override
  public Set adviseProfileRemove() {
    // Our set of profiles includes local members. However, the update
    // attributes message doesn't seem to be able to handle being sent to local
    // members
    Set results = super.adviseProfileRemove();
    results.remove(getDistributionManager().getId());
    return results;
  }

  /**
   * Describes profile data common for all Grid resources
   */
  public abstract static class GridProfile extends DistributionAdvisor.Profile {

    private String host;

    /**
     * a negative port value is used when creating a fake profile meant to only gather information
     * about all available locators.
     */
    private int port;

    private ProfileId id;

    /** for internal use, required for DataSerializer.readObject */
    public GridProfile() {}

    public GridProfile(InternalDistributedMember memberId, int version) {
      super(memberId, version);
    }

    public GridProfile(GridProfile toCopy) {
      super(toCopy.getDistributedMember(), toCopy.version);
      host = toCopy.host;
      port = toCopy.port;
      finishInit();
    }

    public void setHost(String host) {
      this.host = host;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    @Override
    public ProfileId getId() {
      if (id == null) {
        throw new IllegalStateException("profile id not yet initialized");
      }
      return id;
    }

    /**
     * Tell local controllers about the received profile. Also if exchange profiles then add each
     * local controller to reply.
     *
     * @since GemFire 5.7
     */
    protected void tellLocalControllers(boolean removeProfile, boolean exchangeProfiles,
        final List<Profile> replyProfiles) {
      final List<Locator> locators = Locator.getLocators();
      for (Locator locator : locators) {
        InternalLocator l = (InternalLocator) locator;
        DistributionAdvisee advisee = l.getServerLocatorAdvisee();
        if (advisee != null && advisee.getProfile().equals(this)) {
          continue;
        }
        // negative value for port indicates fake profile
        // meant to only gather remote profiles during profile exchange
        if (port > 0) {
          handleDistributionAdvisee(advisee, removeProfile, exchangeProfiles, replyProfiles);
        } else if (exchangeProfiles && advisee != null) {
          replyProfiles.add(advisee.getProfile());
        }
      }
    }

    /**
     * Tell local cache servers about the received profile. Also if exchange profiles then add each
     * local cache server to reply.
     *
     * @since GemFire 5.7
     */
    protected void tellLocalBridgeServers(InternalCache cache, boolean removeProfile,
        boolean exchangeProfiles, final List<Profile> replyProfiles) {

      if (cache != null && !cache.isClosed()) {
        List<?> bridgeServers = cache.getCacheServersAndGatewayReceiver();
        for (Object bridgeServer : bridgeServers) {
          CacheServerImpl bsi = (CacheServerImpl) bridgeServer;
          if (bsi.isRunning()) {
            if (bsi.getProfile().equals(this)) {
              continue;
            }
            // negative value for port indicates fake
            // profile meant to only gather remote profiles during profile
            // exchange
            if (port > 0) {
              handleDistributionAdvisee(bsi, removeProfile, exchangeProfiles, replyProfiles);
            } else if (exchangeProfiles) {
              replyProfiles.add(bsi.getProfile());
            }
          }
        }
      }
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeString(host, out);
      DataSerializer.writePrimitiveInt(port, out);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      host = DataSerializer.readString(in);
      port = DataSerializer.readPrimitiveInt(in);
      finishInit();
    }

    public void finishInit() {
      id = new GridProfileId(this);
    }

    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      sb.append("; host=").append(host);
      sb.append("; port=").append(port);
    }
  }

  /**
   * Basic idea is to base id off of host and port of GridProfile
   */
  public static class GridProfileId implements ProfileId {

    private final GridProfile gp;

    public GridProfileId(GridProfile gp) {
      this.gp = gp;
    }

    public InternalDistributedMember getMemberId() {
      return gp.getDistributedMember();
    }

    public String getHost() {
      return gp.getHost();
    }

    public int getPort() {
      return gp.getPort();
    }

    @Override
    public String toString() {
      return "GridProfile[host=" + gp.getHost() + ",port=" + gp.getPort() + ']';
    }

    @Override
    public int hashCode() {
      final String thisHost = gp.getHost();
      final int thisPort = gp.getPort();
      final String thisMemberId = getMemberId().getUniqueId();
      final int thisMemberIdHashCode = (thisMemberId != null) ? thisMemberId.hashCode() : 0;
      return thisHost != null ? (thisHost.hashCode() ^ thisPort) + thisMemberIdHashCode
          : thisPort + thisMemberIdHashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof GridProfileId) {
        final GridProfileId other = (GridProfileId) obj;

        if (gp.getPort() == other.gp.getPort()) {
          final String thisHost = gp.getHost();
          final String otherHost = other.gp.getHost();
          if (thisHost != null) {
            if (thisHost.equals(otherHost)) {
              if (getMemberId() != null) {
                return getMemberId().getUniqueId().equals(other.getMemberId().getUniqueId());
              } else {
                return other.getMemberId() == null;
              }
            }
          } else {
            return (otherHost == null);
          }
        }
      }
      return false;
    }
  }
}
