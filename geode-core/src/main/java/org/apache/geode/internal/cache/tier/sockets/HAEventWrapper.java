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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl.ClientCqConcurrentMap;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl.CqNameToOp;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl.CqNameToOpHashMap;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl.CqNameToOpSingleEntry;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This new class acts as a wrapper for the existing <code>ClientUpdateMessageImpl</code>. Now, the
 * <code>HARegionQueue</code>s will contain instances of <code>HAEventWrapper</code> as value,
 * instead that of <code>ClientUpdateMessagesImpl</code>. It implements the <code>Conflatable</code>
 * interface to fit itself into the <code>HARegionQueue</code> mechanics. It also has a property to
 * indicate the number of <code>HARegionQueue</code>s referencing this instance.
 *
 * @since GemFire 5.7
 *
 */
public class HAEventWrapper implements Conflatable, DataSerializableFixedID, Sizeable {
  private static final long serialVersionUID = 5874226899495609988L;
  private static final Logger logger = LogService.getLogger();

  /**
   * The name of the <code>Region</code> that was updated
   */
  private String regionName;

  /**
   * The key that was updated
   */
  private Object keyOfInterest;

  /**
   * If the event should be conflated.
   */
  private boolean shouldConflate = false;

  /**
   * The event id of the event
   */
  private EventID eventIdentifier;

  /**
   * The underlying map for all the ha region queues associated with a cache server.
   */
  private Map haContainer;

  /**
   * Indicates the number of <code>HARegionQueue</code>s referencing <code>this</code> instance. All
   * access should be done using rcUpdater.
   */
  @SuppressWarnings("unused")
  private volatile long referenceCount;
  private static final AtomicLongFieldUpdater<HAEventWrapper> rcUpdater =
      AtomicLongFieldUpdater.newUpdater(HAEventWrapper.class, "referenceCount");

  /**
   * If greater than zero, the entry containing this HAEventWrapper instance will not be removed
   * from the haContainer, even if the referenceCount value is zero.
   */
  @SuppressWarnings("unused")
  private transient volatile long putInProgressCount;
  private static final AtomicLongFieldUpdater<HAEventWrapper> putInProgressCountUpdater =
      AtomicLongFieldUpdater.newUpdater(HAEventWrapper.class, "putInProgressCount");

  /**
   * A reference to its <code>ClientUpdateMessage</code> instance.
   */
  private ClientUpdateMessage clientUpdateMessage = null;

  /**
   * This will hold the CQ list while its ClientUpdateMessageImpl is overflown to disk, and reassign
   * it back when it's faulted-in.
   */
  private ClientCqConcurrentMap clientCqs = null;

  /**
   * Parameterized constructor.
   *
   */
  public HAEventWrapper(ClientUpdateMessage event) {
    regionName = event.getRegionName();
    keyOfInterest = event.getKeyOfInterest();
    shouldConflate = event.shouldBeConflated();
    eventIdentifier = event.getEventId();
    rcUpdater.set(this, 0);
    putInProgressCountUpdater.set(this, 0);
    clientUpdateMessage = event;
    clientCqs = ((ClientUpdateMessageImpl) event).getClientCqs();
  }

  public HAEventWrapper(EventID eventId) {
    eventIdentifier = eventId;
    clientUpdateMessage = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_CREATE,
        new ClientProxyMembershipID(), eventId);
    rcUpdater.set(this, 0);
    putInProgressCountUpdater.set(this, 0);
  }

  /**
   * Default constructor
   *
   */
  public HAEventWrapper() {}

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.Conflatable#getEventId()
   */
  @Override
  public EventID getEventId() {
    return eventIdentifier;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.Conflatable#getKeyToConflate()
   */
  @Override
  public Object getKeyToConflate() {
    return keyOfInterest;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.Conflatable#getRegionToConflate()
   */
  @Override
  public String getRegionToConflate() {
    return regionName;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.Conflatable#getValueToConflate()
   */
  @Override
  public Object getValueToConflate() {
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.Conflatable#setLatestValue(java.lang.Object)
   */
  @Override
  public void setLatestValue(Object value) {
    // this.value = (byte[])value;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.Conflatable#shouldBeConflated()
   */
  @Override
  public boolean shouldBeConflated() {
    return shouldConflate;
  }

  public void setClientUpdateMessage(ClientUpdateMessage cum) {
    clientUpdateMessage = cum;
  }

  /**
   * Use this method <B>only</B> when put operation on HARegionQueue is in progress. It'll always
   * return null otherwise.
   *
   * @return an instance of <code>ClientUpdateMessage</code> implementation.
   */
  public ClientUpdateMessage getClientUpdateMessage() {
    return clientUpdateMessage;
  }

  public ClientCqConcurrentMap getClientCqs() {
    return clientCqs;
  }

  public void setHAContainer(Map container) {
    haContainer = container;
  }

  /**
   * This implementation considers only the EventID of <code>HAEventWrapper</code> for the equality
   * test. It allows an instance of {@link EventID} to be tested for equality.
   *
   * @param other The instance of HAEventWrapper or EventID to be tested for equality.
   *
   * @return boolean true if <code>this</code> object's event id matches with that of other.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof HAEventWrapper)) {
      return false;
    }

    return this == other || getEventId().equals(((HAEventWrapper) other).getEventId());
  }

  /**
   * Returns the hash code of underlying event id.
   *
   * @return int The hash code of underlying EventID instance.
   */
  @Override
  public int hashCode() {
    return getEventId().hashCode();
  }

  @Override
  public String toString() {
    if (clientUpdateMessage != null) {
      return "HAEventWrapper[refCount=" + getReferenceCount() + "; putInProgress="
          + putInProgressCountUpdater.get(this) + "; msg=" + clientUpdateMessage + "]";
    } else {
      return "HAEventWrapper[region=" + regionName + "; key=" + keyOfInterest
          + "; refCount=" + getReferenceCount()
          + "; putInProgress=" + putInProgressCountUpdater.get(this) + "; event="
          + eventIdentifier
          + ((clientUpdateMessage == null) ? "; no message" : ";with message")
          + ((clientUpdateMessage == null) ? ""
              : ("; op=" + clientUpdateMessage.getOperation()))
          + ((clientUpdateMessage == null) ? ""
              : ("; version=" + clientUpdateMessage.getVersionTag()))
          + "]";
    }
  }

  /**
   * Calls toData() on its clientUpdateMessage present in the haContainer (client-messages-region or
   * the map).
   *
   * @param out The output stream which the object should be written to.
   */
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    ClientUpdateMessageImpl cum = (ClientUpdateMessageImpl) haContainer.get(this);

    // If the dispatcher sends the cum object to the client and removes it from
    // the haContainer before we do haContainer.get() (above), we indicate that
    // by sending false boolean value.
    if (cum != null) {
      DataSerializer.writePrimitiveBoolean(true, out);
      context.getSerializer().writeObject(cum.getEventId(), out);
    } else {
      DataSerializer.writePrimitiveBoolean(false, out);
      context.getSerializer().writeObject(new EventID(), out);
      // Create a dummy ClientUpdateMessageImpl instance
      cum = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_CREATE,
          new ClientProxyMembershipID(), null);
    }
    InternalDataSerializer.invokeToData(cum, out);
    if (cum.hasCqs()) {
      DataSerializer.writeConcurrentHashMap(cum.getClientCqs(), out);
    }
  }

  /**
   * Calls fromData() on ClientUpdateMessage and sets it as its member variable. Also, sets the
   * referenceCount to zero.
   *
   * @param in The input stream from which the object should be constructed.
   */
  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    if (DataSerializer.readPrimitiveBoolean(in)) {
      // Indicates that we have a ClientUpdateMessage along with the HAEW instance in inputstream.
      eventIdentifier = context.getDeserializer().readObject(in);
      clientUpdateMessage = new ClientUpdateMessageImpl();
      InternalDataSerializer.invokeFromData(clientUpdateMessage, in);
      ((ClientUpdateMessageImpl) clientUpdateMessage).setEventIdentifier(eventIdentifier);
      if (clientUpdateMessage.hasCqs()) {
        {
          ClientCqConcurrentMap cqMap;
          int size = InternalDataSerializer.readArrayLength(in);
          if (size == -1) {
            cqMap = null;
          } else {
            cqMap = new ClientCqConcurrentMap(size, 1.0f, 1);
            for (int i = 0; i < size; i++) {
              ClientProxyMembershipID key = DataSerializer.readObject(in);
              CqNameToOp value;
              {
                byte typeByte = in.readByte();
                if (typeByte == DSCODE.HASH_MAP.toByte()) {
                  int cqNamesSize = InternalDataSerializer.readArrayLength(in);
                  if (cqNamesSize == -1) {
                    throw new IllegalStateException(
                        "The value of a ConcurrentHashMap is not allowed to be null.");
                  } else if (cqNamesSize == 1) {
                    String cqNamesKey = DataSerializer.readObject(in);
                    Integer cqNamesValue = DataSerializer.<Integer>readObject(in);
                    value = new CqNameToOpSingleEntry(cqNamesKey, cqNamesValue);
                  } else if (cqNamesSize == 0) {
                    value = new CqNameToOpSingleEntry(null, 0);
                  } else {
                    value = new CqNameToOpHashMap(cqNamesSize);
                    for (int j = 0; j < cqNamesSize; j++) {
                      String cqNamesKey = DataSerializer.readObject(in);
                      Integer cqNamesValue = DataSerializer.<Integer>readObject(in);
                      value.add(cqNamesKey, cqNamesValue);
                    }
                  }
                } else if (typeByte == DSCODE.NULL.toByte()) {
                  throw new IllegalStateException(
                      "The value of a ConcurrentHashMap is not allowed to be null.");
                } else {
                  throw new IllegalStateException(
                      "Expected DSCODE.NULL or DSCODE.HASH_MAP but read " + typeByte);
                }
              }
              cqMap.put(key, value);
            }
          }
          clientCqs = cqMap;
        }
        ((ClientUpdateMessageImpl) clientUpdateMessage).setClientCqs(clientCqs);
      }
      regionName = clientUpdateMessage.getRegionName();
      keyOfInterest = clientUpdateMessage.getKeyOfInterest();
      shouldConflate = clientUpdateMessage.shouldBeConflated();
      rcUpdater.set(this, 0);
    } else {
      // Read and ignore dummy eventIdentifier instance.
      context.getDeserializer().readObject(in);
      // Read and ignore dummy ClientUpdateMessageImpl instance.
      InternalDataSerializer.invokeFromData(new ClientUpdateMessageImpl(), in);
      // hasCq will be false here, so client CQs are not read from this input
      // stream.
      if (logger.isDebugEnabled()) {
        logger.debug(
            "HAEventWrapper.fromData(): The event has already been sent to the client by the primary server.");
      }
    }
  }

  @Override
  public int getDSFID() {
    return HA_EVENT_WRAPPER;
  }

  /**
   * Returning zero as default size. This is because this method will be called only in case of
   * ha-overflow, in which case, we ignore size of instances of <code>HAEventWrapper</code>, as they
   * are never overflown to disk.
   *
   * @return int Size of this instance in bytes.
   */
  @Override
  public int getSizeInBytes() {
    return 0;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  public long getReferenceCount() {
    return rcUpdater.get(this);
  }

  public long incAndGetReferenceCount() {
    return rcUpdater.incrementAndGet(this);
  }

  public long decAndGetReferenceCount() {
    return rcUpdater.decrementAndGet(this);
  }

  public long incrementPutInProgressCounter(String location) {
    long putInProgressCounter = putInProgressCountUpdater.incrementAndGet(this);

    if (logger.isDebugEnabled()) {
      logger.debug("Incremented PutInProgressCounter from " + location
          + " on HAEventWrapper with Event ID hash code: " + hashCode() + "; System ID hash code: "
          + System.identityHashCode(this) + "; Wrapper details: " + this);
    }

    return putInProgressCounter;
  }

  public long decrementPutInProgressCounter() {
    synchronized (this) {
      long putInProgressCounter = putInProgressCountUpdater.decrementAndGet(this);

      if (logger.isDebugEnabled()) {
        logger.debug("Decremented PutInProgressCounter on HAEventWrapper with Event ID hash code: "
            + hashCode() + "; System ID hash code: "
            + System.identityHashCode(this) + "; Wrapper details: " + this);
      }

      if (putInProgressCounter == 0L) {
        if (logger.isDebugEnabled()) {
          logger.debug("Setting HAEventWrapper ClientUpdateMessage to null.  Event ID hash code: "
              + hashCode()
              + "; System ID hash code: " + System.identityHashCode(this) + "; Wrapper details: "
              + this);
        }
        setClientUpdateMessage(null);
      }

      return putInProgressCounter;
    }
  }

  public boolean getPutInProgress() {
    return putInProgressCountUpdater.get(this) > 0;
  }
}
