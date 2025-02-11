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
package org.apache.geode.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.membership.InternalRole;

/**
 * Configuration attributes for defining reliability requirements and behavior for a
 * <code>Region</code>.
 *
 * <p>
 * <code>MembershipAttributes</code> provides options for configuring a <code>Region</code> to
 * require one or more membership roles to be present in the system for reliable access to the
 * <code>Region</code>. Each {@link Role} is a user defined string name, such as Producer or Backup
 * or FooProducer.
 * </p>
 *
 * <p>
 * The {@link LossAction} defines the behavior when one or more required roles are missing.
 * </p>
 *
 * <p>
 * The {@link ResumptionAction} specifies the action to be taken when reliability resumes.
 * </p>
 *
 * <p>
 * <code>MembershipAttributes</code> have no effect unless one or more required roles are specified.
 * These attributes are immutable after the <code>Region</code> has been created.
 * </p>
 *
 * @deprecated this feature is scheduled to be removed
 */
public class MembershipAttributes implements DataSerializable, Externalizable {

  /**
   * Array of required role names by this process for reliable access to the region
   */
  private /* final */ Set<Role> requiredRoles;

  /**
   * The configuration defining how this process behaves when there are missing required roles
   */
  private /* final */ LossAction lossAction;

  /**
   * The action to take when missing required roles return to the system
   */
  private /* final */ ResumptionAction resumptionAction;

  /**
   * Creates a new <code>MembershipAttributes</code> with the default configuration of no required
   * roles.
   */
  public MembershipAttributes() {
    requiredRoles = Collections.emptySet();
    lossAction = LossAction.FULL_ACCESS;
    resumptionAction = ResumptionAction.NONE;
  }

  /**
   * Creates a new <code>MembershipAttributes</code> with the specified required role names.
   * Reliability policy will default to {@linkplain LossAction#NO_ACCESS NO_ACCESS}, and resumption
   * action will default to {@linkplain ResumptionAction#REINITIALIZE REINITIALIZE}.
   *
   * @param requiredRoles array of role names required by this process for reliable access to the
   *        region
   * @throws IllegalArgumentException if no requiredRoles are specified
   */
  public MembershipAttributes(String[] requiredRoles) {
    this(requiredRoles, LossAction.NO_ACCESS, ResumptionAction.REINITIALIZE);
  }

  /**
   * Creates a new <code>MembershipAttributes</code> with the specified required role names,
   * reliability policy, and resumption action.
   *
   * @param requiredRoles array of role names required by this process for reliable access to the
   *        region
   * @param lossAction the configuration defining how this process behaves when there are missing
   *        required roles
   * @param resumptionAction the action to take when missing required roles return to the system
   * @throws IllegalArgumentException if the resumptionAction is incompatible with the lossAction or
   *         if no requiredRoles are specified
   */
  public MembershipAttributes(String[] requiredRoles, LossAction lossAction,
      ResumptionAction resumptionAction) {
    this.requiredRoles = toRoleSet(requiredRoles);
    if (this.requiredRoles.isEmpty()) {
      throw new IllegalArgumentException(
          "One or more required roles must be specified.");
    }
    this.lossAction = lossAction;
    this.resumptionAction = resumptionAction;
  }

  /**
   * Returns the set of {@linkplain org.apache.geode.distributed.Role Role}s that are required for
   * the reliability of this region.
   */
  public Set<Role> getRequiredRoles() {
    return Collections.unmodifiableSet(requiredRoles);
  }

  /**
   * Returns true if there are one or more required roles specified.
   */
  public boolean hasRequiredRoles() {
    return !requiredRoles.isEmpty();
  }

  /**
   * Returns the reliability policy that describes behavior if any required roles are missing.
   */
  public LossAction getLossAction() {
    return lossAction;
  }

  /**
   * Returns the resumption action that describes behavior when
   */
  public ResumptionAction getResumptionAction() {
    return resumptionAction;
  }

  private Set<Role> toRoleSet(String[] roleNames) {
    if (roleNames == null || roleNames.length == 0) {
      return Collections.emptySet();
    }
    Set<Role> roleSet = new HashSet<>();
    for (final String roleName : roleNames) {
      roleSet.add(InternalRole.getRole(roleName));
    }
    return roleSet;
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * @param other the reference object with which to compare.
   * @return true if this object is the same as the obj argument; false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (!(other instanceof MembershipAttributes)) {
      return false;
    }
    final MembershipAttributes that = (MembershipAttributes) other;

    if (requiredRoles != that.requiredRoles
        && !(requiredRoles != null && requiredRoles.equals(that.requiredRoles))) {
      return false;
    }
    if (lossAction != that.lossAction
        && !(lossAction != null && lossAction.equals(that.lossAction))) {
      return false;
    }
    return resumptionAction == that.resumptionAction
        || resumptionAction != null
            && resumptionAction.equals(that.resumptionAction);
  }

  /**
   * Returns a hash code for the object. This method is supported for the benefit of hashtables such
   * as those provided by java.util.Hashtable.
   *
   * @return the integer 0 if description is null; otherwise a unique integer.
   */
  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;

    result = mult * result + (requiredRoles == null ? 0 : requiredRoles.hashCode());
    result = mult * result + (lossAction == null ? 0 : lossAction.hashCode());
    result = mult * result + (resumptionAction == null ? 0 : resumptionAction.hashCode());

    return result;
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    if (!hasRequiredRoles()) {
      return "RequiredRoles(none)";
    } else {
      final StringBuilder sb = new StringBuilder();
      sb.append("RequiredRoles(");
      boolean comma = false;
      for (final Role requiredRole : requiredRoles) {
        if (comma) {
          sb.append(",");
        }
        Role role = requiredRole;
        sb.append(role.getName());
        comma = true;
      }
      sb.append("); Policy:");
      sb.append(lossAction.toString());
      sb.append("; Action:");
      sb.append(resumptionAction.toString());
      return sb.toString();
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    String[] names = new String[requiredRoles.size()];
    Iterator<Role> iter = requiredRoles.iterator();
    for (int i = 0; i < names.length; i++) {
      names[i] = iter.next().getName();
    }
    DataSerializer.writeStringArray(names, out);
    out.writeByte(lossAction.ordinal);
    out.writeByte(resumptionAction.ordinal);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    requiredRoles = toRoleSet(DataSerializer.readStringArray(in));
    lossAction = LossAction.fromOrdinal(in.readByte());
    resumptionAction = ResumptionAction.fromOrdinal(in.readByte());
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // added to fix bug 36619
    toData(out);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // added to fix bug 36619
    fromData(in);
  }
}
