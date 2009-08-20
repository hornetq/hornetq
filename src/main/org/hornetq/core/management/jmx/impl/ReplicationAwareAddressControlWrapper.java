/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.management.jmx.impl;

import javax.management.MBeanInfo;

import org.hornetq.core.management.AddressControl;
import org.hornetq.core.management.ReplicationOperationInvoker;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.management.impl.AddressControlImpl;
import org.hornetq.core.management.impl.MBeanInfoHelper;

/**
 * A ReplicationAwareAddressControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ReplicationAwareAddressControlWrapper extends ReplicationAwareStandardMBeanWrapper implements
         AddressControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final AddressControlImpl localAddressControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareAddressControlWrapper(final AddressControlImpl localAddressControl,
                                                final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(ResourceNames.CORE_ADDRESS + localAddressControl.getAddress(),
            AddressControl.class,
            replicationInvoker);

      this.localAddressControl = localAddressControl;
   }

   // AddressControlMBean implementation ------------------------------

   public String getAddress()
   {
      return localAddressControl.getAddress();
   }

   public String[] getQueueNames() throws Exception
   {
      return localAddressControl.getQueueNames();
   }

   public Object[] getRoles() throws Exception
   {
      return localAddressControl.getRoles();
   }
   
   public String getRolesAsJSON() throws Exception
   {
      return localAddressControl.getRolesAsJSON();
   }

   public void removeRole(final String name) throws Exception
   {
      replicationAwareInvoke("removeRole", name);
   }

   public void addRole(final String name,
                       final boolean send,
                       final boolean consume,
                       final boolean createDurableQueue,
                       final boolean deleteDurableQueue,
                       final boolean createNonDurableQueue,
                       final boolean deleteNonDurableQueue,
                       final boolean manage) throws Exception
   {
      replicationAwareInvoke("addRole",
                             name,
                             send,
                             consume,
                             createDurableQueue,
                             deleteDurableQueue,
                             createNonDurableQueue,
                             deleteNonDurableQueue,
                             manage);
   }

   // StandardMBean overrides ---------------------------------------

   @Override
   public MBeanInfo getMBeanInfo()
   {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(),
                           info.getDescription(),
                           info.getAttributes(),
                           info.getConstructors(),
                           MBeanInfoHelper.getMBeanOperationsInfo(AddressControl.class),
                           info.getNotifications());
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
