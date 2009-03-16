/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.core.management.jmx.impl;

import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.management.ReplicationOperationInvoker;
import org.jboss.messaging.core.management.RoleInfo;
import org.jboss.messaging.core.management.impl.AddressControl;
import org.jboss.messaging.core.management.impl.MBeanInfoHelper;

/**
 * A ReplicationAwareAddressControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ReplicationAwareAddressControlWrapper extends ReplicationAwareStandardMBeanWrapper implements AddressControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final AddressControl localAddressControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareAddressControlWrapper(final ObjectName objectName, 
                                                final AddressControl localAddressControl,
                                                final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(objectName, AddressControlMBean.class, replicationInvoker);

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

   public TabularData getRoles() throws Exception
   {
      return localAddressControl.getRoles();
   }

   public void removeRole(final String name) throws Exception
   {
      replicationAwareInvoke("removeRole", name);
   }

   public void addRole(final String name, final boolean create, final boolean read, final boolean write) throws Exception
   {
      replicationAwareInvoke("addRole", name, create, read, write);
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
                           MBeanInfoHelper.getMBeanOperationsInfo(AddressControlMBean.class),
                           info.getNotifications());
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
