/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.management.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.management.MBeanInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.management.RoleInfo;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class AddressControl extends StandardMBean implements
      AddressControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final SimpleString address;
   private MessagingServerManagement server;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AddressControl(final SimpleString address,
         final MessagingServerManagement server)
         throws NotCompliantMBeanException
   {
      super(AddressControlMBean.class);
      this.address = address;
      this.server = server;
   }

   // Public --------------------------------------------------------

   // AddressControlMBean implementation ----------------------------

   public String getAddress()
   {
      return address.toString();
   }

   public String[] getQueueNames() throws Exception
   {
      try
      {
         List<Queue> queues = server.getQueuesForAddress(address);
         String[] queueNames = new String[queues.size()];
         for (int i = 0; i < queues.size(); i++)
         {
            queueNames[i] = queues.get(i).getName().toString();
         }
         return queueNames;
      } catch (Throwable t)
      {
         throw new IllegalStateException(t.getMessage());
      }
   }

   public TabularData getRoles() throws Exception
   {
      return RoleInfo.toTabularData(getRoleInfos());
   }

   public RoleInfo[] getRoleInfos() throws Exception
   {
      Set<Role> roles = server.getSecurityForAddress(address);
      RoleInfo[] roleInfos = new RoleInfo[roles.size()];
      int i = 0;
      for (Role role : roles)
      {
         roleInfos[i++] = new RoleInfo(role.getName(), role
               .isCheckType(CheckType.CREATE),
               role.isCheckType(CheckType.READ), role
                     .isCheckType(CheckType.WRITE));
      }
      return roleInfos;
   }

   public void addRole(final String name, final boolean create,
         final boolean read, final boolean write) throws Exception
   {
      Set<Role> roles = server.getSecurityForAddress(address);
      Role newRole = new Role(name, read, write, create);
      boolean added = roles.add(newRole);
      if (!added)
      {
         throw new IllegalArgumentException("Role " + name + " already exists");
      }
      server.setSecurityForAddress(address, roles);
   }

   public void removeRole(final String role) throws Exception
   {
      Set<Role> roles = server.getSecurityForAddress(address);
      Iterator<Role> it = roles.iterator();
      boolean removed = false;
      while (it.hasNext())
      {
         Role r = it.next();
         if (r.getName().equals(role))
         {
            it.remove();
            removed = true;
            break;
         }
      }
      if (!removed)
      {
         throw new IllegalArgumentException("Role " + role + " does not exist");
      }
      server.setSecurityForAddress(address, roles);
   }

   // StandardMBean overrides ---------------------------------------

   @Override
   public MBeanInfo getMBeanInfo()
   {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(), info.getDescription(), info
            .getAttributes(), info.getConstructors(), MBeanInfoHelper
            .getMBeanOperationsInfo(AddressControlMBean.class), info
            .getNotifications());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
