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

import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.management.RoleInfo;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.utils.SimpleString;

import javax.management.openmbean.TabularData;
import java.util.Iterator;
import java.util.Set;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class AddressControl implements AddressControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final SimpleString address;

   private final PostOffice postOffice;

   private final HierarchicalRepository<Set<Role>> securityRepository;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AddressControl(final SimpleString address,
                         final PostOffice postOffice,
                         final HierarchicalRepository<Set<Role>> securityRepository)
   {
      this.address = address;
      this.postOffice = postOffice;
      this.securityRepository = securityRepository;
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
         Bindings bindings = postOffice.getBindingsForAddress(address);
         String[] queueNames = new String[bindings.getBindings().size()];
         int i = 0;
         for (Binding binding: bindings.getBindings())
         {           
            queueNames[i++] = binding.getUniqueName().toString();
         }
         return queueNames;
      }
      catch (Throwable t)
      {
         throw new IllegalStateException(t.getMessage());
      }
   }

   public TabularData getRoles() throws Exception
   {
      Set<Role> roles = securityRepository.getMatch(address.toString());
      RoleInfo[] roleInfos = new RoleInfo[roles.size()];
      int i = 0;
      for (Role role : roles)
      {
         roleInfos[i++] = new RoleInfo(role.getName(),
                                       CheckType.SEND.hasRole(role),
                                       CheckType.CONSUME.hasRole(role),
                                       CheckType.CREATE_DURABLE_QUEUE.hasRole(role),
                                       CheckType.DELETE_DURABLE_QUEUE.hasRole(role),
                                       CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role),
                                       CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role),
                                       CheckType.MANAGE.hasRole(role));
      }
      return RoleInfo.toTabularData(roleInfos);
   }

   public synchronized void addRole(final String name,
                                    final boolean send,
                                    final boolean consume,
                                    final boolean createDurableQueue,
                                    final boolean deleteDurableQueue,
                                    final boolean createNonDurableQueue,
                                    final boolean deleteNonDurableQueue,
                                    final boolean manage) throws Exception
   {
      Set<Role> roles = securityRepository.getMatch(address.toString());
      Role newRole = new Role(name, send, consume, createDurableQueue, deleteDurableQueue, createNonDurableQueue, deleteNonDurableQueue, manage);
      boolean added = roles.add(newRole);
      if (!added)
      {
         throw new IllegalArgumentException("Role " + name + " already exists");
      }
      securityRepository.addMatch(address.toString(), roles);
   }

   public synchronized void removeRole(final String role) throws Exception
   {
      Set<Role> roles = securityRepository.getMatch(address.toString());
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
      securityRepository.addMatch(address.toString(), roles);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
