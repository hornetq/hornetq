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

package org.hornetq.core.management.impl;

import java.util.Iterator;
import java.util.Set;

import javax.management.StandardMBean;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.AddressControl;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.Role;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class AddressControlImpl extends StandardMBean implements AddressControl
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(AddressControlImpl.class);

   // Attributes ----------------------------------------------------

   private final SimpleString address;

   private final PostOffice postOffice;

   private final HierarchicalRepository<Set<Role>> securityRepository;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AddressControlImpl(final SimpleString address,
                             final PostOffice postOffice,
                             final HierarchicalRepository<Set<Role>> securityRepository)
      throws Exception
   {
      super(AddressControl.class);
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
         for (Binding binding : bindings.getBindings())
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

   public Object[] getRoles() throws Exception
   {
      Set<Role> roles = securityRepository.getMatch(address.toString());

      Object[] objRoles = new Object[roles.size()];

      int i = 0;
      for (Role role : roles)
      {
         objRoles[i++] = new Object[] { role.getName(),
                                       CheckType.SEND.hasRole(role),
                                       CheckType.CONSUME.hasRole(role),
                                       CheckType.CREATE_DURABLE_QUEUE.hasRole(role),
                                       CheckType.DELETE_DURABLE_QUEUE.hasRole(role),
                                       CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role),
                                       CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role),
                                       CheckType.MANAGE.hasRole(role) };
      }
      return objRoles;
   }

   public String getRolesAsJSON() throws Exception
   {
      JSONArray json = new JSONArray();
      Set<Role> roles = securityRepository.getMatch(address.toString());

      for (Role role : roles)
      {
         json.put(new JSONObject(role));
      }
      return json.toString();
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
      Role newRole = new Role(name,
                              send,
                              consume,
                              createDurableQueue,
                              deleteDurableQueue,
                              createNonDurableQueue,
                              deleteNonDurableQueue,
                              manage);
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
