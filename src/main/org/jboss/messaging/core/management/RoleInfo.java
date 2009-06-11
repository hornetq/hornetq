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

package org.jboss.messaging.core.management;

import org.jboss.messaging.utils.json.JSONArray;
import org.jboss.messaging.utils.json.JSONObject;

/**
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class RoleInfo
{
   final private String name;

   final private boolean send;

   final private boolean consume;

   final private boolean createDurableQueue;

   final private boolean deleteDurableQueue;

   final private boolean createNonDurableQueue;

   final private boolean deleteNonDurableQueue;

   final private boolean manage;

   public static final RoleInfo[] from(final String jsonString) throws Exception
   {
      JSONArray array = new JSONArray(jsonString);
      RoleInfo[] roles = new RoleInfo[array.length()];
      for (int i = 0; i < array.length(); i++)
      {
         JSONObject r = array.getJSONObject(i);
         RoleInfo role = new RoleInfo(r.getString("name"),
                                      r.getBoolean("send"),
                                      r.getBoolean("consume"),
                                      r.getBoolean("createDurableQueue"),
                                      r.getBoolean("deleteDurableQueue"),
                                      r.getBoolean("createNonDurableQueue"),
                                      r.getBoolean("deleteNonDurableQueue"),
                                      r.getBoolean("manage"));
         roles[i] = role;
      }
      return roles;
   }

   private RoleInfo(final String name,
                    final boolean send,
                    final boolean consume,
                    final boolean createDurableQueue,
                    final boolean deleteDurableQueue,
                    final boolean createNonDurableQueue,
                    final boolean deleteNonDurableQueue,
                    boolean manage)
   {
      this.name = name;
      this.send = send;
      this.consume = consume;
      this.createDurableQueue = createDurableQueue;
      this.deleteDurableQueue = deleteDurableQueue;
      this.createNonDurableQueue = createNonDurableQueue;
      this.deleteNonDurableQueue = deleteNonDurableQueue;
      this.manage = manage;
   }

   public String getName()
   {
      return name;
   }

   public boolean isSend()
   {
      return send;
   }

   public boolean isConsume()
   {
      return consume;
   }

   public boolean isCreateDurableQueue()
   {
      return createDurableQueue;
   }

   public boolean isDeleteDurableQueue()
   {
      return deleteDurableQueue;
   }

   public boolean isCreateNonDurableQueue()
   {
      return createNonDurableQueue;
   }

   public boolean isDeleteNonDurableQueue()
   {
      return deleteNonDurableQueue;
   }

   public boolean isManage()
   {
      return manage;
   }
}
