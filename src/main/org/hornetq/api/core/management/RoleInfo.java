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

package org.hornetq.api.core.management;

import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * Helper class to create Java Objects from the
 * JSON serialization returned by {@link AddressControl#getRolesAsJSON()}.
 *
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

   /**
    * Returns an array of RoleInfo corresponding to the JSON serialization returned
    * by {@link AddressControl#getRolesAsJSON()}.
    */
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
                    final boolean manage)
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

   /**
    * Returns the name of the role.
    */
   public String getName()
   {
      return name;
   }

   /**
    * Returns whether this role can send messages to the address.
    */
   public boolean isSend()
   {
      return send;
   }

   /**
    * Returns whether this role can consume messages from queues bound to the address.
    */
   public boolean isConsume()
   {
      return consume;
   }

   /**
    * Returns whether this role can create durable queues bound to the address.
    */
   public boolean isCreateDurableQueue()
   {
      return createDurableQueue;
   }

   /**
    * Returns whether this role can delete durable queues bound to the address.
    */
   public boolean isDeleteDurableQueue()
   {
      return deleteDurableQueue;
   }

   /**
    * Returns whether this role can create non-durable queues bound to the address.
    */
   public boolean isCreateNonDurableQueue()
   {
      return createNonDurableQueue;
   }

   /**
    * Returns whether this role can delete non-durable queues bound to the address.
    */
   public boolean isDeleteNonDurableQueue()
   {
      return deleteNonDurableQueue;
   }

   /**
    * Returns whether this role can send management messages to the address.
    */
   public boolean isManage()
   {
      return manage;
   }
}
