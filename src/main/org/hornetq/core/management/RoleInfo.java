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

package org.hornetq.core.management;

import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

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
