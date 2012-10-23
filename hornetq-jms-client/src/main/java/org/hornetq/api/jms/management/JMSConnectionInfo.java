/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.api.jms.management;

import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * A JMSConnectionInfo
 *
 * @author jmesnil
 *
 *
 */
public class JMSConnectionInfo
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String connectionID;

   private final String clientAddress;

   private final long creationTime;

   private final String clientID;

   private final String username;


   // Static --------------------------------------------------------

   public static JMSConnectionInfo[] from(final String jsonString) throws Exception
   {
      JSONArray array = new JSONArray(jsonString);
      JMSConnectionInfo[] infos = new JMSConnectionInfo[array.length()];
      for (int i = 0; i < array.length(); i++)
      {
         JSONObject obj = array.getJSONObject(i);
         String cid = obj.isNull("clientID") ? null : obj.getString("clientID");
         String uname = obj.isNull("principal") ? null : obj.getString("principal");

         JMSConnectionInfo info = new JMSConnectionInfo(obj.getString("connectionID"),
                                                        obj.getString("clientAddress"),
                                                        obj.getLong("creationTime"),
                                                        cid,
                                                        uname);
         infos[i] = info;
      }
      return infos;
   }

   // Constructors --------------------------------------------------

   private JMSConnectionInfo(final String connectionID,
                             final String clientAddress,
                             final long creationTime,
                             final String clientID,
                             final String username)
   {
      this.connectionID = connectionID;
      this.clientAddress = clientAddress;
      this.creationTime = creationTime;
      this.clientID = clientID;
      this.username = username;
   }

   // Public --------------------------------------------------------

   public String getConnectionID()
   {
      return connectionID;
   }

   public String getClientAddress()
   {
      return clientAddress;
   }

   public long getCreationTime()
   {
      return creationTime;
   }

   public String getClientID()
   {
      return clientID;
   }

   public String getUsername()
   {
      return username;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
