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
import org.hornetq.utils.json.JSONException;
import org.hornetq.utils.json.JSONObject;

/**
 * A JMSSessionInfo
 *
 * @author howard
 *
 *
 */
public class JMSSessionInfo
{
   private final String sessionID;

   private final long creationTime;

   public JMSSessionInfo(String sessionID, long creationTime)
   {
      this.sessionID = sessionID;
      this.creationTime = creationTime;
   }

   public static JMSSessionInfo[] from(final String jsonString) throws JSONException
   {
      JSONArray array = new JSONArray(jsonString);
      JMSSessionInfo[] infos = new JMSSessionInfo[array.length()];
      for (int i = 0; i < array.length(); i++)
      {
         JSONObject obj = array.getJSONObject(i);

         JMSSessionInfo info = new JMSSessionInfo(obj.getString("sessionID"),
                                                        obj.getLong("creationTime"));
         infos[i] = info;
      }
      return infos;
   }

   public String getSessionID()
   {
      return sessionID;
   }

   public long getCreationTime()
   {
      return creationTime;
   }
}
