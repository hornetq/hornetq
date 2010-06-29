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

package org.hornetq.core.config;

import java.io.Serializable;

/**
 * A TwitterConnectorConfiguration
 *
 * @author <a href="tm.igarashi@gmail.com">Tomohisa Igarashi</a>
 *
 *
 */
public class TwitterConnectorConfiguration implements Serializable
{
   private static final long serialVersionUID = -641207073030767325L;

   private String connectorName = null;

   private boolean isIncoming = false;

   private String userName = null;

   private String password = null;

   private String queueName = null;

   private int intervalSeconds = 0;

   public boolean isIncoming()
   {
      return isIncoming;
   }
   
   public String getUserName()
   {
      return userName;
   }

   public String getPassword()
   {
      return password;
   }

   public String getQueueName()
   {
      return queueName;
   }

   public int getIntervalSeconds()
   {
      return intervalSeconds;
   }

   public String getConnectorName()
   {
      return connectorName;
   }

   /**
    * @param isIncoming the isIncoming to set
    */
   public void setIncoming(boolean isIncoming)
   {
      this.isIncoming = isIncoming;
   }
   
   /**
    * @param userName the userName to set
    */
   public void setUserName(String userName)
   {
      this.userName = userName;
   }

   /**
    * @param password the password to set
    */
   public void setPassword(String password)
   {
      this.password = password;
   }

   /**
    * @param queueName the queueName to set
    */
   public void setQueueName(String queueName)
   {
      this.queueName = queueName;
   }

   /**
    * @param intervalSeconds the intervalSeconds to set
    */
   public void setIntervalSeconds(int intervalSeconds)
   {
      this.intervalSeconds = intervalSeconds;
   }

   /**
    * @param connectorName the connectorName to set
    */
   public void setConnectorName(String connectorName)
   {
      this.connectorName = connectorName;
   }
}
