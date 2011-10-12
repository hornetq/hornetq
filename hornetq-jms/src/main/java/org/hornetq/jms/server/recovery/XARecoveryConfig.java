/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.hornetq.jms.server.recovery;

import org.hornetq.jms.client.HornetQConnectionFactory;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *
 * A wrapper around info needed for the xa recovery resource
 *         Date: 3/23/11
 *         Time: 10:15 AM
 */
public class XARecoveryConfig
{
   private final HornetQConnectionFactory hornetQConnectionFactory;
   private final String username;
   private final String password;

   public XARecoveryConfig(HornetQConnectionFactory hornetQConnectionFactory, String username, String password)
   {
      this.hornetQConnectionFactory = hornetQConnectionFactory;
      this.username = username;
      this.password = password;
   }

   public HornetQConnectionFactory getHornetQConnectionFactory()
   {
      return hornetQConnectionFactory;
   }

   public String getUsername()
   {
      return username;
   }

   public String getPassword()
   {
      return password;
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      XARecoveryConfig that = (XARecoveryConfig) o;

      if (hornetQConnectionFactory != null ? !hornetQConnectionFactory.equals(that.hornetQConnectionFactory) : that.hornetQConnectionFactory != null)
         return false;
      if (password != null ? !password.equals(that.password) : that.password != null) return false;
      if (username != null ? !username.equals(that.username) : that.username != null) return false;

      return true;
   }

   @Override
   public int hashCode()
   {
      int result = hornetQConnectionFactory != null ? hornetQConnectionFactory.hashCode() : 0;
      result = 31 * result + (username != null ? username.hashCode() : 0);
      result = 31 * result + (password != null ? password.hashCode() : 0);
      return result;
   }
}
