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

package org.hornetq.core.config;

import java.io.Serializable;

/**
 * A QueueConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 13 Jan 2009 09:39:21
 *
 *
 */
public class CoreQueueConfiguration implements Serializable
{
   private static final long serialVersionUID = 650404974977490254L;

   private String address;

   private String name;

   private String filterString;

   private boolean durable;

   public CoreQueueConfiguration(final String address, final String name, final String filterString, final boolean durable)
   {
      this.address = address;
      this.name = name;
      this.filterString = filterString;
      this.durable = durable;
   }

   public String getAddress()
   {
      return address;
   }

   public String getName()
   {
      return name;
   }

   public String getFilterString()
   {
      return filterString;
   }

   public boolean isDurable()
   {
      return durable;
   }

   /**
    * @param address the address to set
    */
   public void setAddress(final String address)
   {
      this.address = address;
   }

   /**
    * @param name the name to set
    */
   public void setName(final String name)
   {
      this.name = name;
   }

   /**
    * @param filterString the filterString to set
    */
   public void setFilterString(final String filterString)
   {
      this.filterString = filterString;
   }

   /**
    * @param durable the durable to set
    */
   public void setDurable(final boolean durable)
   {
      this.durable = durable;
   }
}
