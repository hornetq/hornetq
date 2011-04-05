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

import org.hornetq.core.logging.Logger;
import org.hornetq.utils.UUIDGenerator;

/**
 * A DivertConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 13 Jan 2009 09:36:19
 *
 *
 */
public class DivertConfiguration implements Serializable
{
   private static final long serialVersionUID = 6910543740464269629L;

   private static final Logger log = Logger.getLogger(DivertConfiguration.class);

   private String name;

   private String routingName;

   private String address;

   private String forwardingAddress;

   private boolean exclusive;

   private String filterString;

   private String transformerClassName;

   public DivertConfiguration(final String name,
                              final String routingName,
                              final String address,
                              final String forwardingAddress,
                              final boolean exclusive,
                              final String filterString,
                              final String transformerClassName)
   {
      this.name = name;
      if (routingName == null)
      {
         this.routingName = UUIDGenerator.getInstance().generateStringUUID();
      }
      else
      {
         this.routingName = routingName;
      }
      this.address = address;
      this.forwardingAddress = forwardingAddress;
      this.exclusive = exclusive;
      this.filterString = filterString;
      this.transformerClassName = transformerClassName;
   }

   public String getName()
   {
      return name;
   }

   public String getRoutingName()
   {
      return routingName;
   }

   public String getAddress()
   {
      return address;
   }

   public String getForwardingAddress()
   {
      return forwardingAddress;
   }

   public boolean isExclusive()
   {
      return exclusive;
   }

   public String getFilterString()
   {
      return filterString;
   }

   public String getTransformerClassName()
   {
      return transformerClassName;
   }

   /**
    * @param name the name to set
    */
   public void setName(final String name)
   {
      this.name = name;
   }

   /**
    * @param routingName the routingName to set
    */
   public void setRoutingName(final String routingName)
   {
      this.routingName = routingName;
   }

   /**
    * @param address the address to set
    */
   public void setAddress(final String address)
   {
      this.address = address;
   }

   /**
    * @param forwardingAddress the forwardingAddress to set
    */
   public void setForwardingAddress(final String forwardingAddress)
   {
      this.forwardingAddress = forwardingAddress;
   }

   /**
    * @param exclusive the exclusive to set
    */
   public void setExclusive(final boolean exclusive)
   {
      this.exclusive = exclusive;
   }

   /**
    * @param filterString the filterString to set
    */
   public void setFilterString(final String filterString)
   {
      this.filterString = filterString;
   }

   /**
    * @param transformerClassName the transformerClassName to set
    */
   public void setTransformerClassName(final String transformerClassName)
   {
      this.transformerClassName = transformerClassName;
   }
}
