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
package org.hornetq.core.client.impl;

import java.io.Serializable;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Aug 16, 2010
 */
public class TopologyMember implements Serializable
{
   private static final long serialVersionUID = 1123652191795626133L;

   private final Pair<TransportConfiguration, TransportConfiguration> connector;

   /** transient to avoid serialization changes */
   private transient long uniqueEventID = System.currentTimeMillis();

   public TopologyMember(final Pair<TransportConfiguration, TransportConfiguration> connector)
   {
      this.connector = connector;
      uniqueEventID = System.currentTimeMillis();
   }

   public TopologyMember(final TransportConfiguration a, final TransportConfiguration b)
   {
      this(new Pair<TransportConfiguration, TransportConfiguration>(a, b));
   }

   public TransportConfiguration getA()
   {
      return connector.a;
   }

   public TransportConfiguration getB()
   {
      return connector.b;
   }

   public void setB(final TransportConfiguration param)
   {
      connector.b = param;
   }

   public void setA(final TransportConfiguration param)
   {
      connector.a = param;
   }

   /**
    * @return the uniqueEventID
    */
   public long getUniqueEventID()
   {
      return uniqueEventID;
   }

   /**
    * @param uniqueEventID the uniqueEventID to set
    */
   public void setUniqueEventID(final long uniqueEventID)
   {
      this.uniqueEventID = uniqueEventID;
   }

   public Pair<TransportConfiguration, TransportConfiguration> getConnector()
   {
      return connector;
   }

   @Override
   public String toString()
   {
      return "TopologyMember[connector=" + connector + "]";
   }
}
