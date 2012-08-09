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
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Aug 16, 2010
 */
public class TopologyMember implements Serializable
{
   private static final long serialVersionUID = 1123652191795626133L;

   private final Pair<TransportConfiguration, TransportConfiguration> connector;

   private final String nodeName;

   /** transient to avoid serialization changes */
   private transient long uniqueEventID = System.currentTimeMillis();

   public TopologyMember(final TransportConfiguration a, final TransportConfiguration b)
   {
      this(null, a, b);
   }

   public TopologyMember(final String nodeName, final TransportConfiguration a, final TransportConfiguration b)
   {
      this.nodeName = nodeName;
      this.connector = new Pair<TransportConfiguration, TransportConfiguration>(a, b);
      uniqueEventID = System.currentTimeMillis();
   }

   public TransportConfiguration getA()
   {
      return connector.getA();
   }

   public TransportConfiguration getB()
   {
      return connector.getB();
   }

   public void setB(final TransportConfiguration param)
   {
      connector.setB(param);
   }

   public void setA(final TransportConfiguration param)
   {
      connector.setA(param);
   }

   /**
    * @return the uniqueEventID
    */
   public long getUniqueEventID()
   {
      return uniqueEventID;
   }

   public String getNodeName()
   {
      return nodeName;
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
      return "TopologyMember[name = " + nodeName + ", connector=" + connector + "]";
   }
}
