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

package org.hornetq.jms.client;

import javax.jms.XAQueueConnectionFactory;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ServerLocator;

/**
 * A class that represents a XAQueueConnectionFactory.
 * 
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public class HornetQXAQueueConnectionFactory extends HornetQConnectionFactory implements XAQueueConnectionFactory
{
   private static final long serialVersionUID = 8612457847251087454L;

   /**
    * 
    */
   public HornetQXAQueueConnectionFactory()
   {
      super();
   }

   /**
    * @param serverLocator
    */
   public HornetQXAQueueConnectionFactory(ServerLocator serverLocator)
   {
      super(serverLocator);
   }

   /**
    * @param ha
    * @param discoveryAddress
    * @param discoveryPort
    */
   public HornetQXAQueueConnectionFactory(final boolean ha, final String discoveryAddress, final int discoveryPort)
   {
      super(ha, discoveryAddress, discoveryPort);
   }

   /**
    * @param ha
    * @param initialConnectors
    */
   public HornetQXAQueueConnectionFactory(final boolean ha, final TransportConfiguration... initialConnectors)
   {
      super(ha, initialConnectors);
   }

}
