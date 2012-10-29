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

package org.hornetq.jms.client;

import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnectionFactory;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ServerLocator;


/**
 * A class that represents a ConnectionFactory.
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class HornetQJMSConnectionFactory extends HornetQConnectionFactory implements TopicConnectionFactory,
         QueueConnectionFactory
{

   private final static long serialVersionUID = -2810634789345348326L;

   /**
    *
    */
   public HornetQJMSConnectionFactory()
   {
      super();
   }

   /**
    * @param serverLocator
    */
   public HornetQJMSConnectionFactory(ServerLocator serverLocator)
   {
      super(serverLocator);
    }

   /**
    * @param ha
    * @param discoveryAddress
    * @param discoveryPort
    */
   public HornetQJMSConnectionFactory(boolean ha, final DiscoveryGroupConfiguration groupConfiguration)
   {
      super(ha, groupConfiguration);
   }

   /**
    * @param ha
    * @param initialConnectors
    */
   public HornetQJMSConnectionFactory(boolean ha, TransportConfiguration... initialConnectors)
   {
      super(ha, initialConnectors);
   }

}
