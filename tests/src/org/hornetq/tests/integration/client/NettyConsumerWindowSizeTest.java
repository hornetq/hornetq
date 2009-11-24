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

package org.hornetq.tests.integration.client;

import java.util.HashMap;

import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.integration.transports.netty.TransportConstants;

/**
 * A NettyConsumerWindowSizeTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class NettyConsumerWindowSizeTest extends ConsumerWindowSizeTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   protected boolean isNetty()
   {
      return true;
   }
   
   protected ClientSessionFactory createNettyFactory()
   {
      HashMap<String, Object> parameters = new HashMap<String, Object>();
      
      parameters.put(TransportConstants.TCP_NODELAY_PROPNAME, true);
      
      TransportConfiguration config = new TransportConfiguration(NETTY_CONNECTOR_FACTORY, parameters);
      
      return new ClientSessionFactoryImpl(config);
      
      //return super.createNettyFactory();
   }

   protected Configuration createDefaultConfig(final boolean netty)
   {
      if (netty)
      {
         
         HashMap<String, Object> parameters = new HashMap<String, Object>();
         
         parameters.put(TransportConstants.TCP_NODELAY_PROPNAME, true);

         return createDefaultConfig(parameters, INVM_ACCEPTOR_FACTORY, NETTY_ACCEPTOR_FACTORY);
      }
      else
      {
         new Exception("This test wasn't supposed to use InVM").printStackTrace();
         return super.createDefaultConfig(false);
      }
   }



   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
