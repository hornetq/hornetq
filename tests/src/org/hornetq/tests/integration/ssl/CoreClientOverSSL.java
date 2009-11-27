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

package org.hornetq.tests.integration.ssl;

import java.util.Arrays;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.integration.transports.netty.TransportConstants;
import org.hornetq.jms.client.HornetQTextMessage;

/**
 * This client will open a connection, send a message to a queue over SSL and
 * exit.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class CoreClientOverSSL
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(CoreClientOverSSL.class);

   // Static --------------------------------------------------------

   public static void main(String[] args)
   {
      try
      {
         System.out.println("Starting******");
         
         log.debug("args = " + Arrays.asList(args));

         if (args.length != 1)
         {
            log.fatal("unexpected number of args (should be 1)");
            System.exit(1);
         }

         boolean sslEnabled = Boolean.parseBoolean(args[0]); 
         
         System.out.println("ssl enabled is " + sslEnabled);
        
         TransportConfiguration tc = new TransportConfiguration("org.hornetq.integration.transports.netty.NettyConnectorFactory");
         tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, sslEnabled);
         ClientSessionFactory sf = new ClientSessionFactoryImpl(tc);                 
         ClientSession session = sf.createSession(false, true, true);
         ClientProducer producer = session.createProducer(CoreClientOverSSLTest.QUEUE);

         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.getBodyBuffer().writeString(CoreClientOverSSLTest.MESSAGE_TEXT_FROM_CLIENT);
         producer.send(message);

         session.close();
      }
      catch (Throwable t)
      {
         log.error(t.getMessage(), t);
         System.exit(1);
      }
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
