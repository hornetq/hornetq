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

package org.hornetq.tests.integration.clientcrash;

import java.util.Arrays;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;

/**
 * Code to be run in an external VM, via main().
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class CrashClient2
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(CrashClient2.class);

   // Static ---------------------------------------------------------------------------------------

   public static void main(final String[] args) throws Exception
   {
      try
      {
         log.debug("args = " + Arrays.asList(args));

         ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName()));
         locator.setClientFailureCheckPeriod(ClientCrashTest.PING_PERIOD);
         locator.setConnectionTTL(ClientCrashTest.CONNECTION_TTL);
         ClientSessionFactory sf = locator.createSessionFactory();


         ClientSession session = sf.createSession(true, true, 1000000);
         ClientProducer producer = session.createProducer(ClientCrashTest.QUEUE);

         ClientMessage message = session.createMessage(false);
         message.getBodyBuffer().writeString(ClientCrashTest.MESSAGE_TEXT_FROM_CLIENT);

         producer.send(message);
         
         //Now consume the message, but don't let ack get to server
         
         //Consume the message
         ClientConsumer cons = session.createConsumer(ClientCrashTest.QUEUE);
         
         session.start();
         
         ClientMessage msg = cons.receive(10000);
         
         if (msg == null)
         {
            log.error("Didn't receive msg");
            
            System.exit(1);
         }

         // exit without closing the session properly
         System.exit(9);
      }
      catch (Throwable t)
      {
         log.error(t.getMessage(), t);
         System.exit(1);
      }
   }

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // Command implementation -----------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
