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
package org.hornetq.jms.example;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.InitialContext;

import org.hornetq.api.core.management.HornetQServerControl;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.common.example.HornetQExample;

/**
 * An example that shows how to kick off a client connected to HornetQby using JMX.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ClientKickoffExample extends HornetQExample
{
   private final static String JMX_URL = "service:jmx:rmi:///jndi/rmi://localhost:3000/jmxrmi";

   public static void main(final String[] args)
   {
      new ClientKickoffExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      QueueConnection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perform a lookup on the Connection Factory
         QueueConnectionFactory cf = (QueueConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 3.Create a JMS Connection
         connection = cf.createQueueConnection();

         // Step 4. Set an exception listener on the connection to be notified after a problem occurred
         final AtomicReference<JMSException> exception = new AtomicReference<JMSException>();
         connection.setExceptionListener(new ExceptionListener()
         {
            @Override
			public void onException(final JMSException e)
            {
               exception.set(e);
            }
         });

         // Step 5. We start the connection
         connection.start();

         // Step 6. Create a HornetQServerControlMBean proxy to manage the server
         ObjectName on = ObjectNameBuilder.DEFAULT.getHornetQServerObjectName();
         JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(JMX_URL), new HashMap<String, String>());
         MBeanServerConnection mbsc = connector.getMBeanServerConnection();
         HornetQServerControl serverControl = MBeanServerInvocationHandler.newProxyInstance(mbsc,
                                                                                            on,
                                                                                            HornetQServerControl.class,
                                                                                            false);

         // Step 7. List the remote address connected to the server
         System.out.println("List of remote addresses connected to the server:");
         System.out.println("----------------------------------");
         String[] remoteAddresses = serverControl.listRemoteAddresses();
         for (String remoteAddress : remoteAddresses)
         {
            System.out.println(remoteAddress);
         }
         System.out.println("----------------------------------");

         // Step 8. Close the connections for the 1st remote address and kickoff the client
         serverControl.closeConnectionsForAddress(remoteAddresses[0]);

         // Sleep a little bit so that the stack trace from the server won't be
         // mingled with the JMSException received on the ExceptionListener
         Thread.sleep(1000);

         // Step 9. Display the exception received by the connection's ExceptionListener
         System.err.println("\nException received from the server:");
         System.err.println("----------------------------------");
         exception.get().printStackTrace();
         System.err.println("----------------------------------");

         return true;
      }
      finally
      {
         // Step 10. Be sure to close the resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if (connection != null)
         {
            connection.close();
         }
      }
   }

}
