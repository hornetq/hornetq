/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
   * by the @authors tag. See the copyright.txt in the distribution for a
   * full listing of individual contributors.
   *
   * This is free software; you can redistribute it and/or modify it
   * under the terms of the GNU Lesser General Public License as
   * published by the Free Software Foundation; either version 2.1 of
   * the License, or (at your option) any later version.
   *
   * This software is distributed in the hope that it will be useful,
   * but WITHOUT ANY WARRANTY; without even the implied warranty of
   * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
   * Lesser General Public License for more details.
   *
   * You should have received a copy of the GNU Lesser General Public
   * License along with this software; if not, write to the Free
   * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
   * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
   */
package org.jboss.jms.example;

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

import org.jboss.common.example.JMSExample;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.ObjectNames;

/**
 * An example that shows how to kick off a client connected to JBoss Messagingby using JMX.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ClientKickoffExample extends JMSExample
{
   private String JMX_URL = "service:jmx:rmi:///jndi/rmi://localhost:3000/jmxrmi";

   public static void main(String[] args)
   {
      String[] serverJMXArgs = new String[] { "-Dcom.sun.management.jmxremote",
                                             "-Dcom.sun.management.jmxremote.port=3000",
                                             "-Dcom.sun.management.jmxremote.ssl=false",
                                             "-Dcom.sun.management.jmxremote.authenticate=false" };
      new ClientKickoffExample().run(serverJMXArgs, args);
   }

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
            public void onException(JMSException e)
            {
               exception.set(e);
            }
         });

         // Step 5. We start the connection
         connection.start();

         // Step 6. Create a MessagingServerControlMBean proxy to manage the server
         ObjectName on = ObjectNames.getMessagingServerObjectName();
         JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(JMX_URL), new HashMap<String, String>());
         MBeanServerConnection mbsc = connector.getMBeanServerConnection();
         MessagingServerControlMBean serverControl = (MessagingServerControlMBean)MBeanServerInvocationHandler.newProxyInstance(mbsc,
                                                                                                                                on,
                                                                                                                                MessagingServerControlMBean.class,
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
