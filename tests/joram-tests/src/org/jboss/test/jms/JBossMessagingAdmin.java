/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.test.jms;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PING_PERIOD;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Hashtable;

import javax.management.ObjectName;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import junit.framework.Assert;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientRequestor;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.security.impl.SecurityStoreImpl;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.tests.util.SpawnedVMSupport;
import org.jboss.messaging.utils.SimpleString;
import org.objectweb.jtests.jms.admin.Admin;

/**
 * A JBossMessagingAdmin
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class JBossMessagingAdmin implements Admin
{

   private ClientSession clientSession;

   private ClientRequestor requestor;

   private Context context;

   private Process serverProcess;

   public JBossMessagingAdmin()
   {
      try
      {
         Hashtable<String, String> env = new Hashtable<String, String>();
         env.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
         env.put("java.naming.provider.url", "jnp://localhost:1099");
         env.put("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
         context = new InitialContext(env);
      }
      catch (NamingException e)
      {
         e.printStackTrace();
      }
   }

   public void start() throws Exception
   {
      ClientSessionFactoryImpl sf = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()));
      clientSession = sf.createSession(SecurityStoreImpl.CLUSTER_ADMIN_USER,
                                       ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_PASSWORD,
                                       false,
                                       true,
                                       true,
                                       false,
                                       1);
      requestor = new ClientRequestor(clientSession, ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS);
      clientSession.start();
   }

   public void stop() throws Exception
   {
      requestor.close();
   }

   public void createConnectionFactory(String name)
   {
      try
      {
         invokeSyncOperation(ObjectNames.getJMSServerObjectName(),
                             "createSimpleConnectionFactory",
                             name,
                             NettyConnectorFactory.class.getName(),
                             DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                             DEFAULT_PING_PERIOD,
                             DEFAULT_CONNECTION_TTL,
                             DEFAULT_CALL_TIMEOUT,
                             null,
                             DEFAULT_ACK_BATCH_SIZE,
                             DEFAULT_ACK_BATCH_SIZE,
                             DEFAULT_CONSUMER_WINDOW_SIZE,
                             DEFAULT_CONSUMER_MAX_RATE,
                             DEFAULT_SEND_WINDOW_SIZE,
                             DEFAULT_PRODUCER_MAX_RATE,
                             DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                             DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                             true,
                             true,
                             DEFAULT_AUTO_GROUP,
                             DEFAULT_MAX_CONNECTIONS,
                             DEFAULT_PRE_ACKNOWLEDGE,
                             DEFAULT_RETRY_INTERVAL,
                             DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                             DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                             DEFAULT_MAX_RETRIES_AFTER_FAILOVER,
                             name);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }

   }

   public Context createContext() throws NamingException
   {
      return context;
   }

   public void createQueue(String name)
   {
      Boolean result;
      try
      {
         result = (Boolean)invokeSyncOperation(ObjectNames.getJMSServerObjectName(), "createQueue", name, name);
         Assert.assertEquals(true, result.booleanValue());
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void createQueueConnectionFactory(String name)
   {
      createConnectionFactory(name);
   }

   public void createTopic(String name)
   {
      Boolean result;
      try
      {
         result = (Boolean)invokeSyncOperation(ObjectNames.getJMSServerObjectName(), "createTopic", name, name);
         Assert.assertEquals(true, result.booleanValue());
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void createTopicConnectionFactory(String name)
   {
      createConnectionFactory(name);
   }

   public void deleteConnectionFactory(String name)
   {
      try
      {
         invokeSyncOperation(ObjectNames.getJMSServerObjectName(), "destroyConnectionFactory", name);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void deleteQueue(String name)
   {
      Boolean result;
      try
      {
         result = (Boolean)invokeSyncOperation(ObjectNames.getJMSServerObjectName(), "destroyQueue", name);
         Assert.assertEquals(true, result.booleanValue());
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void deleteQueueConnectionFactory(String name)
   {
      deleteConnectionFactory(name);
   }

   public void deleteTopic(String name)
   {
      Boolean result;
      try
      {
         result = (Boolean)invokeSyncOperation(ObjectNames.getJMSServerObjectName(), "destroyTopic", name);
         Assert.assertEquals(true, result.booleanValue());
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void deleteTopicConnectionFactory(String name)
   {
      deleteConnectionFactory(name);
   }

   public String getName()
   {
      return this.getClass().getName();
   }

   public void startServer() throws Exception
   {
      serverProcess = SpawnedVMSupport.spawnVM(SpawnedJMSServer.class.getName(), false);
      InputStreamReader isr = new InputStreamReader(serverProcess.getInputStream());
      final BufferedReader br = new BufferedReader(isr);
      String line = null;
      while ((line = br.readLine()) != null)
      {
         System.out.println(line);
         line.replace('|', '\n');
         if (line.startsWith("Listening for transport"))
         {
            continue;
         }
         else if ("OK".equals(line.trim()))
         {
            new Thread()
            {
               public void run()
               {
                  try
                  {
                     String line = null;
                     while ((line = br.readLine()) != null)
                     {
                        System.out.println("JoramServerOutput: " + line);
                     }
                  }
                  catch (Exception e)
                  {
                     e.printStackTrace();
                  }
               }
            }.start();
            return;
         }
         else
         {
            // something went wrong with the server, destroy it:
            serverProcess.destroy();
            throw new IllegalStateException("Unable to start the spawned server :" + line);
         }
      }
   }

   public void stopServer() throws Exception
   {
      OutputStreamWriter osw = new OutputStreamWriter(serverProcess.getOutputStream());
      osw.write("STOP\n");
      osw.flush();
      int exitValue = serverProcess.waitFor();
      if (exitValue != 0)
      {
         serverProcess.destroy();
      }
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private Object invokeSyncOperation(ObjectName objectName, String operationName, Object... parameters)
   {
      ClientMessage message = clientSession.createClientMessage(false);
      ManagementHelper.putOperationInvocation(message, objectName, operationName, parameters);
      ClientMessage reply;
      try
      {
         reply = requestor.request(message, 3000);
      }
      catch (Exception e)
      {
         throw new IllegalStateException("Exception while invoking " + operationName + " on " + objectName, e);
      }
      if (reply == null)
      {
         throw new IllegalStateException("no reply received when invoking " + operationName + " on " + objectName);
      }
      if (!ManagementHelper.hasOperationSucceeded(reply))
      {
         throw new IllegalStateException("opertation failed when invoking " + operationName +
                                         " on " +
                                         objectName +
                                         ": " +
                                         ManagementHelper.getOperationExceptionMessage(reply));

      }
      return reply.getProperty(new SimpleString(operationName));
   }

   // Inner classes -------------------------------------------------

}
