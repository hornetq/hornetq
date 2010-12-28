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

package org.hornetq.jms;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import junit.framework.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientRequestor;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.tests.util.SpawnedVMSupport;
import org.objectweb.jtests.jms.admin.Admin;

/**
 * A HornetQAdmin
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class HornetQAdmin implements Admin
{

   private ClientSession clientSession;

   private ClientRequestor requestor;

   private Context context;

   private Process serverProcess;

   private ClientSessionFactory sf;
   
   ServerLocator serverLocator;

   public HornetQAdmin()
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
      serverLocator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName()));
      sf = serverLocator.createSessionFactory();
      clientSession = sf.createSession(ConfigurationImpl.DEFAULT_CLUSTER_USER,
                                       ConfigurationImpl.DEFAULT_CLUSTER_PASSWORD,
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
      
      if (sf != null)
      {
         sf.close();
      }
      
      if (serverLocator != null)
      {
         serverLocator.close();
      }
      
      sf = null;
      serverLocator = null;
   }

   public void createConnectionFactory(final String name)
   {
      createConnection(name, 0);
   }
   
   private void createConnection(final String name, final int cfType)
   {
      try
      {
         invokeSyncOperation(ResourceNames.JMS_SERVER,
                             "createConnectionFactory",
                             name,
                             false,
                             false,
                             cfType,
                             "netty",
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

   public void createQueue(final String name)
   {
      Boolean result;
      try
      {
         result = (Boolean)invokeSyncOperation(ResourceNames.JMS_SERVER, "createQueue", name, name);
         Assert.assertEquals(true, result.booleanValue());
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void createQueueConnectionFactory(final String name)
   {
      createConnection(name, 1);
   }

   public void createTopic(final String name)
   {
      Boolean result;
      try
      {
         result = (Boolean)invokeSyncOperation(ResourceNames.JMS_SERVER, "createTopic", name, name);
         Assert.assertEquals(true, result.booleanValue());
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void createTopicConnectionFactory(final String name)
   {
      createConnection(name, 2);
   }

   public void deleteConnectionFactory(final String name)
   {
      try
      {
         invokeSyncOperation(ResourceNames.JMS_SERVER, "destroyConnectionFactory", name);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void deleteQueue(final String name)
   {
      Boolean result;
      try
      {
         result = (Boolean)invokeSyncOperation(ResourceNames.JMS_SERVER, "destroyQueue", name);
         Assert.assertEquals(true, result.booleanValue());
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void deleteQueueConnectionFactory(final String name)
   {
      deleteConnectionFactory(name);
   }

   public void deleteTopic(final String name)
   {
      Boolean result;
      try
      {
         result = (Boolean)invokeSyncOperation(ResourceNames.JMS_SERVER, "destroyTopic", name);
         Assert.assertEquals(true, result.booleanValue());
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void deleteTopicConnectionFactory(final String name)
   {
      deleteConnectionFactory(name);
   }

   public String getName()
   {
      return this.getClass().getName();
   }

   public void startServer() throws Exception
   {
      String[] vmArgs = new String[] { "-Dorg.hornetq.logger-delegate-factory-class-name=org.hornetq.jms.SysoutLoggerDelegateFactory" };
      serverProcess = SpawnedVMSupport.spawnVM(SpawnedJMSServer.class.getName(), vmArgs, false);
      InputStreamReader isr = new InputStreamReader(serverProcess.getInputStream());

      final BufferedReader br = new BufferedReader(isr);
      String line = null;
      while ((line = br.readLine()) != null)
      {
         System.out.println("SERVER: " + line);
         line.replace('|', '\n');
         if ("OK".equals(line.trim()))
         {
            new Thread()
            {
               @Override
               public void run()
               {
                  try
                  {
                     String line = null;
                     while ((line = br.readLine()) != null)
                     {
                        System.out.println("SERVER: " + line);
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
         else if ("KO".equals(line.trim()))
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

   private Object invokeSyncOperation(final String resourceName, final String operationName, final Object... parameters) throws Exception
   {
      ClientMessage message = clientSession.createMessage(false);
      ManagementHelper.putOperationInvocation(message, resourceName, operationName, parameters);
      ClientMessage reply;
      try
      {
         reply = requestor.request(message, 3000);
      }
      catch (Exception e)
      {
         throw new IllegalStateException("Exception while invoking " + operationName + " on " + resourceName, e);
      }
      if (reply == null)
      {
         throw new IllegalStateException("no reply received when invoking " + operationName + " on " + resourceName);
      }
      if (!ManagementHelper.hasOperationSucceeded(reply))
      {
         throw new IllegalStateException("operation failed when invoking " + operationName +
                                         " on " +
                                         resourceName +
                                         ": " +
                                         ManagementHelper.getResult(reply));
      }
      return ManagementHelper.getResult(reply);
   }

   // Inner classes -------------------------------------------------

}
