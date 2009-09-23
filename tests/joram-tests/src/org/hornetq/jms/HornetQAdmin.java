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
import java.util.HashMap;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import junit.framework.Assert;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientRequestor;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.management.impl.ManagementHelper;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.integration.transports.netty.NettyConnectorFactory;
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
      ClientSessionFactoryImpl sf = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()));
      clientSession = sf.createSession(ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_USER,
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
         invokeSyncOperation(ResourceNames.JMS_SERVER,
                             "createConnectionFactory",
                             name,
                             NettyConnectorFactory.class.getName(),
                             new HashMap<String, Object>(),
                             new String[] {name});
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
         result = (Boolean)invokeSyncOperation(ResourceNames.JMS_SERVER, "createQueue", name, name);
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
         result = (Boolean)invokeSyncOperation(ResourceNames.JMS_SERVER, "createTopic", name, name);
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
         invokeSyncOperation(ResourceNames.JMS_SERVER, "destroyConnectionFactory", name);
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
         result = (Boolean)invokeSyncOperation(ResourceNames.JMS_SERVER, "destroyQueue", name);
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
         result = (Boolean)invokeSyncOperation(ResourceNames.JMS_SERVER, "destroyTopic", name);
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
         System.out.println("SERVER: " + line);
         line.replace('|', '\n');
         if ("OK".equals(line.trim()))
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
                        System.out.println("server output: " + line);
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

   private Object invokeSyncOperation(String resourceName, String operationName, Object... parameters)
      throws Exception
   {
      ClientMessage message = clientSession.createClientMessage(false);
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
