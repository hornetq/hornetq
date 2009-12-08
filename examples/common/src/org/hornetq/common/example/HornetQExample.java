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
package org.hornetq.common.example;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.naming.InitialContext;

/**
 * a baee class for examples. This takes care of starting and stopping the server as well as deploying any queue needed.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public abstract class HornetQExample
{
   protected static Logger log = Logger.getLogger(HornetQExample.class.getName());

   private Process[] servers;

   private Connection conn;

   private boolean failure = false;

   private String serverClasspath;

   private String serverProps;

   public abstract boolean runExample() throws Exception;

   private boolean logServerOutput;

   private String[] configs;

   protected void run(final String[] configs)
   {
      String runServerProp = System.getProperty("hornetq.example.runServer");
      String logServerOutputProp = System.getProperty("hornetq.example.logserveroutput");
      serverClasspath = System.getProperty("hornetq.example.server.classpath");
      boolean runServer = runServerProp == null ? true : Boolean.valueOf(runServerProp);
      logServerOutput = logServerOutputProp == null ? false : Boolean.valueOf(logServerOutputProp);
      serverProps = System.getProperty("hornetq.example.server.args");
      if (System.getProperty("hornetq.example.server.override.args") != null)
      {
         serverProps = System.getProperty("hornetq.example.server.override.args");
      }
      System.out.println("serverProps = " + serverProps);
      HornetQExample.log.info("hornetq.example.runServer is " + runServer);

      this.configs = configs;

      try
      {
         if (runServer)
         {
            startServers();
         }

         if (!runExample())
         {
            failure = true;
         }
         System.out.println("example complete");
      }
      catch (Throwable e)
      {
         failure = true;
         e.printStackTrace();
      }
      finally
      {
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (JMSException e)
            {
               // ignore
            }
         }
         if (runServer)
         {
            try
            {
               stopServers();
            }
            catch (Throwable throwable)
            {
               throwable.printStackTrace();
            }
         }
      }
      reportResultAndExit();
   }

   protected void killServer(final int id) throws Exception
   {
      System.out.println("Killing server " + id);

      // We kill the server by creating a new file in the server dir which is checked for by the server
      // We can't use Process.destroy() since this does not do a hard kill - it causes shutdown hooks
      // to be called which cleanly shutdown the server
      File file = new File("server" + id + "/KILL_ME");

      file.createNewFile();
   }

   protected void stopServer(final int id) throws Exception
   {
      System.out.println("Stopping server " + id);

      stopServer(servers[id]);
   }

   protected InitialContext getContext(final int serverId) throws Exception
   {
      String jndiFilename = "server" + serverId + "/client-jndi.properties";
      File jndiFile = new File(jndiFilename);
      HornetQExample.log.info("using " + jndiFile + " for jndi");
      Properties props = new Properties();
      FileInputStream inStream = null;
      try
      {
         inStream = new FileInputStream(jndiFile);
         props.load(inStream);
      }
      finally
      {
         if (inStream != null)
         {
            inStream.close();
         }
      }
      return new InitialContext(props);
   }

   protected void startServer(final int index) throws Exception
   {
      String config = configs[index];
      HornetQExample.log.info("starting server with config '" + config + "' " + "logServerOutput " + logServerOutput);
      servers[index] = SpawnedVMSupport.spawnVM(serverClasspath,
                                                "HornetQServer_" + index,
                                                SpawnedHornetQServer.class.getName(),
                                                serverProps,
                                                logServerOutput,
                                                "STARTED::",
                                                "FAILED::",
                                                config,
                                                "hornetq-beans.xml");
   }

   private void startServers() throws Exception
   {
      servers = new Process[configs.length];
      for (int i = 0; i < configs.length; i++)
      {
         startServer(i);
      }
   }

   private void stopServers() throws Exception
   {
      for (Process server : servers)
      {
         if (server != null)
         {
            stopServer(server);
         }
      }
   }

   private void stopServer(final Process server) throws Exception
   {
      if (!System.getProperty("os.name").contains("Windows") && !System.getProperty("os.name").contains("Mac OS X"))
      {
         if (server.getInputStream() != null)
         {
            server.getInputStream().close();
         }
         if (server.getErrorStream() != null)
         {
            server.getErrorStream().close();
         }
      }
      server.destroy();
   }

   private void reportResultAndExit()
   {
      if (failure)
      {
         System.err.println();
         System.err.println("#####################");
         System.err.println("###    FAILURE!   ###");
         System.err.println("#####################");
         System.exit(1);
      }
      else
      {
         System.out.println();
         System.out.println("#####################");
         System.out.println("###    SUCCESS!   ###");
         System.out.println("#####################");
         System.exit(0);
      }
   }
}
