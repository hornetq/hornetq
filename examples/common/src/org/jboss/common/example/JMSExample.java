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
package org.jboss.common.example;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * a baee class for examples. This takes care of starting and stopping the server as well as deploying any queue needed.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public abstract class JMSExample
{
   protected static Logger log = Logger.getLogger(JMSExample.class.getName());

   private Process[] servers;

   private Connection conn;

   private boolean failure = false;

   public abstract boolean runExample() throws Exception;
   
   private boolean logServerOutput;
   
   private String[] allVMArgs;
   
   private String[] configs;
   
   private static final String[] defaultArgs = new String[] {"-Xms512M",
                                                             "-Xmx512M",
                                                             "-XX:+UseParallelGC",
                                                             "-XX:+AggressiveOpts",
                                                             "-XX:+UseFastAccessorMethods"};
      
   protected void run(String[] serverVMArgs, String[] configs)
   {
      String runServerProp = System.getProperty("jbm.example.runServer");
      String logServerOutputProp = System.getProperty("jbm.example.logserveroutput");
      boolean runServer = runServerProp == null ? true : Boolean.valueOf(runServerProp);
      logServerOutput = logServerOutputProp == null?false:Boolean.valueOf(logServerOutputProp);
      log.info("jbm.example.runServer is " + runServer);
            
      allVMArgs = new String[serverVMArgs == null ? 1 : serverVMArgs.length + 1];
      if (serverVMArgs != null)
      {
         System.arraycopy(serverVMArgs, 0, allVMArgs, 0, serverVMArgs.length);
      }      
      String logProps = System.getProperty("java.util.logging.config.file");
      allVMArgs[allVMArgs.length - 1] = "-Djava.util.logging.config.file=" + logProps;
      
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

   protected void run(String[] args)
   {
      run(defaultArgs, args);
   }

   protected void killServer(int id) throws Exception
   {
      System.out.println("Killing server " + id);
      
      // We kill the server by creating a new file in the server dir which is checked for by the server
      // We can't use Process.destroy() since this does not do a hard kill - it causes shutdown hooks
      // to be called which cleanly shutdown the server
      File file = new File("server" + id + "/KILL_ME");
      
      file.createNewFile();
   }
   
   protected void stopServer(int id) throws Exception 
   {
      System.out.println("Stopping server " + id);
      
      stopServer(servers[id]);
   }
   
   protected InitialContext getContext(int serverId) throws Exception
   {
      String jndiFilename = "server" + serverId + "/client-jndi.properties";
      File jndiFile = new File(jndiFilename);
      log.info("using " + jndiFile + " for jndi");
      Properties props = new Properties();
      FileInputStream inStream = null;
      try
      {
         inStream = new FileInputStream(jndiFile);
         props.load(inStream);
      }
      finally
      {
         if(inStream != null)
         {
            inStream.close();
         }
      }
      return new InitialContext(props);
   }
   
   protected void startServer(int index) throws Exception
   {
      String config = configs[index];
      log.info("starting server with config '" + config + "' " + "logServerOutput " + logServerOutput);      
      StringBuilder args = new StringBuilder();
      for (int i = 0; i < allVMArgs.length; i++)
      {
         args.append(allVMArgs[i]);
         if (i != allVMArgs.length - 1)
         {
            args.append(",");
         }
      }
      log.info("and vm args: " + args.toString());
      servers[index] = SpawnedVMSupport.spawnVM(
            SpawnedJMSServer.class.getName(),
            allVMArgs,
            logServerOutput,
            "STARTED::",
            "FAILED::",
            config,
            "jbm-jboss-beans.xml");   
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
         stopServer(server);
      }
   }
   
   private void stopServer(Process server) throws Exception
   {
      if (server.getInputStream() != null)
      {
         server.getInputStream().close();
      }
      if (server.getErrorStream() != null)
      {
         server.getErrorStream().close();
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
