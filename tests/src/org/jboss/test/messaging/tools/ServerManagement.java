/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.test.messaging.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.rmi.Naming;
import java.util.Hashtable;
import java.util.Set;

import javax.management.ObjectName;

import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.plugin.contract.ChannelMapper;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.remoting.transport.Connector;
import org.jboss.test.messaging.tools.jmx.rmi.LocalTestServer;
import org.jboss.test.messaging.tools.jmx.rmi.RMITestServer;
import org.jboss.test.messaging.tools.jmx.rmi.Server;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactory;
import org.jboss.test.messaging.tools.jndi.RemoteInitialContextFactory;

/**
 * Collection of static methods to use to start/stop and interact with the in-memory JMS server. It
 * is also use to start/stop a remote server.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerManagement
{
   // Constants -----------------------------------------------------

   // logging levels used by the remote client to forward log output on a remote server
   public static int FATAL = 0;
   public static int ERROR = 1;
   public static int WARN = 2;
   public static int INFO = 3;
   public static int DEBUG = 4;
   public static int TRACE = 5;

   public static final String DEFAULT_QUEUE_CONTEXT = "/queue";
   public static final String DEFAULT_TOPIC_CONTEXT = "/topic";

   // Static --------------------------------------------------------

   private static Logger log = Logger.getLogger(ServerManagement.class);

   private static final int RMI_SERVER_LOOKUP_RETRIES = 10;

   private static Server server;
   private static volatile Process process;
   private static Thread vmStarter;

   public static boolean isLocal()
   {
      return !"true".equals(System.getProperty("remote"));
   }

   public static boolean isRemote()
   {
      return !isLocal();
   }
   
   public static Server getServer()
   {
      return server;
   }

   public static synchronized void create() throws Exception
   {
      if (server != null)
      {
         return;
      }

      if (isLocal())
      {
         server = new LocalTestServer();
         return;
      }

      server = acquireRemote(RMI_SERVER_LOOKUP_RETRIES);

      if (server != null)
      {
         // RMI server started
         return;

      }

      // the remote RMI server is not started

      // I could attempt to start the remote server VM from the test itself (see commented out code)
      // but when running such a test from a forking ant, ant blocks forever waiting for *this* VM
      // to exit. That's why I require the remote server to be started in advance.

      throw new IllegalStateException("The RMI server doesn't seem to be started. " +
                                      "Start it and re-run the test.");

//      // start the service container and the JMS server in a different VM
//      vmStarter = new Thread(new VMStarter(), "External VM Starter Thread");
//      vmStarter.setDaemon(true);
//      vmStarter.start();
//
//      server = acquireRemote(RMI_SERVER_LOOKUP_RETRIES);
//
//      if (server == null)
//      {
//         throw new IllegalStateException("Cannot find remote server " +
//                                         TestServer.RMI_SERVER_NAME + " in registry");
//      }
   }

   public static synchronized void start(String config) throws Exception
   {
      create();

      if (isLocal())
      {
         log.info("IN-VM TEST");
      }
      else
      {
         log.info("REMOTE TEST");
      }

      server.start(config);

      log.debug("server started");
   }

   public static synchronized void stop() throws Exception
   {
      if (server != null)
      {
         server.stop();
      }
   }

   public static synchronized void destroy() throws Exception
   {
      stop();

      server.destroy();

      if (isRemote())
      {
         log.debug("destroying the remote server VM");
         process.destroy();
         log.debug("remote server VM destroyed");
      }
      server = null;
   }

   public static void disconnect() throws Exception
   {
      if (isRemote())
      {
         server = null;
         process = null;
         if (vmStarter != null)
         {
            vmStarter.interrupt();
            vmStarter = null;
         }
      }
   }

   public static ObjectName deploy(String mbeanConfiguration) throws Exception
   {
      insureStarted();
      return server.deploy(mbeanConfiguration);
   }

   public static void undeploy(ObjectName on) throws Exception
   {
      insureStarted();
      server.undeploy(on);
   }

   public static Object getAttribute(ObjectName on, String attribute) throws Exception
   {
      insureStarted();
      return server.getAttribute(on, attribute);
   }

   public static void setAttribute(ObjectName on, String name, String valueAsString)
      throws Exception
   {
      insureStarted();
      server.setAttribute(on, name, valueAsString);
   }

   public static Object invoke(ObjectName on, String operationName,
                               Object[] params, String[] signature) throws Exception
   {
      insureStarted();
      return server.invoke(on, operationName, params, signature);
   }

   public static Set query(ObjectName pattern) throws Exception
   {
      insureStarted();
      return server.query(pattern);
   }

   public static void log(int level, String text)
   {
      if (isRemote())
      {
         if (server == null)
         {
            log.debug("The remote server has not been created yet " +
                      "so this log won't make it to the server!");
            return;
         }
         
         try
         {
            server.log(level, text);
         }
         catch(Exception e)
         {
            log.error("failed to forward the logging request to the remote server", e);
         }
      }
   }

   public static void startServerPeer() throws Exception
   {
      startServerPeer(null, null, null);
   }

   /**
    * @param serverPeerID - if null, the jboss-service.xml value will be used.
    * @param defaultQueueJNDIContext - if null, the jboss-service.xml value will be used.
    * @param defaultTopicJNDIContext - if null, the jboss-service.xml value will be used.
    */
   public static void startServerPeer(String serverPeerID,
                                      String defaultQueueJNDIContext,
                                      String defaultTopicJNDIContext) throws Exception
   {
      insureStarted();
      server.startServerPeer(serverPeerID, defaultQueueJNDIContext, defaultTopicJNDIContext);
   }

   public static void stopServerPeer() throws Exception
   {
      insureStarted();
      server.stopServerPeer();
   }

   public static boolean isServerPeerStarted() throws Exception
   {
      insureStarted();
      return server.isServerPeerStarted();
   }

   public static ObjectName getServerPeerObjectName() throws Exception
   {
      insureStarted();
      return server.getServerPeerObjectName();
   }

   public static ObjectName getChannelMapperObjectName() throws Exception
   {
      insureStarted();
      return server.getChannelMapperObjectName();
   }

   public static Connector getConnector() throws Exception
   {
      if (isRemote())
      {
         throw new IllegalStateException("Cannot get a remote connector!");
      }

      insureStarted();
      return server.getConnector();
   }

   public static MessageStore getMessageStore() throws Exception
   {
      insureStarted();
      return server.getMessageStore();
   }

   public static DestinationManager getDestinationManager()
      throws Exception
   {
      insureStarted();
      return server.getDestinationManager();
   }

   public static ChannelMapper getChannelMapper()
      throws Exception
   {
      insureStarted();
      return server.getChannelMapper();
   }

   public static void configureSecurityForDestination(String destName, String config)
      throws Exception
   {
      insureStarted();
      server.configureSecurityForDestination(destName, config);
   }
   
   public static void setDefaultSecurityConfig(String config) throws Exception
   {
      insureStarted();
      server.setDefaultSecurityConfig(config);
   }
   
   public static String getDefaultSecurityConfig() throws Exception
   {
      insureStarted();
      return server.getDefaultSecurityConfig();
   }

   public static void deployTopic(String name) throws Exception
   {
      deployTopic(name, null);
   }
   
   public static void deployTopic(String name, String jndiName) throws Exception
   {
      insureStarted();
      server.deployTopic(name, jndiName);
   }

   public static void deployQueue(String name) throws Exception
   {
      deployQueue(name, null);
   }
   
   public static void deployQueue(String name, String jndiName) throws Exception
   {
      insureStarted();
      server.deployQueue(name, jndiName);
   }

   public static void undeployQueue(String name) throws Exception
   {
      undeployDestination(true, name);
   }

   public static void undeployTopic(String name) throws Exception
   {
      undeployDestination(false, name);
   }

   private static void undeployDestination(boolean isQueue, String name) throws Exception
   {
      insureStarted();
      server.undeployDestination(isQueue, name);
   }

   public static Hashtable getJNDIEnvironment()
   {
      if (isLocal())
      {
         return InVMInitialContextFactory.getJNDIEnvironment();
      }
      else
      {
         return RemoteInitialContextFactory.getJNDIEnvironment();
      }
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private static void insureStarted() throws Exception
   {
      if (server == null)
      {
         throw new Exception("The server has not been created!");
      }
      if (!server.isStarted())
      {
         throw new Exception("The server has not been started!");
      }
   }

   private static Server acquireRemote(int initialRetries)
   {
      String name = "//localhost:" + RMITestServer.RMI_REGISTRY_PORT + "/" + RMITestServer.RMI_SERVER_NAME;
      Server s = null;
      int retries = initialRetries;
      while(s == null && retries > 0)
      {
         int attempt = initialRetries - retries + 1;
         try
         {
            log.info("trying to connect to the remote RMI server" +
                     (attempt == 1 ? "" : ", attempt " + attempt));
            s = (Server)Naming.lookup(name);
            log.info("connected to the remote server");
         }
         catch(Exception e)
         {
            log.debug("failed to get the RMI server stub, attempt " +
                      (initialRetries - retries + 1), e);

            try
            {
               Thread.sleep(1500);
            }
            catch(InterruptedException e2)
            {
               // OK
            }

            retries--;
         }
      }
      return s;
   }


   // Inner classes -------------------------------------------------

   static class VMStarter implements Runnable
   {
      public void run()
      {
         // start a remote java process that runs a TestServer

         String userDir = System.getProperty("user.dir");
         String javaClassPath = System.getProperty("java.class.path");
         String fileSeparator = System.getProperty("file.separator");
         String javaHome = System.getProperty("java.home");
         String moduleOutput = System.getProperty("module.output");

         String osName = System.getProperty("os.name").toLowerCase();
         boolean isWindows = osName.indexOf("windows") != -1;

         String javaExecutable =
            javaHome + fileSeparator + "bin" + fileSeparator + "java" + (isWindows ? ".exe" : "");

         String[] cmdarray = new String[]
         {
            javaExecutable,
            "-cp",
            javaClassPath,
            "-Dmodule.output=" + moduleOutput,
            "-Dremote.test.suffix=-remote",
            "org.jboss.test.messaging.tools.jmx.rmi.TestServer",
         };

         String[] environment;
         if (isWindows)
         {
            environment = new String[]
            {
               "SYSTEMROOT=C:\\WINDOWS" // TODO get this from environment, as it may be diffrent on different machines
            };
         }
         else
         {
            environment = new String[0];
         }

         Runtime runtime = Runtime.getRuntime();

         try
         {
            log.debug("creating external process");

            Thread stdoutLogger = new Thread(new RemoteProcessLogger(RemoteProcessLogger.STDOUT),
                                             "Remote VM STDOUT Logging Thread");
            Thread stderrLogger = new Thread(new RemoteProcessLogger(RemoteProcessLogger.STDERR),
                                             "Remote VM STDERR Logging Thread");

            stdoutLogger.setDaemon(true);
            stdoutLogger.setDaemon(true);
            stdoutLogger.start();
            stderrLogger.start();

            process = runtime.exec(cmdarray, environment, new File(userDir));
         }
         catch(Exception e)
         {
            log.error("Error spawning remote server", e);
         }
      }
   }

   /**
    * This logger is used to get and display the output generated at stdout or stderr by the
    * RMI server VM.
    */
   static class RemoteProcessLogger implements Runnable
   {
      public static final int STDOUT = 0;
      public static final int STDERR = 1;

      private int type;
      private BufferedReader br;
      private PrintStream out;

      public RemoteProcessLogger(int type)
      {
         this.type = type;

         if (type == STDOUT)
         {
            out = System.out;
         }
         else if (type == STDERR)
         {
            out = System.err;
         }
         else
         {
            throw new IllegalArgumentException("Unknown type " + type);
         }
      }

      public void run()
      {
         while(process == null)
         {
            try
            {
               Thread.sleep(50);
            }
            catch(InterruptedException e)
            {
               // OK
            }
         }

         if (type == STDOUT)
         {
            br = new BufferedReader(new InputStreamReader(process.getInputStream()));
         }
         else if (type == STDERR)
         {
            br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
         }

         String line;
         try
         {
            while((line = br.readLine()) != null)
            {
               out.println(line);
            }
         }
         catch(Exception e)
         {
            log.error("failed to read from process " + process, e);
         }
      }
   }
}
