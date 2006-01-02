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

import java.util.Hashtable;
import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.rmi.Naming;

import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.StateManager;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.jmx.rmi.RMIServer;
import org.jboss.test.messaging.tools.jmx.rmi.Server;
import org.jboss.test.messaging.tools.jndi.RemoteInitialContextFactory;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactory;
import org.jboss.messaging.core.MessageStore;
import org.jboss.remoting.transport.Connector;
import org.w3c.dom.Element;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

/**
 * Collection of static methods to use to start/stop and interact with the in-memory JMS server.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerManagement
{
   // Constants -----------------------------------------------------

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

   public static synchronized void create() throws Exception
   {
      if (server != null)
      {
         return;
      }

      if (isLocal())
      {
         server = new RMIServer(false);
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
//                                         RMIServer.RMI_SERVER_NAME + " in registry");
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

   public static void startServerPeer() throws Exception
   {
      insureStarted();
      server.startServerPeer();
   }

   public static void stopServerPeer() throws Exception
   {
      insureStarted();
      server.stopServerPeer();
   }

   public static ServerPeer getServerPeer() throws Exception
   {
      if (isRemote())
      {
         throw new IllegalStateException("Cannot get a remote server peer!");
      }

      insureStarted();
      return server.getServerPeer();
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

   public static StateManager getStateManager() throws Exception
   {
      insureStarted();
      return server.getStateManager();
   }

   public static void setSecurityConfig(String destName, String config) throws Exception
   {
      insureStarted();
      server.setSecurityConfig(destName, config);
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

   public static void undeployTopic(String name) throws Exception
   {
      insureStarted();
      server.undeployTopic(name);
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
      insureStarted();
      server.undeployQueue(name);
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

   public static Element stringToElement(String s) throws Exception
   {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder parser = factory.newDocumentBuilder();
      Document doc = parser.parse(new InputSource(new StringReader(s)));
      return doc.getDocumentElement();
   }

   public static String elementToString(Node n) throws Exception
   {

      String name = n.getNodeName();
      if (name.startsWith("#"))
      {
         return "";
      }

      StringBuffer sb = new StringBuffer();
      sb.append('<').append(name);

      NamedNodeMap attrs = n.getAttributes();
      if (attrs != null)
      {
         for(int i = 0; i < attrs.getLength(); i++)
         {
            Node attr = attrs.item(i);
            sb.append(' ').append(attr.getNodeName() + "=\"" + attr.getNodeValue() + "\"");
         }
      }

      NodeList children = n.getChildNodes();

      if (children.getLength() == 0)
      {
         sb.append("/>").append('\n');
      }
      else
      {
         sb.append('>').append('\n');
         for(int i = 0; i < children.getLength(); i++)
         {
            sb.append(elementToString(children.item(i)));

         }
         sb.append("</").append(name).append('>');
      }
      return sb.toString();
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
      String name = "//localhost:" + RMIServer.RMI_REGISTRY_PORT + "/" + RMIServer.RMI_SERVER_NAME;
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
         // start a remote java process that runs a RMIServer

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
            "org.jboss.test.messaging.tools.jmx.rmi.RMIServer",
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
