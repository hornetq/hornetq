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

import java.rmi.Naming;
import java.util.Hashtable;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import javax.management.ObjectName;
import javax.management.NotificationListener;
import javax.management.Notification;
import javax.transaction.UserTransaction;
import org.jboss.jms.message.MessageIdGeneratorFactory;
import org.jboss.jms.server.DestinationManager;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.test.messaging.tools.jmx.rmi.LocalTestServer;
import org.jboss.test.messaging.tools.jmx.rmi.RMITestServer;
import org.jboss.test.messaging.tools.jmx.rmi.Server;
import org.jboss.test.messaging.tools.jmx.rmi.NotificationListenerID;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactory;
import org.jboss.test.messaging.tools.jndi.RemoteInitialContextFactory;

/**
 * Collection of static methods to use to start/stop and interact with the in-memory JMS server. It
 * is also use to start/stop a remote server.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerManagement
{
   // Constants -----------------------------------------------------

   public static final int MAX_SERVER_COUNT = 10;

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

   private static Server[] servers = new Server[MAX_SERVER_COUNT];
   
   private static boolean[] killed = new boolean[MAX_SERVER_COUNT];
   
   // Map<NotificationListener - NotificationListenerPoller>
   private static Map notificationListenerPollers = new HashMap();

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
      return getServer(0);
   }
   
   public static Server getServer(int i)
   {
      if (killed[i])
      {
         log.warn("Server " + i + " cannot be got since it has been killed");
      }
      
      Server s = servers[i];
      
      if (s == null)
      {
         throw new IllegalStateException("Server " + i + " is not started!");
      }
      
      return s;
   }
   
   public static synchronized void create() throws Exception
   {
      create(0);
   }

   public static synchronized void create(int index) throws Exception
   {
      if (killed[index])
      {
         log.warn("Server " + index + " cannot created since it has been killed");
         
         return;
      }
      
      if (servers[index] != null)
      {
         return;
      }

      if (isLocal())
      {
         servers[index] = new LocalTestServer(index);
         return;
      }

      servers[index] = acquireRemote(RMI_SERVER_LOOKUP_RETRIES, index);

      if (servers[index] != null)
      {
         // RMI server started
         return;

      }

      // the remote RMI server is not started

      // I could attempt to start the remote server VM from the test itself (see commented out code)
      // but when running such a test from a forking ant, ant blocks forever waiting for *this* VM
      // to exit. That's why I require the remote server to be started in advance.

      throw new IllegalStateException("The RMI server " + index + " doesn't seem to be started. " +
                                      "Start it and re-run the test.");

   }
   
   public static synchronized void start(String config) throws Exception
   {
      start(config, 0);
   }

   public static synchronized void start(String config, int index) throws Exception
   {
      if (killed[index])
      {
         log.warn("Server " + index + " cannot been started since it has been killed");
         
         return;
      }
      
      create(index);

      if (isLocal())
      {
         log.info("IN-VM TEST");
      }
      else
      {
         log.info("REMOTE TEST");
      }
      
      MessageIdGeneratorFactory.instance.clear();      

      // Now start the server
      servers[index].start(config);

      log.debug("server started");
   }

   public static synchronized void stop() throws Exception
   {
      stop(0);
   }

   public static synchronized void stop(int index) throws Exception
   {
      if (killed[index])
      {
         log.warn("Server " + index + " cannot been stopped since it has been killed");
         
         return;
      }
      
      if (servers[index] == null)
      {
         log.warn("Server " + index + " has not been created, so it cannot be stopped");
         return;
      }

      if (!servers[index].isStarted())
      {
         log.warn("Server " + index + " either has not been started, or it is stopped already");
         return;
      }

      servers[index].stop();
   }

   /**
    * TODO - this methods should be removed, to not be confused with kill(index)
    * @deprecated
    */
   public static synchronized void destroy() throws Exception
   {
      stop();
      servers[0].kill();
      servers[0] = null;
   }

   /**
    * Abruptly kills the VM running the specified server.
    */
   public static synchronized void kill(int index) throws Exception
   {
      if (servers[index] == null)
      {
         log.warn("Server " + index + " has not been created, so it cannot be killed");
         return;
      }

      servers[index].kill();
      servers[index] = null;
      
      killed[index] = true;
   }
   
   public static synchronized boolean isKilled(int index)
   {
      return killed[index];
   }

   public static void disconnect() throws Exception
   {
      if (isRemote())
      {
         servers[0] = null;
      }
   }

   public static ObjectName deploy(String mbeanConfiguration) throws Exception
   {
      insureStarted();
      return servers[0].deploy(mbeanConfiguration);
   }

   public static void undeploy(ObjectName on) throws Exception
   {
      insureStarted();
      servers[0].undeploy(on);
   }

   public static Object getAttribute(ObjectName on, String attribute) throws Exception
   {
      insureStarted();
      return servers[0].getAttribute(on, attribute);
   }

   public static void setAttribute(ObjectName on, String name, String valueAsString)
      throws Exception
   {
      insureStarted();
      servers[0].setAttribute(on, name, valueAsString);
   }

   public static Object invoke(ObjectName on, String operationName,
                               Object[] params, String[] signature) throws Exception
   {
      insureStarted();
      return servers[0].invoke(on, operationName, params, signature);
   }

   public static void addNotificationListener(int serverIndex, ObjectName on,
                                              NotificationListener listener) throws Exception
   {
      if (killed[serverIndex])
      {
         log.warn("Server " + serverIndex + " cannot addNotificationListener it has been killed");
         
         return;
      }
      
      insureStarted(serverIndex);

      if (isLocal())
      {
         // add the listener directly to the server
         servers[serverIndex].addNotificationListener(on, listener);
      }
      else
      {
         // is remote, need to poll
         NotificationListenerPoller p =
            new NotificationListenerPoller((Server)servers[serverIndex], on, listener);

         synchronized(notificationListenerPollers)
         {
            notificationListenerPollers.put(listener, p);
         }

         new Thread(p, "Poller for " + Integer.toHexString(p.hashCode())).start();
      }
   }

   public static void removeNotificationListener(int serverIndex, ObjectName on,
                                                 NotificationListener listener) throws Exception
   {
      if (killed[serverIndex])
      {
         log.warn("Server " + serverIndex + " cannot removeNotificationListener it has been killed");
         
         return;
      }
      
      insureStarted(serverIndex);

      if (isLocal())
      {
         // remove the listener directly
         servers[serverIndex].removeNotificationListener(on, listener);
      }
      else
      {
         // is remote

         NotificationListenerPoller p = null;
         synchronized(notificationListenerPollers)
         {
            p = (NotificationListenerPoller)notificationListenerPollers.remove(listener);
         }

         if (p != null)
         {
            // stop the polling thread
            p.stop();
         }
      }
   }

   public static Set query(ObjectName pattern) throws Exception
   {
      insureStarted();
      return servers[0].query(pattern);
   }

   public static UserTransaction getUserTransaction() throws Exception
   {
      insureStarted();
      return servers[0].getUserTransaction();
   }
   
   public static void log(int level, String text)
   {
      log(level, text, 0);
   }

   public static void log(int level, String text, int index)
   {
      if (killed[index])
      {
         log.warn("Server " + index + " cannot log it has been killed");
         
         return;
      }
      
      if (isRemote())
      {
         if (servers[index] == null)
         {
            log.debug("The remote server " + index + " has not been created yet " +
                      "so this log won't make it to the server!");
            return;
         }

         try
         {
            servers[index].log(level, text);
         }
         catch(Exception e)
         {
            log.error("failed to forward the logging request to the remote server", e);
         }
      }
   }

   public static void startServerPeer() throws Exception
   {
      startServerPeer(0, null, null);
   }

   /**
    * @param serverPeerID - if null, the jboss-service.xml value will be used.
    * @param defaultQueueJNDIContext - if null, the jboss-service.xml value will be used.
    * @param defaultTopicJNDIContext - if null, the jboss-service.xml value will be used.
    */
   public static void startServerPeer(int serverPeerID,
                                      String defaultQueueJNDIContext,
                                      String defaultTopicJNDIContext) throws Exception
   {
      insureStarted();
      servers[0].startServerPeer(serverPeerID, defaultQueueJNDIContext, defaultTopicJNDIContext, false);
   }

   public static void stopServerPeer() throws Exception
   {
      insureStarted();
      servers[0].stopServerPeer();
   }

   public static boolean isServerPeerStarted() throws Exception
   {
      insureStarted();
      return servers[0].isServerPeerStarted();
   }

   public static ObjectName getServerPeerObjectName() throws Exception
   {
      insureStarted();
      return servers[0].getServerPeerObjectName();
   }

   /**
    * @return a Set<String> with the subsystems currently registered with the Connector.
    *         This method is supposed to work locally as well as remotely.
    */
   public static Set getConnectorSubsystems() throws Exception
   {
      insureStarted();
      return servers[0].getConnectorSubsystems();
   }

   /**
    * Add a ServerInvocationHandler to the remoting Connector. This method is supposed to work
    * locally as well as remotely.
    */
   public static void addServerInvocationHandler(String subsystem,
                                                 ServerInvocationHandler handler) throws Exception
   {
      insureStarted();
      servers[0].addServerInvocationHandler(subsystem, handler);
   }

   /**
    * Remove a ServerInvocationHandler from the remoting Connector. This method is supposed to work
    * locally as well as remotely.
    */
   public static void removeServerInvocationHandler(String subsystem)
      throws Exception
   {
      insureStarted();
      servers[0].removeServerInvocationHandler(subsystem);
   }

   public static MessageStore getMessageStore() throws Exception
   {
      insureStarted();
      return servers[0].getMessageStore();
   }

   public static DestinationManager getDestinationManager()
      throws Exception
   {
      insureStarted();
      return servers[0].getDestinationManager();
   }

   public static PersistenceManager getPersistenceManager()
      throws Exception
   {
      insureStarted();
      return servers[0].getPersistenceManager();
   }

   public static void configureSecurityForDestination(String destName, String config)
      throws Exception
   {
      insureStarted();
      servers[0].configureSecurityForDestination(destName, config);
   }

   public static void setDefaultSecurityConfig(String config) throws Exception
   {
      insureStarted();
      servers[0].setDefaultSecurityConfig(config);
   }

   public static String getDefaultSecurityConfig() throws Exception
   {
      insureStarted();
      return servers[0].getDefaultSecurityConfig();
   }
   
   /**
    * Simulates a topic deployment (copying the topic descriptor in the deploy directory).
    */
   public static void deployClusteredTopic(String name, int serverIndex) throws Exception
   {
      if (killed[serverIndex])
      {
         log.warn("Server " + serverIndex + " cannot deployClusteredTopic it has been killed");
      }
      
      insureStarted(serverIndex);
      servers[serverIndex].deployTopic(name, null, true);
   }

   /**
    * Simulates a topic deployment (copying the topic descriptor in the deploy directory).
    */
   public static void deployTopic(String name) throws Exception
   {
      deployTopic(name, null);
   }

   /**
    * Simulates a topic deployment (copying the topic descriptor in the deploy directory).
    */
   public static void deployTopic(String name, String jndiName) throws Exception
   {
      insureStarted();
      servers[0].deployTopic(name, jndiName, false);
   }

   /**
    * Simulates a topic deployment (copying the topic descriptor in the deploy directory).
    */
   public static void deployTopic(String name, int fullSize, int pageSize, int downCacheSize)
      throws Exception
   {
      deployTopic(name, null, fullSize, pageSize, downCacheSize);
   }

   /**
    * Simulates a topic deployment (copying the topic descriptor in the deploy directory).
    */
   public static void deployTopic(String name, String jndiName, int fullSize, int pageSize,
                                  int downCacheSize) throws Exception
   {
      insureStarted();
      servers[0].deployTopic(name, jndiName, fullSize, pageSize, downCacheSize, false);
   }

   /**
    * Simulates a topic un-deployment (deleting the topic descriptor from the deploy directory).
    */
   public static void undeployTopic(String name) throws Exception
   {
      undeployDestination(false, name);
   }
   
   /**
    * Simulates a topic un-deployment (deleting the topic descriptor from the deploy directory).
    */
   public static void undeployTopic(String name, int serverIndex) throws Exception
   {
      if (killed[serverIndex])
      {
         log.warn("Server " + serverIndex + " cannot undeployTopic it has been killed");
      }
      
      undeployDestination(false, name, serverIndex);
   }

   /**
    * Creates a topic programatically.
    */
   public static void createTopic(String name, String jndiName) throws Exception
   {
      insureStarted();
      servers[0].createTopic(name, jndiName);
   }

   /**
    * Destroys a programatically created topic.
    */
   public static boolean destroyTopic(String name) throws Exception
   {
      return servers[0].destroyDestination(false, name);
   }
   
   /**
    * Simulates a queue deployment (copying the queue descriptor in the deploy directory).
    */
   public static void deployClusteredQueue(String name, int serverIndex) throws Exception
   {
      if (killed[serverIndex])
      {
         log.warn("Server " + serverIndex + " cannot deployClusteredQueue it has been killed");
      }
      
      insureStarted(serverIndex);
      servers[serverIndex].deployQueue(name, null, true);
   }

   /**
    * Simulates a queue deployment (copying the queue descriptor in the deploy directory).
    */
   public static void deployQueue(String name) throws Exception
   {
      deployQueue(name, null);
   }

   /**
    * Simulates a queue deployment (copying the queue descriptor in the deploy directory).
    */
   public static void deployQueue(String name, String jndiName) throws Exception
   {
      insureStarted();
      servers[0].deployQueue(name, jndiName, false);
   }

   /**
    * Simulates a queue deployment (copying the queue descriptor in the deploy directory).
    */
   public static void deployQueue(String name, int fullSize, int pageSize, int downCacheSize)
      throws Exception
   {
      deployQueue(name, null, fullSize, pageSize, downCacheSize);
   }

   /**
    * Simulates a queue deployment (copying the queue descriptor in the deploy directory).
    */
   public static void deployQueue(String name, String jndiName, int fullSize, int pageSize,
                                  int downCacheSize) throws Exception
   {
      insureStarted();
      servers[0].deployQueue(name, jndiName, fullSize, pageSize, downCacheSize, false);
   }

   /**
    * Simulates a queue un-deployment (deleting the queue descriptor from the deploy directory).
    */
   public static void undeployQueue(String name) throws Exception
   {
      undeployDestination(true, name);
   }
   
   /**
    * Simulates a queue un-deployment (deleting the queue descriptor from the deploy directory).
    */
   public static void undeployQueue(String name, int serverIndex) throws Exception
   {
      if (killed[serverIndex])
      {
         log.warn("Server " + serverIndex + " cannot undeplyQueue it has been killed");
      }
      
      undeployDestination(true, name, serverIndex);
   }

   /**
    * Creates a queue programatically.
    */
   public static void createQueue(String name, String jndiName) throws Exception
   {
      insureStarted();
      servers[0].createQueue(name, jndiName);
   }

   /**
    * Destroys a programatically created queue.
    */
   public static boolean destroyQueue(String name) throws Exception
   {
      return servers[0].destroyDestination(true, name);
   }

   /**
    * Simulates a destination un-deployment (deleting the destination descriptor from the deploy
    * directory).
    */
   private static void undeployDestination(boolean isQueue, String name) throws Exception
   {
      insureStarted();
      servers[0].undeployDestination(isQueue, name);
   }
   
   /**
    * Simulates a destination un-deployment (deleting the destination descriptor from the deploy
    * directory).
    */
   private static void undeployDestination(boolean isQueue, String name, int serverIndex)
      throws Exception
   {
      insureStarted(serverIndex);
      servers[serverIndex].undeployDestination(isQueue, name);
   }
                                                                                                                
   public static void deployConnectionFactory(String objectName,
                                              String[] jndiBindings,
                                              int prefetchSize,
                                              int defaultTempQueueFullSize,
                                              int defaultTempQueuePageSize,
                                              int defaultTempQueueDownCacheSize)
      throws Exception
   {
      servers[0].deployConnectionFactory(objectName,
                                     jndiBindings,
                                     prefetchSize,
                                     defaultTempQueueFullSize,
                                     defaultTempQueuePageSize,
                                     defaultTempQueueDownCacheSize);
   }

   public static void deployConnectionFactory(String objectName,
                                              String[] jndiBindings,
                                              int prefetchSize)
      throws Exception
   {
      servers[0].deployConnectionFactory(objectName, jndiBindings, prefetchSize);
   }
   
   public static void deployConnectionFactory(String objectName,
                                              String[] jndiBindings)
      throws Exception
   {
      servers[0].deployConnectionFactory(objectName, jndiBindings);
   }

   public static void undeployConnectionFactory(ObjectName objectName) throws Exception
   {
      servers[0].undeployConnectionFactory(objectName);
   }

   public static Hashtable getJNDIEnvironment()
   {
      return getJNDIEnvironment(0);
   }
   
   public static Hashtable getJNDIEnvironment(int serverIndex)
   {
      if (isLocal())
      {
         return InVMInitialContextFactory.getJNDIEnvironment(serverIndex);
      }
      else
      {
         return RemoteInitialContextFactory.getJNDIEnvironment(serverIndex);
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
      insureStarted(0);
   }
   
   private static void insureStarted(int index) throws Exception
   {
      if (servers[index] == null)
      {
         throw new Exception("The server " + index + " has not been created!");
      }
      if (!servers[index].isStarted())
      {
         throw new Exception("The server " + index + " has not been started!");
      }
   }

   private static Server acquireRemote(int initialRetries, int index)
   {
      String name =
         "//localhost:" + RMITestServer.DEFAULT_REGISTRY_PORT + "/" +
         RMITestServer.RMI_SERVER_PREFIX + index;

      Server s = null;
      int retries = initialRetries;

      while(s == null && retries > 0)
      {
         int attempt = initialRetries - retries + 1;
         try
         {
            log.info("trying to connect to the remote RMI server " + index + 
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

//   static class VMStarter implements Runnable
//   {
//      public void run()
//      {
//         // start a remote java process that runs a TestServer
//
//         String userDir = System.getProperty("user.dir");
//         String javaClassPath = System.getProperty("java.class.path");
//         String fileSeparator = System.getProperty("file.separator");
//         String javaHome = System.getProperty("java.home");
//         String moduleOutput = System.getProperty("module.output");
//
//         String osName = System.getProperty("os.name").toLowerCase();
//         boolean isWindows = osName.indexOf("windows") != -1;
//
//         String javaExecutable =
//            javaHome + fileSeparator + "bin" + fileSeparator + "java" + (isWindows ? ".exe" : "");
//
//         String[] cmdarray = new String[]
//         {
//            javaExecutable,
//            "-cp",
//            javaClassPath,
//            "-Dmodule.output=" + moduleOutput,
//            "-Dremote.test.suffix=-remote",
//            "org.jboss.test.messaging.tools.jmx.rmi.TestServer",
//         };
//
//         String[] environment;
//         if (isWindows)
//         {
//            environment = new String[]
//            {
//               "SYSTEMROOT=C:\\WINDOWS" // TODO get this from environment, as it may be diffrent on different machines
//            };
//         }
//         else
//         {
//            environment = new String[0];
//         }
//
//         Runtime runtime = Runtime.getRuntime();
//
//         try
//         {
//            log.debug("creating external process");
//
//            Thread stdoutLogger = new Thread(new RemoteProcessLogger(RemoteProcessLogger.STDOUT),
//                                             "Remote VM STDOUT Logging Thread");
//            Thread stderrLogger = new Thread(new RemoteProcessLogger(RemoteProcessLogger.STDERR),
//                                             "Remote VM STDERR Logging Thread");
//
//            stdoutLogger.setDaemon(true);
//            stdoutLogger.setDaemon(true);
//            stdoutLogger.start();
//            stderrLogger.start();
//
//            process = runtime.exec(cmdarray, environment, new File(userDir));
//         }
//         catch(Exception e)
//         {
//            log.error("Error spawning remote server", e);
//         }
//      }
//   }
//
//   /**
//    * This logger is used to get and display the output generated at stdout or stderr by the
//    * RMI server VM.
//    */
//   static class RemoteProcessLogger implements Runnable
//   {
//      public static final int STDOUT = 0;
//      public static final int STDERR = 1;
//
//      private int type;
//      private BufferedReader br;
//      private PrintStream out;
//
//      public RemoteProcessLogger(int type)
//      {
//         this.type = type;
//
//         if (type == STDOUT)
//         {
//            out = System.out;
//         }
//         else if (type == STDERR)
//         {
//            out = System.err;
//         }
//         else
//         {
//            throw new IllegalArgumentException("Unknown type " + type);
//         }
//      }
//
//      public void run()
//      {
//         while(process == null)
//         {
//            try
//            {
//               Thread.sleep(50);
//            }
//            catch(InterruptedException e)
//            {
//               // OK
//            }
//         }
//
//         if (type == STDOUT)
//         {
//            br = new BufferedReader(new InputStreamReader(process.getInputStream()));
//         }
//         else if (type == STDERR)
//         {
//            br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
//         }
//
//         String line;
//         try
//         {
//            while((line = br.readLine()) != null)
//            {
//               out.println(line);
//            }
//         }
//         catch(Exception e)
//         {
//            log.error("failed to read from process " + process, e);
//         }
//      }
//   }

   private static long listenerIDCounter = 0;

   static class NotificationListenerPoller implements Runnable
   {
      public static final int POLL_INTERVAL = 500;

      private long id;
      private Server server;
      private NotificationListener listener;
      private volatile boolean running;

      private synchronized static long generateID()
      {
         return listenerIDCounter++;
      }

      NotificationListenerPoller(Server server, ObjectName on, NotificationListener listener)
         throws Exception
      {
         id = generateID();
         this.server = server;

         server.addNotificationListener(on, new NotificationListenerID(id));

         this.listener = listener;
         this.running = true;
      }

      public void run()
      {
         while(running)
         {
            try
            {
               List notifications = server.pollNotificationListener(id);

               for(Iterator i = notifications.iterator(); i.hasNext(); )
               {
                  Notification n = (Notification)i.next();
                  listener.handleNotification(n, null);
               }

               Thread.sleep(POLL_INTERVAL);
            }
            catch(Exception e)
            {
               log.error(e);
               stop();
            }
         }
      }

      public void stop()
      {
         running = false;
      }
   }

}
