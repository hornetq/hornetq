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
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import javax.management.ObjectName;
import javax.management.NotificationListener;
import javax.management.Notification;
import javax.transaction.UserTransaction;
import org.jboss.jms.server.DestinationManager;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.test.messaging.tools.jmx.rmi.LocalTestServer;
import org.jboss.test.messaging.tools.jmx.rmi.RMITestServer;
import org.jboss.test.messaging.tools.jmx.rmi.Server;
import org.jboss.test.messaging.tools.jmx.rmi.NotificationListenerID;
import org.jboss.test.messaging.tools.jmx.ServiceAttributeOverrides;
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

   private static ServerHolder[] servers = new ServerHolder[MAX_SERVER_COUNT];

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

   public static boolean isClustered()
   {
      return "true".equals(System.getProperty("test.clustered"));
   }

   /**
    * May return null if the server is not initialized.
    */
   public synchronized static Server getServer()
   {
      return getServer(0);
   }

   /**
    * May return null if the corresponding server is not initialized.
    */
   public synchronized static Server getServer(int i)
   {
      if (servers[i] == null)
      {
         return null;
      }
      else
      {
         return ((ServerHolder)servers[i]).getServer();
      }
   }

   public static synchronized Server create() throws Exception
   {
      return create(0);
   }

   /**
    * Makes sure that a "hollow" TestServer (either local or remote, depending on the nature of the
    * test), exists and it's ready to be started.
    */
   public static synchronized Server create(int i) throws Exception
   {
      if (servers[i] == null)
      {
         if (isLocal())
         {
            servers[i] = new ServerHolder(new LocalTestServer(i), false);
         }
         else
         {
            Server s = acquireRemote(2, i, true);

            if (s != null)
            {
               servers[i] = new ServerHolder(s, false);
            }
            else
            {
               // most likely the remote server is not started, so spawn it
               servers[i] = new ServerHolder(ServerManagement.spawn(i), true);
               log.info("server " + i + " online");
            }
         }
      }

      return servers[i].getServer();
   }


   /**
    * Will clear the database at startup.
    */
   public static void start(String config) throws Exception
   {
      start(0, config, true);
   }

   /**
    * Will clear the database at startup.
    */
   public static void start(int i, String config) throws Exception
   {
      start(i, config, true);
   }

   public static void start(int i, String config, boolean clearDatabase) throws Exception
   {
      start(i, config, null, clearDatabase);
   }

   public static void start(int i, String config,
                            ServiceAttributeOverrides attrOverrides,
                            boolean clearDatabase) throws Exception
   {
      start(i, config, attrOverrides, clearDatabase, true);
   }

   /**
    * When this method correctly completes, the server (local or remote) is started and fully
    * operational (the service container and the server peer are created and started).
    */
   public static synchronized void start(int i, String config,
                                         ServiceAttributeOverrides attrOverrides,
                                         boolean clearDatabase,
                                         boolean startMessagingServer) throws Exception
   {
      Server s = create(i);

      log.info("starting server " + i);

      s.start(config, attrOverrides, clearDatabase, startMessagingServer);

      log.info("server " + i + " started");
   }


   public static synchronized boolean isStarted(int i)
   {
      if (servers[i] == null)
      {
         return false;
      }

      try
      {
         return servers[i].getServer().isStarted();
      }
      catch(Exception e)
      {
         log.warn("Exception on isStarted", e);
         return false;
      }
   }

   public static synchronized void stop() throws Exception
   {
      stop(0);
   }

   /**
    * The method stops the local or remote server, bringing it to a "hollow" state. A stopped
    * server is identical with a server that has just been created, but not started.
    * @return true if the server was effectively stopped, or false if the server was alreayd stopped
    *         when the method was invoked.
    */
   public static synchronized boolean stop(int i) throws Exception
   {
      if (servers[i] == null)
      {
         log.warn("server " + i + " has not been created, so it cannot be stopped");
         return false;
      }
      else
      {
         boolean stopped = servers[i].getServer().stop();
         if (stopped)
         {
            log.info("server " + i + " stopped");
         }
         return stopped;
      }
   }

   /**
    * Abruptly kills the VM running the specified server, simulating a crash. A local server
    * cannot be killed, the method is a noop if this is the case.
    */
   public static synchronized void kill(int i) throws Exception
   {
      if (servers[i] == null)
      {
         log.warn("server " + i + " has not been created, so it cannot be killed");
      }
      else
      {
         log.trace("invoking kill() on server " + i);
         servers[i].getServer().kill();
         log.info("server " + i + " killed");
         servers[i] = null;
      }
   }

   /**
    * Kills the server and waits keep trying any dumb communication until the server is effectively
    * killed. We had to implement this method as kill will actually schedule a thread that will
    * perform System.exit after few milliseconds. We will use this method in places where we need
    * the server killed.
    */
   public static synchronized void killAndWait(int i) throws Exception
   {
      Server server = servers[i].getServer();
      kill(i);
      try
      {
         while(true)
         {
            server.ping();
            log.debug("server " + i + " still alive ...");
            Thread.sleep(10);
         }
      }
      catch (Throwable e)
      {
        // e.printStackTrace();
      }

      log.debug("server " + i + " killed and dead");
   }

   /**
    * This method make sure that all servers that have been implicitely spawned when as a side
    * effect of create() and/or start() are killed. The method is important because a forked
    * ant junit task won't exit if processes created by it are still active. If you run tests
    * from ant, always call killSpawnedServers() in tearDown().
    *
    * The servers created directed invoking spawn() are not subject to destroySpawnedServers(); they
    * need to be explicitely killed.
    *
    * @return a List<Integer> containing the indexes of the destroyed servers.
    *
    */
   public static synchronized List destroySpawnedServers() throws Exception
   {
      List destroyed = new ArrayList();

      for(int i = 0; i < servers.length; i++)
      {
         if (servers[i] != null && servers[i].isSpawned())
         {
            Server s = servers[i].getServer();
            destroyed.add(new Integer(s.getServerID()));
            s.stop();
            s.kill();
            servers[i] = null;
         }
      }

      return destroyed;
   }

   /**
    * For a local test, is a noop, but for a remote test, the method call spawns a new VM,
    * irrespective of the fact that a server with same index may already exist (if you want to
    * avoid conflicts, you need to check this externally).
    *
    * The remote server so created is no different from a server started using start-rmi-server
    * script.
    */
   private static synchronized Server spawn(final int i) throws Exception
   {
      if (isLocal())
      {
         return null;
      }

      StringBuffer sb = new StringBuffer();

      sb.append("java").append(' ');

      sb.append("-Xmx512M").append(' ');


      String remoteDebugIndex = System.getProperty("test.remote.debug.index");
      if (remoteDebugIndex != null)
      {
         int index = Integer.parseInt(remoteDebugIndex);

         sb.append("-Xdebug -Xnoagent -Djava.compiler=NONE ").
            append("-Xrunjdwp:transport=dt_shmem,server=n,suspend=n,address=rmiserver_").
            append(index).append(' ');
      }

      String moduleOutput = System.getProperty("module.output");
      if (moduleOutput == null)
      {
         moduleOutput = "./output";
      }

      sb.append("-Dmodule.output=").append(moduleOutput).append(' ');

      sb.append("-Dtest.bind.address=localhost").append(' ');

      String database = System.getProperty("test.database");
      if (database != null)
      {
         sb.append("-Dtest.database=").append(database).append(' ');
      }

      String serialization = System.getProperty("test.serialization");
      if (serialization != null)
      {
         sb.append("-Dtest.serialization=").append(serialization).append(' ');
      }

      sb.append("-Dtest.server.index=").append(i).append(' ');

      String clustered = System.getProperty("test.clustered");
      if (clustered != null && clustered.trim().length() == 0)
      {
         clustered = null;
      }
      if (clustered != null)
      {
         sb.append("-Dtest.clustered=").append(clustered).append(' ');
      }

      String remoting = System.getProperty("test.remoting");
      if (remoting != null)
      {
         sb.append("-Dtest.remoting=").append(remoting).append(' ');
      }

      String testLogfileSuffix = System.getProperty("test.logfile.suffix");

      if (testLogfileSuffix == null)
      {
         testLogfileSuffix = "undefined-test-type";
      }
      else
      {
         int pos;
         if ((pos = testLogfileSuffix.lastIndexOf("client")) != -1)
         {
            testLogfileSuffix = testLogfileSuffix.substring(0, pos) + "server";
         }

         // We need to add the i even in the non clustered case since we can have multiple
         // non clustered servers
         testLogfileSuffix += i;
      }

      sb.append("-Dtest.logfile.suffix=").append(testLogfileSuffix).append(' ');

      String classPath = System.getProperty("java.class.path");

      //System.out.println("CLASSPATH: " + classPath);

      if (System.getProperty("os.name").equals("Linux"))
      {
         sb.append("-cp").append(" ").append(classPath).append(" ");
      }
      else
      {
         sb.append("-cp").append(" \"").append(classPath).append("\" ");
      }

      // As there is a problem with Multicast and JGroups on Linux (in certain JDKs)
      // The stack introduced by multiplexer might fail under Linux if we don't have this
      if (System.getProperty("os.name").equals("Linux"))
      {
         sb.append(" -Djava.net.preferIPv4Stack=true ");
      }

      sb.append("org.jboss.test.messaging.tools.jmx.rmi.RMITestServer");

      String commandLine = sb.toString();

      //System.out.println(commandLine);

      Process process = Runtime.getRuntime().exec(commandLine);

      log.trace("process: " + process);

      // if you ever need to debug the spawing process, turn this flag to true:

      String parameterVerbose = System.getProperty("test.spawn.verbose");
      final boolean verbose = parameterVerbose!=null && parameterVerbose.equals("true");

      final BufferedReader rs = new BufferedReader(new InputStreamReader(process.getInputStream()));
      final BufferedReader re = new BufferedReader(new InputStreamReader(process.getErrorStream()));

      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               String line;

               while((line = rs.readLine()) != null)
               {
                  if (verbose)
                  {
                     System.out.println("SERVER " + i + " STDOUT: " + line);
                  }
               }
            }
            catch(Exception e)
            {
               log.error("exception", e);
            }
         }

      }, "Server " + i + " STDOUT reader thread").start();

      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               String line;

               while((line = re.readLine()) != null)
               {
                  if (verbose)
                  {
                     System.out.println("SERVER " + i + " STDERR: " + line);
                  }
               }
            }
            catch(Exception e)
            {
               log.error("exception", e);
            }
         }

      }, "Server " + i + " STDERR reader thread").start();


      // put the invoking thread on wait until the server is actually up and running and bound
      // in the RMI registry

      long maxWaitTime = 30; // seconds
      long startTime = System.currentTimeMillis();
      Server s = null;

      log.info("spawned server " + i + ", waiting for it to come online");

      while(System.currentTimeMillis() - startTime < maxWaitTime * 1000)
      {
         s = acquireRemote(1, i, true);
         if (s != null)
         {
            break;
         }
      }

      if (s == null)
      {
         log.error("Cannot contact newly spawned server " + i + ", most likely the attempt failed, timing out ...");
         throw new Exception("Cannot contact newly spawned server " + i + ", most likely the attempt failed, timing out ...");
      }

      return s;
   }


   public static ObjectName deploy(String mbeanConfiguration) throws Exception
   {
      insureStarted();
      return servers[0].getServer().deploy(mbeanConfiguration);
   }

   public static void undeploy(ObjectName on) throws Exception
   {
      insureStarted();
      servers[0].getServer().undeploy(on);
   }

   public static Object getAttribute(ObjectName on, String attribute) throws Exception
   {
      return getAttribute(0, on, attribute);
   }

   public static Object getAttribute(int serverIndex, ObjectName on, String attribute)
      throws Exception
   {
      insureStarted(serverIndex);
      return servers[serverIndex].getServer().getAttribute(on, attribute);
   }


   public static void setAttribute(ObjectName on, String name, String valueAsString)
      throws Exception
   {
      insureStarted();
      servers[0].getServer().setAttribute(on, name, valueAsString);
   }

   public static Object invoke(ObjectName on, String operationName,
                               Object[] params, String[] signature) throws Exception
   {
      insureStarted();
      return servers[0].getServer().invoke(on, operationName, params, signature);
   }

   public static void addNotificationListener(int serverIndex, ObjectName on,
                                              NotificationListener listener) throws Exception
   {
      insureStarted(serverIndex);

      if (isLocal())
      {
         // add the listener directly to the server
         servers[serverIndex].getServer().addNotificationListener(on, listener);
      }
      else
      {
         // is remote, need to poll
         NotificationListenerPoller p =
            new NotificationListenerPoller(((ServerHolder)servers[serverIndex]).getServer(),
                                           on, listener);

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
      insureStarted(serverIndex);

      if (isLocal())
      {
         // remove the listener directly
         servers[serverIndex].getServer().removeNotificationListener(on, listener);
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

   /**
    * Install dynamically an AOP advice that will do "bad things" on the server, simulating all
    * sorts of failures. I expect the name of this method to be refactored as we learn more about
    * this type of testing.
    */
   public static void poisonTheServer(int serverIndex, int type) throws Exception
   {
      insureStarted(serverIndex);
      servers[serverIndex].getServer().poisonTheServer(type);
      // TODO (ovidiu): this is prone to race conditions, as somebody from the client may try to
      //       use (and create) an new server that is being poisoned, while the poisoned server is
      //       still alive.
      servers[serverIndex] = null;
   }


   public static Set query(ObjectName pattern) throws Exception
   {
      insureStarted();
      return servers[0].getServer().query(pattern);
   }

   public static UserTransaction getUserTransaction() throws Exception
   {
      insureStarted();
      return servers[0].getServer().getUserTransaction();
   }

   public static void log(int level, String text)
   {
      log(level, text, 0);
   }

   public static void log(int level, String text, int index)
   {
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
            servers[index].getServer().log(level, text);
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
      startServerPeer(serverPeerID, defaultQueueJNDIContext, defaultTopicJNDIContext, null);
   }

   /**
    * @param serverPeerID - if null, the jboss-service.xml value will be used.
    * @param defaultQueueJNDIContext - if null, the jboss-service.xml value will be used.
    * @param defaultTopicJNDIContext - if null, the jboss-service.xml value will be used.
    */
   public static void startServerPeer(int serverPeerID,
                                      String defaultQueueJNDIContext,
                                      String defaultTopicJNDIContext,
                                      ServiceAttributeOverrides attrOverrids) throws Exception
   {
      insureStarted();
      servers[0].getServer().startServerPeer(serverPeerID, defaultQueueJNDIContext,
                                             defaultTopicJNDIContext, attrOverrids, false);
   }

   public static void stopServerPeer() throws Exception
   {
      insureStarted();
      servers[0].getServer().stopServerPeer();
   }

   public static boolean isServerPeerStarted() throws Exception
   {
      insureStarted();
      return servers[0].getServer().isServerPeerStarted();
   }

   public static ObjectName getServerPeerObjectName() throws Exception
   {
      insureStarted();
      return servers[0].getServer().getServerPeerObjectName();
   }

   /**
    * @return a Set<String> with the subsystems currently registered with the Connector.
    *         This method is supposed to work locally as well as remotely.
    */
   public static Set getConnectorSubsystems() throws Exception
   {
      insureStarted();
      return servers[0].getServer().getConnectorSubsystems();
   }

   /**
    * Add a ServerInvocationHandler to the remoting Connector. This method is supposed to work
    * locally as well as remotely.
    */
   public static void addServerInvocationHandler(String subsystem,
                                                 ServerInvocationHandler handler) throws Exception
   {
      insureStarted();
      servers[0].getServer().addServerInvocationHandler(subsystem, handler);
   }

   /**
    * Remove a ServerInvocationHandler from the remoting Connector. This method is supposed to work
    * locally as well as remotely.
    */
   public static void removeServerInvocationHandler(String subsystem)
      throws Exception
   {
      insureStarted();
      servers[0].getServer().removeServerInvocationHandler(subsystem);
   }

   public static MessageStore getMessageStore() throws Exception
   {
      insureStarted();
      return servers[0].getServer().getMessageStore();
   }

   public static DestinationManager getDestinationManager()
      throws Exception
   {
      insureStarted();
      return servers[0].getServer().getDestinationManager();
   }

   public static PersistenceManager getPersistenceManager()
      throws Exception
   {
      insureStarted();
      return servers[0].getServer().getPersistenceManager();
   }

   public static void configureSecurityForDestination(String destName, String config)
      throws Exception
   {
      configureSecurityForDestination(0, destName, config);
   }

   public static void configureSecurityForDestination(int serverID, String destName, String config)
      throws Exception
   {
      insureStarted(serverID);
      servers[serverID].getServer().configureSecurityForDestination(destName, config);
   }

   public static void setDefaultSecurityConfig(String config) throws Exception
   {
      insureStarted();
      servers[0].getServer().setDefaultSecurityConfig(config);
   }

   public static String getDefaultSecurityConfig() throws Exception
   {
      insureStarted();
      return servers[0].getServer().getDefaultSecurityConfig();
   }

   /**
    * Simulates a topic deployment (copying the topic descriptor in the deploy directory).
    */
   public static void deployTopic(String name, int serverIndex) throws Exception
   {
      insureStarted(serverIndex);
      servers[serverIndex].getServer().deployTopic(name, null, true);
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
      servers[0].getServer().deployTopic(name, jndiName, false);
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
      servers[0].getServer().deployTopic(name, jndiName, fullSize, pageSize, downCacheSize, false);
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
      undeployDestination(false, name, serverIndex);
   }

   /**
    * Creates a topic programatically.
    */
   public static void createTopic(String name, String jndiName) throws Exception
   {
      insureStarted();
      servers[0].getServer().createTopic(name, jndiName);
   }

   /**
    * Destroys a programatically created topic.
    */
   public static boolean destroyTopic(String name) throws Exception
   {
      return servers[0].getServer().destroyDestination(false, name);
   }

   /**
    * Simulates a queue deployment (copying the queue descriptor in the deploy directory).
    */
   public static void deployQueue(String name, int serverIndex) throws Exception
   {
      insureStarted(serverIndex);
      servers[serverIndex].getServer().deployQueue(name, null, true);
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
      servers[0].getServer().deployQueue(name, jndiName, false);
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
      servers[0].getServer().deployQueue(name, jndiName, fullSize, pageSize, downCacheSize, false);
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
      undeployDestination(true, name, serverIndex);
   }

   /**
    * Creates a queue programatically.
    */
   public static void createQueue(String name, String jndiName) throws Exception
   {
      insureStarted();
      servers[0].getServer().createQueue(name, jndiName);
   }

   /**
    * Destroys a programatically created queue.
    */
   public static boolean destroyQueue(String name) throws Exception
   {
      return servers[0].getServer().destroyDestination(true, name);
   }

   /**
    * Simulates a destination un-deployment (deleting the destination descriptor from the deploy
    * directory).
    */
   private static void undeployDestination(boolean isQueue, String name) throws Exception
   {
      insureStarted();
      servers[0].getServer().undeployDestination(isQueue, name);
   }

   /**
    * Simulates a destination un-deployment (deleting the destination descriptor from the deploy
    * directory).
    */
   private static void undeployDestination(boolean isQueue, String name, int serverIndex)
      throws Exception
   {
      insureStarted(serverIndex);
      servers[serverIndex].getServer().undeployDestination(isQueue, name);
   }

   public static void deployConnectionFactory(String objectName,
                                              String[] jndiBindings,
                                              int prefetchSize,
                                              int defaultTempQueueFullSize,
                                              int defaultTempQueuePageSize,
                                              int defaultTempQueueDownCacheSize)
      throws Exception
   {
      servers[0].getServer().deployConnectionFactory(objectName,
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
      servers[0].getServer().deployConnectionFactory(objectName, jndiBindings, prefetchSize);
   }

   public static void deployConnectionFactory(String objectName,
                                              String[] jndiBindings)
      throws Exception
   {
      servers[0].getServer().deployConnectionFactory(objectName, jndiBindings);
   }

   public static void undeployConnectionFactory(ObjectName objectName) throws Exception
   {
      servers[0].getServer().undeployConnectionFactory(objectName);
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

   public static Server acquireRemote(int initialRetries, int index, boolean quiet)
   {
      String name =
         "//localhost:" + RMITestServer.DEFAULT_REGISTRY_PORT + "/" +
         RMITestServer.RMI_SERVER_PREFIX + index;

      Server s = null;
      int retries = initialRetries;

      while (s == null && retries > 0)
      {
         int attempt = initialRetries - retries + 1;
         try
         {
            String msg = "trying to connect to the remote RMI server " + index +
               (attempt == 1 ? "" : ", attempt " + attempt);

            if(quiet)
            {
               log.debug(msg);
            }
            else
            {
               log.info(msg);
            }

            s = (Server)Naming.lookup(name);

            log.debug("connected to remote server " + index);
         }
         catch(Exception e)
         {
            log.debug("failed to get the RMI server stub, attempt " +
               (initialRetries - retries + 1), e);

            try
            {
               Thread.sleep(500);
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

   public static String getRemotingTransport(int serverIndex) throws Exception
   {
      insureStarted(serverIndex);
      return servers[serverIndex].getServer().getRemotingTransport();
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
   
   private static void insureStarted(int i) throws Exception
   {
      if (servers[i] == null)
      {
         throw new Exception("Server " + i + " has not been created!");
      }

      if (!servers[i].getServer().isStarted())
      {
         throw new Exception("Server " + i + " has not been started!");
      }
   }

   // Inner classes -------------------------------------------------

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

   private static class ServerHolder
   {
      private Server server;
      private boolean spawned;

      ServerHolder(Server server, boolean spawned)
      {
         this.server = server;
         this.spawned = spawned;
      }

      public Server getServer()
      {
         return server;
      }

      public boolean isSpawned()
      {
         return spawned;
      }
   }

}
