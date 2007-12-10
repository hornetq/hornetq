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

import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.JmsServer;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.test.messaging.tools.container.*;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.transaction.UserTransaction;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.Naming;
import java.util.*;

/**
 * Collection of static methods to use to start/stop and interact with the in-memory JMS server. It
 * is also use to start/stop a remote server.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *          <p/>
 *          $Id$
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

   private static List<Server> servers = new ArrayList<Server>();


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
    * May return null if the corresponding server is not initialized.
    */
   public synchronized static Server getServer(int i)
   {
      return servers.get(i);
   }


   /**
    * Makes sure that a "hollow" TestServer (either local or remote, depending on the nature of the
    * test), exists and it's ready to be started.
    */
   public static synchronized Server create(int i) throws Exception
   {
      if (isLocal())
      {
         log.info("Attempting to create local server " + i);
         return new LocalTestServer(i);
      }
      else
      {
         //Need to spawn a new server - we DON'T use start-rmi-server any more, so we know if the servers[i] is null
         //the server is not there - killing a server sets servers[i] to null
         log.info("Attempting to create remote server " + i);
         return ServerManagement.spawn(i);
      }

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
   public static void start(int i, String config,
                             ServiceAttributeOverrides attrOverrides,
                             boolean clearDatabase,
                             boolean startMessagingServer) throws Exception
   {
      log.info("Attempting to start server " + i);

      //servers.get(i).start(config,  attrOverrides, clearDatabase, startMessagingServer);

      /*Server s = create(i);

      s.start(config, attrOverrides, clearDatabase, startMessagingServer);
*/
      log.info("server " + i + " started");
   }


   public static synchronized boolean isStarted(int i) throws Exception
   {
      return servers.get(i).isStarted();
   }

   public static void stop() throws Exception
   {
      stop(0);
   }

   /**
    * The method stops the local or remote server, bringing it to a "hollow" state. A stopped
    * server is identical with a server that has just been created, but not started.
    *
    * @return true if the server was effectively stopped, or false if the server was alreayd stopped
    *         when the method was invoked.
    */
   public static boolean stop(int i) throws Exception
   {
      return servers.get(i).stop();
   }

   public static synchronized void kill(int i) throws Exception
   {
      log.info("Attempting to kill server " + i);

      if (i == 0)
      {
         //Cannot kill server 0 if there are any other servers since it has the rmi registry in it
         for (int j = 1; j < servers.size(); j++)
         {
            if (servers.get(j) != null)
            {
               throw new IllegalStateException("Cannot kill server 0, since server[" + j + "] still exists");
            }
         }
      }

      if (i > servers.size())
      {
         log.info("server " + i + " has not been created or has already been killed, so it cannot be killed");
      }
      else
      {
         Server server = servers.get(i);
         log.info("invoking kill() on server " + i);
         try
         {
            server.kill();
         }
         catch (Throwable t)
         {
            // This is likely to throw an exception since the server dies before the response is received
         }

         log.info("Waiting for server to die");

         try
         {
            while (true)
            {
               server.ping();
               log.debug("server " + i + " still alive ...");
               Thread.sleep(100);
            }
         }
         catch (Throwable e)
         {
            //Ok
         }

         Thread.sleep(300);

         log.info("server " + i + " killed and dead");
      }

   }

   /**
    * This method make sure that all servers that have been implicitely spawned when as a side
    * effect of create() and/or start() are killed. The method is important because a forked
    * ant junit task won't exit if processes created by it are still active. If you run tests
    * from ant, always call killSpawnedServers() in tearDown().
    * <p/>
    * The servers created directed invoking spawn() are not subject to destroySpawnedServers(); they
    * need to be explicitely killed.
    *
    * @return a List<Integer> containing the indexes of the destroyed servers.
    */
   public static synchronized List destroySpawnedServers() throws Exception
   {
      log.info("################# Destroying spawned servers****");
      List destroyed = new ArrayList();

      for (Server server : servers)
      {
         destroyed.add(new Integer(server.getServerID()));

         log.info("Killing spawned server " + server.getServerID());

         try
         {
            server.kill();
         }
         catch (Throwable t)
         {
         }
      }

      return destroyed;
   }

   /**
    * For a local test, is a noop, but for a remote test, the method call spawns a new VM
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

      String remoteDebugIndex = "";//System.getProperty("test.remote.debug.index");
      if (remoteDebugIndex != null)                             
      {

         //sb.append("-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5006 ");
      }


      String objectStoreDir = System.getProperty("objectstore.dir");

      if (objectStoreDir != null)
      {
         sb.append("-Dobjectstore.dir=" + objectStoreDir).append(" ");
      }

      String moduleOutput = System.getProperty("module.output");
      if (moduleOutput == null)
      {
         moduleOutput = "./output";
      }

      sb.append("-Dmodule.output=").append(moduleOutput).append(' ');

      String bindAddress = System.getProperty("test.bind.address");
      if (bindAddress == null)
      {
         bindAddress = "localhost";
      }

      sb.append("-Dtest.bind.address=").append(bindAddress).append(' ');

      //Use test.bind.address for the jgroups.bind_addr

      String jgroupsBindAddr = bindAddress;

      sb.append("-D").append(org.jgroups.Global.BIND_ADDR).append("=")
              .append(jgroupsBindAddr).append(' ');

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

      String groupName = System.getProperty("jboss.messaging.groupname");
      log.info("******* GROUP NAME IS " + groupName);
      if (groupName != null)
      {
         sb.append("-Djboss.messaging.groupname=").append(groupName).append(' ');
      }

      String dataChannelUDPPort = System.getProperty("jboss.messaging.datachanneludpport");
      log.info("*** data UDP port is " + dataChannelUDPPort);
      if (dataChannelUDPPort != null)
      {
         sb.append("-Djboss.messaging.datachanneludpport=").append(dataChannelUDPPort).append(' ');
      }

      String controlChannelUDPPort = System.getProperty("jboss.messaging.controlchanneludpport");
      log.info("*** control UDP port is " + controlChannelUDPPort);
      if (controlChannelUDPPort != null)
      {
         sb.append("-Djboss.messaging.controlchanneludpport=").append(controlChannelUDPPort).append(' ');
      }

      String dataChannelUDPAddress = System.getProperty("jboss.messaging.datachanneludpaddress");
      log.info("*** data UDP address is " + dataChannelUDPAddress);
      if (dataChannelUDPAddress != null)
      {
         sb.append("-Djboss.messaging.datachanneludpaddress=").append(dataChannelUDPAddress).append(' ');
      }

      String controlChannelUDPAddress = System.getProperty("jboss.messaging.controlchanneludpaddress");
      log.info("*** control UDP address is " + controlChannelUDPAddress);
      if (controlChannelUDPAddress != null)
      {
         sb.append("-Djboss.messaging.controlchanneludpaddress=").append(controlChannelUDPAddress).append(' ');
      }

      sb.append("-Djava.naming.factory.initial=org.jboss.test.messaging.tools.container.InVMInitialContextFactory  ");
      String ipttl = System.getProperty("jboss.messaging.ipttl");
      log.info("*** ip_ttl is " + ipttl);
      if (ipttl != null)
      {
         sb.append("-Djboss.messaging.ipttl=").append(ipttl).append(' ');
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

      sb.append("org.jboss.test.messaging.tools.container.RMITestServer");

      String commandLine = sb.toString();

      Process process = Runtime.getRuntime().exec(commandLine);

      log.trace("process: " + process);

      // if you ever need to debug the spawing process, turn this flag to true:

      String parameterVerbose = "true";//System.getProperty("test.spawn.verbose");
      final boolean verbose = parameterVerbose != null && parameterVerbose.equals("true");

      final BufferedReader rs = new BufferedReader(new InputStreamReader(process.getInputStream()));
      final BufferedReader re = new BufferedReader(new InputStreamReader(process.getErrorStream()));

      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               String line;

               while ((line = rs.readLine()) != null)
               {
                  if (verbose)
                  {
                     System.out.println("SERVER " + i + " STDOUT: " + line);
                  }
               }
            }
            catch (Exception e)
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

               while ((line = re.readLine()) != null)
               {
                  if (verbose)
                  {
                     System.out.println("SERVER " + i + " STDERR: " + line);
                  }
               }
            }
            catch (Exception e)
            {
               log.error("exception", e);
            }
         }

      }, "Server " + i + " STDERR reader thread").start();

      // put the invoking thread on wait until the server is actually up and running and bound
      // in the RMI registry

      log.info("spawned server " + i + ", waiting for it to come online");

      Server s = acquireRemote(500, i, true);

      log.info("Server contacted");

      if (s == null)
      {
         log.error("Cannot contact newly spawned server " + i + ", most likely the attempt failed, timing out ...");
         throw new Exception("Cannot contact newly spawned server " + i + ", most likely the attempt failed, timing out ...");
      }

      return s;
   }


   public static ObjectName deploy(String mbeanConfiguration) throws Exception
   {

      return servers.get(0).deploy(mbeanConfiguration);
   }

   public static void undeploy(ObjectName on) throws Exception
   {

      servers.get(0).undeploy(on);
   }

   public static Object getAttribute(ObjectName on, String attribute) throws Exception
   {
      return getAttribute(0, on, attribute);
   }

   public static Object getAttribute(int serverIndex, ObjectName on, String attribute)
           throws Exception
   {

      return servers.get(serverIndex).getAttribute(on, attribute);
   }


   public static void setAttribute(ObjectName on, String name, String valueAsString)
           throws Exception
   {

      servers.get(0).setAttribute(on, name, valueAsString);
   }

   public static Object invoke(ObjectName on, String operationName,
                               Object[] params, String[] signature) throws Exception
   {

      return servers.get(0).invoke(on, operationName, params, signature);
   }

   public static void addNotificationListener(int serverIndex, ObjectName on,
                                              NotificationListener listener) throws Exception
   {


      if (isLocal())
      {
         // add the listener directly to the server
         servers.get(serverIndex).addNotificationListener(on, listener);
      }
      else
      {
         // is remote, need to poll
         NotificationListenerPoller p =
                 new NotificationListenerPoller(servers.get(serverIndex),
                         on, listener);

         synchronized (notificationListenerPollers)
         {
            notificationListenerPollers.put(listener, p);
         }

         new Thread(p, "Poller for " + Integer.toHexString(p.hashCode())).start();
      }
   }

   public static void removeNotificationListener(int serverIndex, ObjectName on,
                                                 NotificationListener listener) throws Exception
   {


      if (isLocal())
      {
         // remove the listener directly
         servers.get(serverIndex).removeNotificationListener(on, listener);
      }
      else
      {
         // is remote

         NotificationListenerPoller p = null;
         synchronized (notificationListenerPollers)
         {
            p = (NotificationListenerPoller) notificationListenerPollers.remove(listener);
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
    *
    * @return a reference to the server that has been poisoned. Use this reference to kill the
    *         server after use.
    */
   public static Server poisonTheServer(int serverIndex, int type) throws Exception
   {

      Server poisoned = servers.get(serverIndex);

      //We set the server to null so it can be recreated again, but ONLY for those poisons that cause the server to get killed
      //We do not do this for other poisons that don't

      /*if (type != PoisonInterceptor.LONG_SEND && type != PoisonInterceptor.NULL)
      {
         servers.get(0) = null;
      }*/

      poisoned.poisonTheServer(type);

      return poisoned;
   }

   public static UserTransaction getUserTransaction() throws Exception
   {

      return servers.get(0).getUserTransaction();
   }

   public static void log(int level, String text)
   {
      log(level, text, 0);
   }

   public static void log(int level, String text, int index)
   {
      if (isRemote())
      {
         if (servers.get(index) == null)
         {
            log.debug("The remote server " + index + " has not been created yet " +
                    "so this log won't make it to the server!");
            return;
         }

         try
         {
            servers.get(index).log(level, text);
         }
         catch (Exception e)
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
    * @param serverPeerID            - if null, the jboss-service.xml value will be used.
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
    * @param serverPeerID            - if null, the jboss-service.xml value will be used.
    * @param defaultQueueJNDIContext - if null, the jboss-service.xml value will be used.
    * @param defaultTopicJNDIContext - if null, the jboss-service.xml value will be used.
    */
   public static void startServerPeer(int serverPeerID,
                                      String defaultQueueJNDIContext,
                                      String defaultTopicJNDIContext,
                                      ServiceAttributeOverrides attrOverrids) throws Exception
   {

      servers.get(0).startServerPeer(serverPeerID, defaultQueueJNDIContext,
              defaultTopicJNDIContext, attrOverrids, false);
   }

   public static void stopServerPeer() throws Exception
   {

      servers.get(0).stopServerPeer();
   }

   public static boolean isServerPeerStarted() throws Exception
   {

      return servers.get(0).isServerPeerStarted();
   }

   public static ObjectName getServerPeerObjectName() throws Exception
   {

      return servers.get(0).getServerPeerObjectName();
   }

   /**
    * @return a Set<String> with the subsystems currently registered with the Connector.
    *         This method is supposed to work locally as well as remotely.
    */
   public static Set getConnectorSubsystems() throws Exception
   {

      return servers.get(0).getConnectorSubsystems();
   }

   /**
    * Add a ServerInvocationHandler to the remoting Connector. This method is supposed to work
    * locally as well as remotely.
    */
   public static void addServerInvocationHandler(String subsystem,
                                                 ServerInvocationHandler handler) throws Exception
   {

      servers.get(0).addServerInvocationHandler(subsystem, handler);
   }

   /**
    * Remove a ServerInvocationHandler from the remoting Connector. This method is supposed to work
    * locally as well as remotely.
    */
   public static void removeServerInvocationHandler(String subsystem)
           throws Exception
   {

      servers.get(0).removeServerInvocationHandler(subsystem);
   }

   public static MessageStore getMessageStore() throws Exception
   {

      return servers.get(0).getMessageStore();
   }

   public static DestinationManager getDestinationManager()
           throws Exception
   {

      return servers.get(0).getDestinationManager();
   }

   public static PersistenceManager getPersistenceManager()
           throws Exception
   {

      return servers.get(0).getPersistenceManager();
   }

   public static void configureSecurityForDestination(String destName, String config)
           throws Exception
   {
      configureSecurityForDestination(0, destName, config);
   }

   public static void configureSecurityForDestination(int serverID, String destName, String config)
           throws Exception
   {
      //servers.get(0).configureSecurityForDestination(destName, config);
   }

   public static void setDefaultSecurityConfig(String config) throws Exception
   {

      servers.get(0).setDefaultSecurityConfig(config);
   }

   public static String getDefaultSecurityConfig() throws Exception
   {

      return servers.get(0).getDefaultSecurityConfig();
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

            if (quiet)
            {
               log.debug(msg);
            }
            else
            {
               log.info(msg);
            }

            s = (Server) Naming.lookup(name);

            log.debug("connected to remote server " + index);
         }
         catch (Exception e)
         {
            log.debug("failed to get the RMI server stub, attempt " +
                    (initialRetries - retries + 1), e);

            try
            {
               Thread.sleep(500);
            }
            catch (InterruptedException e2)
            {
               // OK
            }

            retries--;
         }
      }

      return s;
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------


   private static JmsServer getJmsServer(int id)
   {
      try
      {
         if (isLocal())
         {
            return servers.get(id).getJmsServer();
         }
         else
         {
            return null;
         }
      }
      catch (Exception e)
      {
         throw new RuntimeException();
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
         while (running)
         {
            try
            {
               List notifications = server.pollNotificationListener(id);

               for (Iterator i = notifications.iterator(); i.hasNext();)
               {
                  Notification n = (Notification) i.next();
                  listener.handleNotification(n, null);
               }

               Thread.sleep(POLL_INTERVAL);
            }
            catch (Exception e)
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
