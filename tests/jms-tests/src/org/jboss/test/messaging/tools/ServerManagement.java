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
package org.jboss.test.messaging.tools;

import java.rmi.Naming;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.test.messaging.tools.container.InVMInitialContextFactory;
import org.jboss.test.messaging.tools.container.LocalTestServer;
import org.jboss.test.messaging.tools.container.RMITestServer;
import org.jboss.test.messaging.tools.container.Server;


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

   /**
    * Makes sure that a "hollow" TestServer (either local or remote, depending on the nature of the
    * test), exists and it's ready to be started.
    */
   public static synchronized Server create(int i) throws Exception
   {
      log.info("Attempting to create local server " + i);
      return new LocalTestServer(i);
   }

   public static void start(int i, String config, boolean clearDatabase) throws Exception
   {
      start(i, config, clearDatabase, true);
   }

   /**
    * When this method correctly completes, the server (local or remote) is started and fully
    * operational (the service container and the server peer are created and started).
    */
   public static void start(int i, String config,
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
      servers.get(0).stop();
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

   public static void log(int level, String text)
   {
      log(level, text, 0);
   }

   public static void log(int level, String text, int index)
   {
      try
      {
         servers.get(0).log(level, text);
      }
      catch (Exception e)
      {
         log.error("failed to forward the logging request to the remote server", e);
      }
   }

   public static void startServerPeer() throws Exception
   {
      startServerPeer(0);
   }

   /**
    * @param serverPeerID            - if null, the jboss-service.xml value will be used.
    */
   public static void startServerPeer(int serverPeerID) throws Exception
   {

      servers.get(0).startServerPeer(serverPeerID);
   }

   public static void stopServerPeer() throws Exception
   {

      servers.get(0).stopServerPeer();
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

   public static Hashtable getJNDIEnvironment()
   {
      return getJNDIEnvironment(0);
   }

   public static Hashtable getJNDIEnvironment(int serverIndex)
   {
      return InVMInitialContextFactory.getJNDIEnvironment(serverIndex);
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


//   private static JmsServer getJmsServer(int id)
//   {
//      try
//      {
//         if (isLocal())
//         {
//            return servers.get(id).getJmsServer();
//         }
//         else
//         {
//            return null;
//         }
//      }
//      catch (Exception e)
//      {
//         throw new RuntimeException();
//      }
//   }
   // Inner classes -------------------------------------------------

}
