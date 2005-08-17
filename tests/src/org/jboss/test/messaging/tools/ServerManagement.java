/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools;

import org.jboss.messaging.tools.jmx.ServiceContainer;
import org.jboss.messaging.tools.jmx.RemotingJMXWrapper;
import org.jboss.jms.server.ServerPeer;
import org.jboss.remoting.transport.Connector;

import javax.transaction.TransactionManager;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerManagement
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   private static ServiceContainer sc;
   private static ServerPeer serverPeer;

   public synchronized static void startInVMServer() throws Exception
   {
      startInVMServer("transaction,remoting", null);
   }

   public synchronized static void startInVMServer(TransactionManager tm) throws Exception
   {
      startInVMServer("transaction,remoting", tm);
   }

   public synchronized static void startInVMServer(String config) throws Exception
   {
      startInVMServer(config, null);
   }

   /**
    * @param tm - specifies a specific TransactionManager instance to bind into the mbeanServer.
    *        If null, the default JBoss TransactionManager implementation will be used.
    */
   public synchronized static void startInVMServer(String config, TransactionManager tm)
         throws Exception
   {
      if (sc != null)
      {
         throw new Exception("server already started!");
      }
      sc = new ServiceContainer(config, tm);
      sc.start();
      serverPeer = new ServerPeer("ServerPeer0");
      serverPeer.start();
   }



   public synchronized static void stopInVMServer() throws Exception
   {
      if (sc == null)
      {
         return;
      }
      serverPeer.stop();
      serverPeer = null;
      sc.stop();
      sc = null;
   }

   public static ServerPeer getServerPeer() throws Exception
   {
      return serverPeer;
   }

   public static Connector getConnector() throws Exception
   {
      RemotingJMXWrapper remoting =
            (RemotingJMXWrapper)sc.getService(ServiceContainer.REMOTING_OBJECT_NAME);
      return remoting.getConnector();
   }

   public static void deployTopic(String name) throws Exception
   {
      insureStarted();
      serverPeer.getDestinationManager().createTopic(name);
   }

   public static void undeployTopic(String name) throws Exception
   {
      insureStarted();
      serverPeer.getDestinationManager().destroyTopic(name);
   }

   public static void deployQueue(String name) throws Exception
   {
      insureStarted();
      serverPeer.getDestinationManager().createQueue(name);
   }

   public static void undeployQueue(String name) throws Exception
   {
      insureStarted();
      serverPeer.getDestinationManager().destroyQueue(name);
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private static void insureStarted() throws Exception
   {
      if (sc == null)
      {
         throw new Exception("The server has not been started!");
      }
   }

   // Inner classes -------------------------------------------------
}
