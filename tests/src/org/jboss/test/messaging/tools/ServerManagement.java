/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools;

import org.jboss.jms.tools.ServerWrapper;
import org.jboss.jms.util.InVMInitialContextFactory;
import org.jboss.jms.server.ServerPeer;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerManagement
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   private static ServerWrapper serverWrapper;

   public synchronized static void startInVMServer() throws Exception
   {
      if (serverWrapper != null)
      {
         throw new Exception("server already started!");
      }
      serverWrapper = new ServerWrapper(InVMInitialContextFactory.getJNDIEnvironment());
      serverWrapper.start();
   }

   public synchronized static void stopInVMServer() throws Exception
   {
      if (serverWrapper == null)
      {
         return;
      }
      serverWrapper.stop();
      serverWrapper = null;
   }

   public static void deployTopic(String name) throws Exception
   {
      insureStarted();
      serverWrapper.deployTopic(name);
   }

   public static void undeployTopic(String name) throws Exception
   {
      insureStarted();
      serverWrapper.undeployTopic(name);
   }

   public static void deployQueue(String name) throws Exception
   {
      insureStarted();
      serverWrapper.deployQueue(name);
   }

   public static void undeployQueue(String name) throws Exception
   {
      insureStarted();
      serverWrapper.undeployQueue(name);
   }

   public static ServerPeer getServerPeer() throws Exception
   {
      insureStarted();
      return serverWrapper.getServerPeer();
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private static void insureStarted() throws Exception
   {
      if (serverWrapper == null)
      {
         throw new Exception("The server has not been started!");
      }
   }

   // Inner classes -------------------------------------------------
}
