/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools;

import org.jboss.jms.tools.ServerWrapper;

import java.util.Hashtable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerManagement
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   private static Hashtable jndiEnvironment;
   private static ServerWrapper serverWrapper;


   public static Hashtable getJNDIEnvironment()
   {
      if (jndiEnvironment == null)
      {
         jndiEnvironment = new Hashtable();
         jndiEnvironment.put("java.naming.factory.initial",
                             "org.jboss.test.messaging.tools.InVMInitialContextFactory");
         jndiEnvironment.put("java.naming.provider.url", "irrelevant");
         jndiEnvironment.put("java.naming.factory.url.pkgs", "irrelevant");
      }
      return jndiEnvironment;
   }

   public synchronized static void startInVMServer() throws Exception
   {
      if (serverWrapper != null)
      {
         throw new Exception("server already started!");
      }
      serverWrapper = new ServerWrapper(getJNDIEnvironment());
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
      if (serverWrapper == null)
      {
         throw new Exception("The server has not been started!");
      }
      serverWrapper.deployTopic(name);
   }

   public static void undeployTopic(String name) throws Exception
   {
      if (serverWrapper == null)
      {
         throw new Exception("The server has not been started!");
      }
      serverWrapper.deployQueue(name);
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
