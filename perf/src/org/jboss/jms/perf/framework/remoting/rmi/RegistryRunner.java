/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.remoting.rmi;

import org.jboss.logging.Logger;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class RegistryRunner extends UnicastRemoteObject implements RegistryManagement
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RegistryRunner.class);

   // Static --------------------------------------------------------

   public static Registry registry;

   public static void main(String[] args)
   {
      try
      {
         int port = 7777;

         System.setProperty("java.rmi.server.hostname", "localhost");

         registry = LocateRegistry.createRegistry(port);

         log.info("registry created");

         registry.bind("//localhost:7777/management", new RegistryRunner());

         log.info("management instance bound");
      }
      catch(Throwable t)
      {
         log.error("registry failed", t);
      }

   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public RegistryRunner() throws Exception
   {
   }

   // RegistryManagement interface ----------------------------------

   public void kill() throws Exception
   {
      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               Thread.sleep(500);
               log.debug("just about to exit");
               System.exit(0);
            }
            catch(Exception e)
            {
            }
         }
      }).run();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
