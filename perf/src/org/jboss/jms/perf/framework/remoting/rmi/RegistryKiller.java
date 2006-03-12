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


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class RegistryKiller
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RegistryKiller.class);

   // Static --------------------------------------------------------

   public static void main(String[] args)
   {
      try
      {
         Registry registry = LocateRegistry.getRegistry("localhost", 7777);

         log.info("registry: " + registry);

         RegistryManagement m = (RegistryManagement)registry.lookup("//localhost:7777/management");

         m.kill();

         log.info("registry killed");
      }
      catch(Throwable t)
      {
         log.error("exception", t);
      }
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // RegistryManagement interface ----------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
