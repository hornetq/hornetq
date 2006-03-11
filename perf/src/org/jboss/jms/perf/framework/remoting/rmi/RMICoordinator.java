/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.remoting.rmi;

import org.jboss.jms.perf.framework.remoting.Coordinator;
import org.jboss.jms.perf.framework.remoting.Result;
import org.jboss.jms.perf.framework.remoting.Request;
import org.jboss.logging.Logger;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class RMICoordinator implements Coordinator
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RMICoordinator.class);

   // Static --------------------------------------------------------

   public static boolean isValidURL(String url)
   {
      try
      {
         new RMIURL(url);
         return true;
      }
      catch(Exception e)
      {
         return false;
      }
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Coordinator implementation ------------------------------------

   public Result sendToExecutor(String executorURL, Request request) throws Throwable
   {
      RMIURL url = new RMIURL(executorURL);

      log.debug("sending request to the rmi server");

      Registry r = LocateRegistry.getRegistry(url.getHost(), url.getPort());
      Server server = (Server)r.lookup(url.getURL());
      Result result = server.execute(request);

      log.debug("received response from the rmi server");

      return result;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

