/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.remoting.rmi;

import org.jboss.logging.Logger;
import org.jboss.jms.perf.framework.protocol.KillRequest;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class ExecutorKiller
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ExecutorKiller.class);

   // Static --------------------------------------------------------

   public static void main(String[] args)
   {
      // catch everything and dump it on the log, since most likely the stdout and stderr of this VM
      // are going to be discarded

      try
      {

         String name = args[0];
         int port = -1;
         String host = null;

         if (args.length > 1)
         {
            port = Integer.parseInt(args[1]);

            if (args.length > 2)
            {
               host = args[2];
            }
         }

         new ExecutorKiller(name, port, host).run();
      }
      catch(Throwable t)
      {
         // catch everything and dump it on the log, since most likely the stdout and stderr
         // of this VM are going to be discarded
         log.error("rmi executor failed to start", t);
      }
   }


   // Attributes ----------------------------------------------------

   private String name;
   private int registryPort;
   private String registryHost;
   private String url;

   // Constructors --------------------------------------------------

   public ExecutorKiller(String name,
                         int registryPort,
                         String registryHost) throws Exception
   {
      this.name = name;
      this.registryPort = registryPort;
      this.registryHost = registryHost;
   }

   // Public --------------------------------------------------------

   public void run() throws Exception
   {
      if (registryPort > 0 && registryPort < 65535)
      {
         if (registryHost != null)
         {
            url = "//" + registryHost + ":" + registryPort + "/" + name;
         }
         else
         {
            url = "//localhost:" + registryPort + "/" + name;
         }
      }

      Registry r = LocateRegistry.getRegistry(registryHost, registryPort);
      Server server = (Server)r.lookup(url);
      server.execute(new KillRequest());
      log.info("kill request sent sucessfully");
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
