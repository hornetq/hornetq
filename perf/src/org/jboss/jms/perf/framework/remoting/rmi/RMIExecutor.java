/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.remoting.rmi;

import org.jboss.logging.Logger;
import org.jboss.jms.perf.framework.remoting.Result;
import org.jboss.jms.perf.framework.remoting.Request;

import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class RMIExecutor extends UnicastRemoteObject implements Server
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RMIExecutor.class);

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

         new RMIExecutor(name, port, host).start();
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

   private Registry registry;

   // Constructors --------------------------------------------------

   public RMIExecutor(String name,
                      int registryPort,
                      String registryHost) throws Exception
   {
      this.name = name;
      this.registryPort = registryPort;
      this.registryHost = registryHost;
      registry = null;
   }

   // Server implemenation ------------------------------------------

   public Result execute(Request request) throws Exception
   {
      log.debug("receiving request for execution: " + request);
      Result result = request.execute();
      log.debug("request executed successfully");
      return result;
   }

   // Public --------------------------------------------------------

   public void start() throws Exception
   {
      if (registryPort > 0 && registryPort < 65535)
      {
         if (registryHost != null)
         {
            url = "//" + registryHost + ":" + registryPort + "/" + name;
            System.setProperty("java.rmi.server.hostname", registryHost);
            registry = LocateRegistry.createRegistry(registryPort);
            log.debug("registry(1) : " + registry + ", url: " + url);
         }
         else
         {
            url = "//localhost:" + registryPort + "/" + name;
            registry = LocateRegistry.createRegistry(registryPort);
            log.debug("registry(2) : " + registry + ", url: " + url);
         }
      }
      else
      {
         throw new Exception("registry port name needed");
      }

      registry.bind(url, this);
      log.info(this + " started");
   }

   public void stop() throws Exception
   {
      registry.unbind(name);
      log.info(this + " stopped");
   }

   public String toString()
   {
      return "RMIExecutor[" + url + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
