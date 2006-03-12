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
import org.jboss.jms.perf.framework.remoting.Executor;
import org.jboss.jms.perf.framework.remoting.Context;

import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.AlreadyBoundException;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class RMIExecutor extends UnicastRemoteObject implements Server, Context
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

         RMIExecutor executor = new RMIExecutor(name, port, host);
         executor.setColocated(false);
         executor.start();
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

   private Executor delegateExecutor;

   private boolean colocated;

   // Constructors --------------------------------------------------

   public RMIExecutor(String name,
                      int registryPort,
                      String registryHost) throws Exception
   {
      this.name = name;
      this.registryPort = registryPort;
      this.registryHost = registryHost;
      registry = null;
      delegateExecutor = new Executor(this);
      colocated = true;
   }

   // Server implemenation ------------------------------------------

   public Result execute(Request request) throws Exception
   {
      return delegateExecutor.execute(request);
   }

   // Context implementation ----------------------------------------

   public boolean isColocated()
   {
      return colocated;
   }

   // Public --------------------------------------------------------

   // JMX managed attributes ----------------------------------------

   public int getRegistryPort()
   {
      return registryPort;
   }

   public String getRegistryHost()
   {
      return registryHost;
   }

   public String getName()
   {
      return name;
   }

   // JMX managed operations ----------------------------------------

   public void start() throws Exception
   {
      if (registryPort > 0 && registryPort < 65535)
      {
         if (registryHost != null)
         {
            url = "//" + registryHost + ":" + registryPort + "/" + name;
            System.setProperty("java.rmi.server.hostname", registryHost);
         }
         else
         {
            url = "//localhost:" + registryPort + "/" + name;
         }
      }
      else
      {
         throw new Exception("registry port name needed");
      }

      registry = LocateRegistry.getRegistry(registryHost, registryPort);
      try
      {
         registry.bind(url, this);
         log.info(this + " started");
      }
      catch(AlreadyBoundException e)
      {
         log.warn(this + " already started, try to stop it first!");
      }
   }

   public void stop() throws Exception
   {
      log.debug("Unbinding " + url + " from " + registry);
      registry.unbind(url);
      log.info(this + " stopped");
   }

   public String toString()
   {
      return "RMIExecutor[" + url + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void setColocated(boolean colocated)
   {
      this.colocated = colocated;
   }

   // Inner classes -------------------------------------------------
}
