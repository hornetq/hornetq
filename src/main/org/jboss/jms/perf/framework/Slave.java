/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

import org.jboss.logging.Logger;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.Connector;

/**
 * 
 * A Slave.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class Slave
{
   private static final Logger log = Logger.getLogger(Slave.class);
   
   protected Connector connector;
   
   public static void main(String[] args)
   {
      log.info("Slave starting");
      
      int port;
      String ip = null;
      try
      {
         port = Integer.parseInt(args[0]);
         if (args.length == 2)
         {
            ip = args[1];
         }
      }
      catch (Exception e)
      {
         log.error("Invalid port or no port specified");
         return;
      }
      
      new Slave().run(port, ip);
   }
   
   private Slave()
   {   
   }
   
   private void run(int port, String ip)
   {
      try
      {
         connector = new Connector();
         InvokerLocator locator;
         if (ip == null)
         {
            locator =  new InvokerLocator("socket://0.0.0.0:" + port + "/?socketTimeout=0");
         }
         else
         {
            locator =  new InvokerLocator("socket://" + ip + ":" + port + "/?socketTimeout=0");
         }
         connector.setInvokerLocator(locator.getLocatorURI());
         log.info("Invoker locator: " + locator.getLocatorURI());
         
         connector.create();

         PerfInvocationHandler invocationHandler = new PerfInvocationHandler();

         connector.addInvocationHandler("perftest", invocationHandler);

         log.info("Server running");
         
         connector.start();
      }
      catch (Exception e)
      {
         log.error("Failure in running server", e);
      }  
   }
   

}
