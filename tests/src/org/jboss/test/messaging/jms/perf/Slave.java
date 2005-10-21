/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import org.jboss.logging.Logger;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.Connector;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class Slave
{
   private static final Logger log = Logger.getLogger(Slave.class);
   
   protected Connector connector;
   
   public static void main(String[] args)
   {
      log.info("Slave starting");
      
      int port;
      try
      {
         port = Integer.parseInt(args[0]);
      }
      catch (Exception e)
      {
         log.error("Invalid port or no port specified");
         return;
      }
      
      new Slave().run(port);
   }
   
   private Slave()
   {   
   }
   
   private void run(int port)
   {
      try
      {
         connector = new Connector();
         InvokerLocator locator = new InvokerLocator("socket://localhost:" + port);
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
