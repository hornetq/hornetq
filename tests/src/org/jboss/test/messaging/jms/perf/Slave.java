/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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
