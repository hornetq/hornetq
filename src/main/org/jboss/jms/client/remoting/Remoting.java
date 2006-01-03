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
package org.jboss.jms.client.remoting;

import org.jboss.logging.Logger;
import org.jboss.remoting.transport.Connector;

/**
 * Remoting static utilities.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Remoting
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(Remoting.class);


   // Static --------------------------------------------------------

   /**
    * TODO Get rid of this (http://jira.jboss.org/jira/browse/JBMESSAGING-92)
    * 
    * Also, if you try and create more than about 5 receivers concurrently in the same JVMs
    * it folds.
    * We need to get the UIL2 style transaport ASAP.
    * This way of doing these is also far too heavy on os resources (e.g. sockets)
    * 
    */
   public static synchronized Connector getCallbackServer() throws Exception
   {
      Connector callbackServer = new Connector();

      // use an anonymous port - 0.0.0.0 gets filled in by remoting as the current host
      String locatorURI = "socket://0.0.0.0:0";
      
      callbackServer.setInvokerLocator(locatorURI);
      
      callbackServer.start();
      
      if (log.isTraceEnabled()) log.trace("Started callback server for " +  callbackServer.getLocator());

      // TODO this is unnecessary
      callbackServer.addInvocationHandler("JMS", new ServerInvocationHandlerImpl());

      return callbackServer;
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
