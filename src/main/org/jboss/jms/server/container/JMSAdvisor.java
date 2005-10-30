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
package org.jboss.jms.server.container;

import org.jboss.aop.AspectManager;
import org.jboss.aop.ClassAdvisor;
import org.jboss.jms.server.ServerPeer;

/**
 * A ClassAdvisor that keeps server peer-specific state.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSAdvisor extends ClassAdvisor
{
   // Constants -----------------------------------------------------

   // TODO relocate these constants to a different class, since they're used both on client and server-side, and not only in an AOP context
   public static final String JMS = "JMS";
   
   //These are used to locate the server side delegate instance
   //When we refactor to use JBoss AOP's remote proxy these will become unnecessary
   public static final String CONNECTION_ID = "CONNECTION_ID";
   public static final String SESSION_ID = "SESSION_ID";
   public static final String PRODUCER_ID = "PRODUCER_ID";
   public static final String CONSUMER_ID = "CONSUMER_ID";
   public static final String BROWSER_ID = "BROWSER_ID";
   
   public static final String REMOTING_SESSION_ID = "REMOTING_SESSION_ID";
   public static final String CALLBACK_HANDLER = "CALLBACK_HANDLER";
   public static final String CONNECTION_FACTORY_ID = "CONNECTION_FACTORY_ID";


   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;


   // Constructors --------------------------------------------------

   public JMSAdvisor(String classname, AspectManager manager, ServerPeer serverPeer)
   {
      super(classname, manager);
      this.serverPeer = serverPeer;
   }

   // Public --------------------------------------------------------

   
   public ServerPeer getServerPeer()
   {
      return serverPeer;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
