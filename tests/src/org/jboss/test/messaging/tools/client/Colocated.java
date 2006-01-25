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
package org.jboss.test.messaging.tools.client;

import org.jboss.logging.Logger;
import org.jboss.jms.server.ServerPeer;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.messaging.util.NotYetImplementedException;


/**
 * An interactive command-line JMS client that starts the server in the same VM. Allows you to
 * cut out remoting and access the server using pass-by-reference. Good for debugging without
 * connecting the debugger to two VMs. It also doesn't need a JNDI server, it used inVM
 * implementation.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Colocated extends Client
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Colocated.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private ServiceContainer sc;
   private ServerPeer serverPeer;

   // Constructors --------------------------------------------------

   public Colocated() throws Exception
   {
      sc = new ServiceContainer("transaction,remoting", null);
      sc.start();

      serverPeer = new ServerPeer("ServerPeer0", "/queue", "/topic");
      serverPeer.start();

      deployTopic("T");
      log.info("Topic messaging/topics/T deployed");
   }

   // Public --------------------------------------------------------

   public void deployTopic(String name) throws Exception
   {
      throw new NotYetImplementedException();
      //serverPeer.getDestinationManager().createTopic(name, null);
   }

   public void deployQueue(String name) throws Exception
   {
      throw new NotYetImplementedException();
      //serverPeer.getDestinationManager().createQueue(name, null);
   }

   public void exit() throws Exception
   {
      serverPeer.stop();
      sc.stop();
      System.exit(0);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
