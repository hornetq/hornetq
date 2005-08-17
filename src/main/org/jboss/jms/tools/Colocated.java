/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.tools;

import org.jboss.logging.Logger;
import org.jboss.messaging.tools.jmx.ServiceContainer;
import org.jboss.jms.server.ServerPeer;


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

      serverPeer = new ServerPeer("ServerPeer0");
      serverPeer.start();

      deployTopic("T");
      log.info("Topic messaging/topics/T deployed");
   }

   // Public --------------------------------------------------------

   public void deployTopic(String name) throws Exception
   {
      serverPeer.getDestinationManager().createTopic(name, null);
   }

   public void deployQueue(String name) throws Exception
   {
      serverPeer.getDestinationManager().createQueue(name, null);
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
