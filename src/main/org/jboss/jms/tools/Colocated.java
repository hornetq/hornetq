/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.tools;

import org.jboss.jms.util.InVMInitialContextFactory;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.util.transaction.TransactionManagerImpl;


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

   private ServerWrapper server;

   // Constructors --------------------------------------------------

   public Colocated() throws Exception
   {
      super(InVMInitialContextFactory.getJNDIEnvironment());
      server = new ServerWrapper(InVMInitialContextFactory.getJNDIEnvironment(),
                                 TransactionManagerImpl.getInstance());
      server.start();

      deployTopic("T");
      log.info("Topic messaging/topics/T deployed");
   }

   // Public --------------------------------------------------------

   public void deployTopic(String name) throws Exception
   {
      server.deployTopic(name);
   }

   public void deployQueue(String name) throws Exception
   {
      server.deployQueue(name);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
