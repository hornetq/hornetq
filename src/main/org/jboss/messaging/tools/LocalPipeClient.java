/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tools;

import org.jboss.messaging.core.LocalPipe;
import org.jboss.messaging.core.MessageSupport;
import org.jboss.messaging.interfaces.Routable;

/**
 * Class that provides a command line interface to a LocalPipe. Run it with Clester.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class LocalPipeClient
{

   // Attributes ----------------------------------------------------

   private LocalPipe pipe;
   private int counter = 0;
   private ReceiverManager receiverManager;

   // Constructors --------------------------------------------------

   public LocalPipeClient()
   {
      pipe = new LocalPipe("LocalPipeID");
      receiverManager = new ReceiverManager();
   }

   // Public --------------------------------------------------------

   public void send()
   {
       Routable m = new MessageSupport(new Integer(counter++));
       System.out.println("Sending "+m+" to the pipe: "+pipe.handle(m));

   }

   public void deliver() {
      pipe.deliver();
   }

   public void setReceiver(String name)
   {
      pipe.setReceiver(receiverManager.getReceiver(name));
   }

   public String dumpReceivers()
   {
      return receiverManager.dump();
   }

   public void exit() {
      System.exit(0);
   }
}
