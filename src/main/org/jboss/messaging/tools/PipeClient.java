/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tools;

import org.jboss.messaging.core.Pipe;
import org.jboss.messaging.interfaces.Message;

/**
 * Class that provides a command line interface to a Pipe. Run it with Clester.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class PipeClient
{

   // Attributes ----------------------------------------------------

   private Pipe pipe;
   private int counter = 0;
   private ReceiverManager receiverManager;

   // Constructors --------------------------------------------------

   public PipeClient()
   {
      pipe = new Pipe();
      receiverManager = new ReceiverManager();
   }

   // Public --------------------------------------------------------

   public void send()
   {
       Message m = new MessageImpl(new Integer(counter++));
       System.out.println("Sending "+m+" to the pipe: "+pipe.handle(m));

   }

   public void deliver() {
      pipe.deliver();
   }

   public void setReceiver(String name)
   {
      pipe.setReceiver(receiverManager.getReceiver(name));
   }

   public String dump()
   {
      return ((Pipe)pipe).dump();
   }

   public String dumpReceivers()
   {
      return receiverManager.dump();
   }

   public void exit() {
      System.exit(0);
   }
}
