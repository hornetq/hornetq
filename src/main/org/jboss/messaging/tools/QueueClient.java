/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tools;

import org.jboss.messaging.core.MessageSupport;
import org.jboss.messaging.core.distributed.Queue;
import org.jboss.messaging.interfaces.Routable;
import org.jboss.messaging.util.RpcServer;

/**
 * Class that provides a command line interface to build distributed Queues. Run it with Clester.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class QueueClient extends RpcDispatcherClient
{

   // Attributes ----------------------------------------------------
   
   private Queue queuePeer;
   private int counter = 0;
   private int receiverCounter = 0;

   // Constructors --------------------------------------------------

   public QueueClient() throws Exception
   {
      super(new RpcServer());
      queuePeer = new Queue(rpcDispatcher, "DistributedQueueID");
   }

   // Public --------------------------------------------------------


   public void connect() throws Exception
   {
      queuePeer.connect();
   }

   public void send()
   {
       Routable m = new MessageSupport(new Integer(counter++));
       System.out.println("Sending "+m+" to the pipe input: "+queuePeer.handle(m));
   }

   public void addReceiver()
   {
      queuePeer.add(new ReceiverImpl("Receiver " + receiverCounter++));
   }

   public void exit() {
      System.exit(0);
   }
}
