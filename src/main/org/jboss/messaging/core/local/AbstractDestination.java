/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.messaging.core.Distributor;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.AcknowledgmentStore;
import org.jboss.messaging.core.Receiver;

import java.util.Iterator;
import java.util.Set;
import java.io.Serializable;

/**
 * A Channel with a routing policy in place. It delegates the routing policy to a Router instance.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class AbstractDestination extends ChannelSupport implements Distributor
{
   // Attributes ----------------------------------------------------

   protected LocalPipe inputPipe;
   protected AbstractRouter router;

   // Constructors --------------------------------------------------


   /**
    * By default, a Destination is an asynchronous channel.
    * @param id
    */
   protected AbstractDestination(Serializable id)
   {
      this(id, Channel.ASYNCHRONOUS);
   }

   protected AbstractDestination(Serializable id, boolean mode)
   {
      router = createRouter();
      inputPipe = new LocalPipe(id, mode, router);
   }

   // Channel implementation ----------------------------------------

   public Serializable getReceiverID()
   {
      return inputPipe.getReceiverID();
   }

   public boolean handle(Routable m)
   {
      return inputPipe.handle(m);
   }

   public boolean hasMessages()
   {
      return inputPipe.hasMessages();
   }

   public Set getUnacknowledged()
   {
      return inputPipe.getUnacknowledged();
   }

   public boolean setSynchronous(boolean b)
   {
      return inputPipe.setSynchronous(b);
   }

   public boolean isSynchronous()
   {
      return inputPipe.isSynchronous();
   }

   public void setMessageStore(MessageStore store)
   {
      // the Queue and the LocalPipe share the message store
      super.setMessageStore(store);
      inputPipe.setMessageStore(store);
   }

   public MessageStore getMessageStore()
   {
      return super.getMessageStore();
   }

   public void setAcknowledgmentStore(AcknowledgmentStore store)
   {
      // the Queue and the LocalPipe share the acknowledgment store
      super.setAcknowledgmentStore(store);
      inputPipe.setAcknowledgmentStore(store);
   }

   public AcknowledgmentStore getAcknowledgmentStore()
   {
      return super.getAcknowledgmentStore();
   }


   /**
    * Override if you want a more sophisticated delivery mechanism.
    */
   public boolean deliver()
   {
      return inputPipe.deliver();
   }

   // Distributor interface -----------------------------------------

   public boolean add(Receiver r)
   {
      if (!router.add(r))
      {
         return false;
      }

      // adding a Receiver triggers an asynchronous delivery attempt
      if (inputPipe.hasMessages())
      {
         inputPipe.deliver();
      }
      return true;
   }

   public Receiver get(Serializable receiverID)
   {
      return router.get(receiverID);
   }

   public Receiver remove(Serializable receiverID)
   {
      return router.remove(receiverID);
   }

   public boolean contains(Serializable receiverID)
   {
      return router.contains(receiverID);
   }

   public Iterator iterator()
   {
      return router.iterator();
   }

   public void clear()
   {
      router.clear();
   }

   public boolean acknowledged(Serializable receiverID)
   {
      return router.acknowledged(receiverID);
   }

   // Public --------------------------------------------------------

   // Protected -----------------------------------------------------

   protected abstract AbstractRouter createRouter();

   protected void storeNACKedMessageLocally(Routable r, Serializable receiverID)
   {
      throw new NotYetImplementedException();
   }

}




