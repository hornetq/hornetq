/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.p2p;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.jboss.jms.MessageImpl;
import org.jboss.jms.client.ConsumerDelegate;

/**
 * The p2p consumer
 * 
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class P2PConsumerDelegate
   implements ConsumerDelegate
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private P2PSessionDelegate session = null;
   private MessageListener messageListener = null;
   private Destination destination = null;
   boolean noLocal;
   private boolean waiting = false;
   private Message lastReceivedMessage = null;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public P2PConsumerDelegate(P2PSessionDelegate session, Destination destination, String selector, boolean noLocal)
      throws JMSException
   {
      this.session = session;
      this.destination = destination;
      this.noLocal = noLocal;
   }

   // Public --------------------------------------------------------

   // ConsumerDelegate implementation --------------------------------

	public void close() throws JMSException
	{
	}

	public void closing() throws JMSException
	{
	}

   public Message receive(long timeout) throws JMSException
   {
      Message message = this.lastReceivedMessage;
      if (message == null && timeout != -1)
      {
          this.waiting = true;
          synchronized (this)
          {
              try
              {
                  this.wait(timeout);
              }
              catch (InterruptedException exception){}
          }
          message = this.lastReceivedMessage;
          this.lastReceivedMessage = null;
          this.waiting = false;
      }
      return message;
   }

   public void setMessageListener(MessageListener listener) throws JMSException
   {
      this.messageListener = listener;
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   boolean deliver(MessageImpl message)
   {
       try
       {
           if (this.noLocal && message.isLocal())
           {
               return false;
           }
           if (message.getJMSDestination() != null)
           {
               if (message.getJMSDestination().equals(this.destination))
               {
                   if (this.messageListener != null)
                   {
                       this.messageListener.onMessage((Message)message.clone());
                       return true;
                   }
                   else
                   {
                       if (this.waiting)
                       {
                           this.lastReceivedMessage = (MessageImpl)message.clone();
                           synchronized(this)
                           {
                               this.notify();
                           }
                           return true;
                       }
                   }
               }
           }
           return false;
       }
       catch (Exception e){}
       return false;
   }

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
