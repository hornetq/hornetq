/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;

import org.jboss.logging.Logger;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.server.ServerPeer;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Destination;
import javax.jms.Message;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerProducerDelegate implements ProducerDelegate
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerProducerDelegate.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String id;
   //protected Receiver destination;
   /** I need this to set up the JMSDestination header on outgoing messages */
   protected Destination jmsDestination;
   protected ServerSessionDelegate sessionEndpoint;
   
   protected boolean closed;

   // Constructors --------------------------------------------------

   public ServerProducerDelegate(String id,
                                 Destination jmsDestination,
                                 ServerSessionDelegate parent)
   {
      this.id = id;
      //this.destination = destination;
      this.jmsDestination = jmsDestination;
      sessionEndpoint = parent;
   }

   // ProducerDelegate implementation ------------------------

   public void closing() throws JMSException
   {
      //Currently this does nothing
      if (log.isTraceEnabled()) { log.trace("closing (noop)"); }
   }

   public void close() throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Producer is already closed");
      }
      
      //Currently this does nothing
      if (log.isTraceEnabled()) { log.trace("close (noop)"); }
      this.sessionEndpoint.producers.remove(this.id);
      
      closed = true;
   }
   
   public void send(Destination destination, Message m, int deliveryMode,
                    int priority, long timeToLive) throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Producer is closed");
      }
      
      if (log.isTraceEnabled()) { log.trace("Sending message: " + m); }
      
      sessionEndpoint.connectionEndpoint.sendMessage(m, null);
   }

   public Object getMetaData(Object attr)
   {
      // TODO - See "Delegate Implementation" thread
      // TODO   http://www.jboss.org/index.html?module=bb&op=viewtopic&t=64747

      // NOOP
      log.warn("getMetaData(): NOT handled on the server-side");
      return null;
   }

   public void addMetaData(Object attr, Object metaDataValue)
   {
      // TODO - See "Delegate Implementation" thread
      // TODO   http://www.jboss.org/index.html?module=bb&op=viewtopic&t=64747

      // NOOP
      log.warn("addMetaData(): NOT handled on the server-side");
   }

   public Object removeMetaData(Object attr)
   {
      // TODO - See "Delegate Implementation" thread
      // TODO   http://www.jboss.org/index.html?module=bb&op=viewtopic&t=64747

      // NOOP
      log.warn("removeMetaData(): NOT handled on the server-side");
      return null;
   }
   
   public void setDisableMessageID(boolean value) throws JMSException
   {
      log.warn("setDisableMessageID(): NOT handled on the server-side");
   }
   
   public boolean getDisableMessageID() throws JMSException
   {
      log.warn("getDisableMessageID(): NOT handled on the server-side");
      return false;
   }
   
   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
      log.warn("setDisableMessageTimestamp(): NOT handled on the server-side");
   }
   
   public boolean getDisableMessageTimestamp() throws JMSException
   {
      log.warn("getDisableMessageTimestamp(): NOT handled on the server-side");
      return false;
   }
   
   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
      log.warn("setDeliveryMode(): NOT handled on the server-side");
   }
   
   public int getDeliveryMode() throws JMSException
   {
      log.warn("getDeliveryMode(): NOT handled on the server-side");
      return -1;
   }
   
   public void setPriority(int defaultPriority) throws JMSException
   {
      log.warn("setPriority(): NOT handled on the server-side");   
   }
   
   public int getPriority() throws JMSException
   {
      log.warn("getPriority(): NOT handled on the server-side"); 
      return -1;
   }
   
   public void setTimeToLive(long timeToLive) throws JMSException
   {
      log.warn("setTimeToLive(): NOT handled on the server-side");
   }
   
   public long getTimeToLive() throws JMSException
   {
      log.warn("getTimeToLive(): NOT handled on the server-side");
      return -1;
   }
   
   public Destination getDestination() throws JMSException
   {
      log.warn("getDestination(): NOT handled on the server-side");
      return null;
   }
   
   public void setDestination(Destination d)
   {
      log.warn("setDestination(): NOT handled on the server-side");
   }

   // Public --------------------------------------------------------

   public ServerSessionDelegate getSessionEndpoint()
   {
      return sessionEndpoint;
   }

   public ServerPeer getServerPeer()
   {
      return sessionEndpoint.getConnectionEndpoint().getServerPeer();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
