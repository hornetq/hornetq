/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.message;

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.messaging.core.Routable;
import org.jboss.jms.util.JBossJMSException;

import javax.jms.JMSException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import java.util.Enumeration;
import java.util.Set;
import java.io.Serializable;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JBossMessage implements javax.jms.Message, Routable
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String id;
   protected boolean reliable;
   protected Destination destination;
   protected long timestamp;
   /** GMT milliseconds at which this message expires. 0 means never expires **/
   protected long expiration;
   protected boolean redelivered;

   // Constructors --------------------------------------------------

   public JBossMessage()
   {
      id = null;
      reliable = false;
      timestamp = 0l;
      expiration = Long.MAX_VALUE;
      redelivered = false;
   }

   // javax.jmx.Message implementation ------------------------------
   
   public String getJMSMessageID() throws JMSException
   {
      return id;
   }

   public void setJMSMessageID(String id) throws JMSException
   {
      this.id = id;
   }

   public long getJMSTimestamp() throws JMSException
   {
      return timestamp;
   }

   public void setJMSTimestamp(long timestamp) throws JMSException
   {
      this.timestamp = timestamp;
   }

   public byte [] getJMSCorrelationIDAsBytes() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setJMSCorrelationID(String correlationID) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public String getJMSCorrelationID() throws JMSException
   {
      throw new NotYetImplementedException();
   }
 
   public Destination getJMSReplyTo() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setJMSReplyTo(Destination replyTo) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public Destination getJMSDestination() throws JMSException
   {
      return destination;
   }

   public void setJMSDestination(Destination destination) throws JMSException
   {
      this.destination = destination;
   }
 
   public int getJMSDeliveryMode() throws JMSException
   {
      return reliable ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
   }
 
   public void setJMSDeliveryMode(int deliveryMode) throws JMSException
   {
      if (deliveryMode == DeliveryMode.PERSISTENT)
      {
         reliable = true;
      }
      else if (deliveryMode == DeliveryMode.NON_PERSISTENT)
      {
         reliable = false;
      }
      else
      {
         throw new JBossJMSException("Delivery mode must be either DeliveryMode.PERSISTENT " +
                                     "or DeliveryMode.NON_PERSISTENT");
      }
   }
   
   public boolean getJMSRedelivered() throws JMSException
   {
      return isRedelivered();
   }
 
   public void setJMSRedelivered(boolean redelivered) throws JMSException
   {
      setRedelivered(redelivered);
   }
       
   public String getJMSType() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setJMSType(String type) throws JMSException
   {
      throw new NotYetImplementedException();
   }
 
   public long getJMSExpiration() throws JMSException
   {
      return expiration;
   }
 
   public void setJMSExpiration(long expiration) throws JMSException
   {
      this.expiration = expiration;
   }

   public int getJMSPriority() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setJMSPriority(int priority) throws JMSException
   {
      throw new NotYetImplementedException();
   }
   
   public void clearProperties() throws JMSException
   {
      throw new NotYetImplementedException();
   }
   
   public boolean propertyExists(String name) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public boolean getBooleanProperty(String name) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public byte getByteProperty(String name) throws JMSException
   {
      throw new NotYetImplementedException();
   }
   
   public short getShortProperty(String name) throws JMSException
   {
      throw new NotYetImplementedException();
   }
 
   public int getIntProperty(String name) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public long getLongProperty(String name) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public float getFloatProperty(String name) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public double getDoubleProperty(String name) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public String getStringProperty(String name) throws JMSException
   {
      throw new NotYetImplementedException();
   }
   
   public Object getObjectProperty(String name) throws JMSException
   {
      throw new NotYetImplementedException();
   }
     
   public Enumeration getPropertyNames() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setBooleanProperty(String name, boolean value) throws JMSException
   {
      throw new NotYetImplementedException();
   }
   
   public void setByteProperty(String name, byte value) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setShortProperty(String name, short value) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setIntProperty(String name, int value) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setLongProperty(String name, long value) throws JMSException
   {
      throw new NotYetImplementedException();
   }
   
   public void setFloatProperty(String name, float value) throws JMSException
   {
      throw new NotYetImplementedException();
   }
   
   public void setDoubleProperty(String name, double value) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setStringProperty(String name, String value) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setObjectProperty(String name, Object value) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void acknowledge() throws JMSException
   {
      throw new NotYetImplementedException();
   }
   
   public void clearBody() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   // org.jboss.messaging.core.Routable implementation --------------

   public Serializable getMessageID()
   {
      return id;
   }

   public boolean isReliable()
   {
      return reliable;
   }

   public long getExpirationTime()
   {
      return expiration;
   }

   public boolean isRedelivered()
   {
      return redelivered;
   }

   public void setRedelivered(boolean b)
   {
      redelivered = b;
   }

   public Serializable putHeader(String name, Serializable value)
   {
      throw new NotYetImplementedException();
   }

   public Serializable getHeader(String name)
   {
      throw new NotYetImplementedException();
   }

   public Serializable removeHeader(String name)
   {
      throw new NotYetImplementedException();
   }

   public boolean containsHeader(String name)
   {
      throw new NotYetImplementedException();
   }

   public Set getHeaderNames()
   {
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
