/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.configuration;

import org.w3c.dom.Node;
import org.jboss.jms.util.XMLUtil;
import org.jboss.jms.perf.framework.protocol.Job;

import javax.jms.Session;
import javax.jms.DeliveryMode;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class JobConfiguration
{
   // Constants -----------------------------------------------------

   private static final String DESTINATION = "destination";
   private static final String CONNECTION_FACTORY = "connection-factory";
   private static final String EXECUTOR = "executor";
   private static final String MESSAGES = "messages";
   private static final String MESSAGE_SIZE = "message-size";
   private static final String DURATION = "duration";
   private static final String RATE = "rate";
   private static final String ACKNOWLEDGMENT_MODE = "acknowledgment-mode";
   private static final String DELIVERY_MODE = "delivery-mode";


   // Static --------------------------------------------------------

   public static boolean isValidJobConfigurationElementName(String name)
   {
      if (DESTINATION.equals(name) ||
          CONNECTION_FACTORY.equals(name) ||
          EXECUTOR.equals(name) ||
          MESSAGES.equals(name) ||
          MESSAGE_SIZE.equals(name) ||
          DURATION.equals(name) ||
          RATE.equals(name) ||
          ACKNOWLEDGMENT_MODE.equals(name) ||
          DELIVERY_MODE.equals(name))
      {
         return true;
      }
      return false;
   }

   public static boolean isValidJobName(String name)
   {
      if ("send".equals(name) ||
          "receive".equals(name) ||
          "drain".equals(name) ||
          "fill".equals(name) ||
          "ping".equals(name))
      {
         return true;
      }
      return false;
   }

   public static void validateAcknowledgmentMode(int acknowledgmentMode)
   {
      if (acknowledgmentMode != Session.AUTO_ACKNOWLEDGE &&
          acknowledgmentMode != Session.CLIENT_ACKNOWLEDGE &&
          acknowledgmentMode != Session.DUPS_OK_ACKNOWLEDGE &&
          acknowledgmentMode != Session.SESSION_TRANSACTED)
      {
         throw new IllegalArgumentException("invalid acknowledgment mode: " + acknowledgmentMode);
      }
   }

   public static void validateDeliveryMode(int deliveryMode)
   {
      if (deliveryMode != DeliveryMode.NON_PERSISTENT &&
          deliveryMode != DeliveryMode.PERSISTENT)
      {
         throw new IllegalArgumentException("invalid delivery mode: " + deliveryMode);
      }
   }

   public static int validateAcknowledgmentMode(String acknowledgmentMode)
   {
      if (acknowledgmentMode == null)
      {
         throw new IllegalArgumentException("null acknowledgment mode");
      }

      String s = acknowledgmentMode.toUpperCase();

      if ("AUTO_ACKNOWLEDGE".equals(s))
      {
         return Session.AUTO_ACKNOWLEDGE;
      }
      else if ("CLIENT_ACKNOWLEDGE".equals(s))
      {
         return Session.CLIENT_ACKNOWLEDGE;
      }
      else if ("DUPS_OK_ACKNOWLEDGE".equals(s))
      {
         return Session.DUPS_OK_ACKNOWLEDGE;
      }
      else if ("SESSION_TRANSACTED".equals(s))
      {
         return Session.SESSION_TRANSACTED;
      }
      throw new IllegalArgumentException("invalid acknowledgment mode: " + acknowledgmentMode);
   }

   public static int validateDeliveryMode(String deliveryMode)
   {
      if (deliveryMode == null)
      {
         throw new IllegalArgumentException("null delivery mode");
      }

      String s = deliveryMode.toUpperCase();

      if ("NON_PERSISTENT".equals(s))
      {
         return DeliveryMode.NON_PERSISTENT;
      }
      else if ("PERSISTENT".equals(s))
      {
         return DeliveryMode.PERSISTENT;
      }
      throw new IllegalArgumentException("invalid delivery mode: " + deliveryMode);
   }

   public static String executionURLToString(String executionURL)
   {
      if (executionURL == null)
      {
         return "null";
      }

      int i = executionURL.indexOf("://");
      return executionURL.substring(i + 3);
   }

   // Attributes ----------------------------------------------------

   private String destinationName;
   private String connectionFactoryName;
   private String executor;
   private Integer messageCount;
   private Integer messageSize;
   private Long duration;
   private Integer rate;
   private Integer acknowledgmentMode;
   private Integer deliveryMode;

   // Constructors --------------------------------------------------

   public JobConfiguration()
   {
      destinationName = null;
      connectionFactoryName = null;
      executor = null;
      messageCount = null;
      messageSize = null;
      duration = null;
      rate = null;
      acknowledgmentMode = null;
      deliveryMode = null;
   }

   // Public --------------------------------------------------------

   /**
    * null means no default
    */
   public String getDestinationName()
   {
      return destinationName;
   }

   public void setDestinationName(String destinationName)
   {
      this.destinationName = destinationName;
   }

   /**
    * null means no default
    */
   public String getConnectionFactoryName()
   {
      return connectionFactoryName;
   }

   public void setConnectionFactoryName(String connectionFactoryName)
   {
      this.connectionFactoryName = connectionFactoryName;
   }

   /**
    * null means no default
    */
   public String getExecutor()
   {
      return executor;
   }

   public void setExecutor(String executor)
   {
      this.executor = executor;
   }

   /**
    * null means no default
    */
   public Integer getMessageCount()
   {
      return messageCount;
   }

   public void setMessageCount(int messageCount)
   {
       this.messageCount = new Integer(messageCount);
   }

   /**
    * null means no default
    */
   public Integer getMessageSize()
   {
      return messageSize;
   }

   public void setMessageSize(int messageSize)
   {
      this.messageSize = new Integer(messageSize);
   }

   /**
    * null means no default
    */
   public Long getDuration()
   {
      return duration;
   }

   public void setDuration(long duration)
   {
       this.duration= new Long(duration);
   }

   /**
    * null means no default
    */
   public Integer getRate()
   {
      return rate;
   }

   public void setRate(int rate)
   {
      this.rate = new Integer(rate);
   }

   /**
    * null means no default
    */
   public Integer getAcknowledgmentMode()
   {
      return acknowledgmentMode;
   }

   public void setAcknowledgmentMode(int acknowledgmentMode)
   {
      validateAcknowledgmentMode(acknowledgmentMode);
      this.acknowledgmentMode = new Integer(acknowledgmentMode);
   }

   /**
    * null means no default
    */
   public Integer getDeliveryMode()
   {
      return deliveryMode;
   }

   public void setDeliveryMode(int deliveryMode)
   {
      validateDeliveryMode(deliveryMode);
      this.deliveryMode = new Integer(deliveryMode);
   }

   public JobConfiguration copy()
   {
      JobConfiguration n = new JobConfiguration();
      n.destinationName = this.destinationName;
      n.connectionFactoryName = this.connectionFactoryName;
      n.executor = this.executor;
      n.messageCount = this.messageCount;
      n.messageSize = this.messageSize;
      n.duration = this.duration;
      n.rate = this.rate;
      n.acknowledgmentMode = this.acknowledgmentMode;
      n.deliveryMode = this.deliveryMode;

      return n;
   }

   public void add(Node n) throws Exception
   {
      String name = n.getNodeName();
      String value = XMLUtil.getTextContent(n);

      if (value == null)
      {
         // one more try
         value = n.getNodeValue();
      }

      if (DESTINATION.equals(name))
      {
         setDestinationName(value);
      }
      else if (CONNECTION_FACTORY.equals(name))
      {
         setConnectionFactoryName(value);
      }
      else if (EXECUTOR.equals(name))
      {
         setExecutor(value);
      }
      else if (MESSAGES.equals(name))
      {
         int i = Integer.parseInt(value);
         setMessageCount(i);
      }
      else if (MESSAGE_SIZE.equals(name))
      {
         int i = Integer.parseInt(value);
         setMessageSize(i);
      }
      else if (DURATION.equals(name))
      {
         long l = Long.parseLong(value);
         setDuration(l);
      }
      else if (RATE.equals(name))
      {
         int i = Integer.parseInt(value);
         setRate(i);
      }
      else if (ACKNOWLEDGMENT_MODE.equals(name))
      {
         int i = validateAcknowledgmentMode(value);
         setAcknowledgmentMode(i);
      }
      else if (DELIVERY_MODE.equals(name))
      {
         int i = validateDeliveryMode(value);
         setDeliveryMode(i);
      }
      else
      {
         throw new Exception("Unknown node " + name);
      }
   }

   public void configure(Job j)
   {
      String s;
      Integer i;
      Long l;

      s = getDestinationName();
      if (s != null)
      {
         j.setDestinationName(s);
      }
      s = getConnectionFactoryName();
      if (s != null)
      {
         j.setConnectionFactoryName(s);
      }
      s = getExecutor();
      if (s != null)
      {
         j.setExecutorName(s);
      }
      i = getMessageCount();
      if (i != null)
      {
         j.setMessageCount(i.intValue());
      }
      i = getMessageSize();
      if (i != null)
      {
         j.setMessageSize(i.intValue());
      }
      l = getDuration();
      if (l != null)
      {
         j.setDuration(l.longValue());
      }
      i = getRate();
      if (i != null)
      {
         j.setRate(i.intValue());
      }
      i = getAcknowledgmentMode();
      if (i != null)
      {
         j.setAcknowledgmentMode(i.intValue());
      }
      i = getDeliveryMode();
      if (i != null)
      {
         j.setDeliveryMode(i.intValue());
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
