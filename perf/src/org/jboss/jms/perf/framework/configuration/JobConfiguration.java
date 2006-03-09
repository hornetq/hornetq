/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.configuration;

import org.w3c.dom.Node;
import org.jboss.jms.util.XMLUtil;
import org.jboss.jms.perf.framework.Job;

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
   private static final String EXECUTOR_URL = "executor-url";
   private static final String MESSAGES = "messages";
   private static final String MESSAGE_SIZE = "message-size";
   private static final String DURATION = "duration";
   private static final String RATE = "rate";

   // Static --------------------------------------------------------

   public static boolean isValidElementName(String name)
   {
      if (DESTINATION.equals(name) ||
          CONNECTION_FACTORY.equals(name) ||
          EXECUTOR_URL.equals(name) ||
          MESSAGES.equals(name) ||
          MESSAGE_SIZE.equals(name) ||
          DURATION.equals(name) ||
          RATE.equals(name))
      {
         return true;

      }
      return false;
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
   private String executorURL;
   private Integer messageCount;
   private Integer messageSize;
   private Long duration;
   private Integer rate;

   // Constructors --------------------------------------------------

   public JobConfiguration()
   {
      destinationName = null;
      connectionFactoryName = null;
      executorURL = null;
      messageCount = null;
      messageSize = null;
      duration = null;
      rate = null;
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
   public String getExecutorURL()
   {
      return executorURL;
   }

   public void setExecutorURL(String executorURL)
   {
      this.executorURL = executorURL;
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


   public JobConfiguration copy()
   {
      JobConfiguration n = new JobConfiguration();
      n.destinationName = this.destinationName;
      n.connectionFactoryName = this.connectionFactoryName;
      n.executorURL = this.executorURL;
      n.messageCount = this.messageCount;
      n.messageSize = this.messageSize;
      n.duration = this.duration;
      n.rate = this.rate;

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
      else if (EXECUTOR_URL.equals(name))
      {
         setExecutorURL(value);
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
      s = getExecutorURL();
      if (s != null)
      {
         j.setExecutorURL(s);
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
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
