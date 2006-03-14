/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.protocol;

import org.jboss.jms.perf.framework.remoting.Request;

import java.util.Properties;

/**
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version $Revision$
 *
 * $Id$
 */
public interface Job extends Request
{
   String getType();

   String getExecutorName();

   void setExecutorName(String executorName);

   String getExecutorURL();

   void setExecutorURL(String executorURL);

   int getMessageCount();

   void setMessageCount(int count);

   /**
    * In bytes.
    */
   int getMessageSize();

   /**
    * In bytes.
    */
   void setMessageSize(int messageSize);

   /**
    * In milliseconds.
    */
   long getDuration();

   /**
    * In milliseconds.
    */
   void setDuration(long duration);

   /**
    * In messages/second.
    */
   int getRate();

   /**
    * In messages/second.
    */
   void setRate(int rate);

   Properties getJNDIProperties();

   void setJNDIProperties(Properties jndiProperties);

   String getDestinationName();

   void setDestinationName(String destinationName);

   String getConnectionFactoryName();

   void setConnectionFactoryName(String connectionFactoryName);

   boolean isTransacted();

   void setTransacted(boolean b);

   int getAcknowledgmentMode();

   void setAcknowledgmentMode(int acknowledgmentMode);

   int getDeliveryMode();

   void setDeliveryMode(int deliveryMode);

}
