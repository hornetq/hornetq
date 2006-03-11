/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.protocol;

import java.io.Serializable;
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Session;
import javax.naming.InitialContext;

import org.jboss.logging.Logger;
import org.jboss.jms.perf.framework.configuration.JobConfiguration;
import org.jboss.jms.perf.framework.remoting.Result;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version $Revision$
 *
 * $Id$
 */
public abstract class JobSupport implements Job, Serializable
{
   // Constants -----------------------------------------------------

   private transient static final Logger log = Logger.getLogger(JobSupport.class);

   // Static --------------------------------------------------------

   public static Job create(String type)
   {
      if (SendJob.TYPE.equals(type))
      {
         return new SendJob();
      }
      else if (ReceiveJob.TYPE.equals(type))
      {
         return new ReceiveJob();
      }
      else if (FillJob.TYPE.equals(type))
      {
         return new FillJob();
      }
      else if (DrainJob.TYPE.equals(type))
      {
         return new DrainJob();
      }
      else
      {
         throw new IllegalArgumentException("Unknown job type " + type);
      }
   }

   public static String acknowledgmentModeToString(int ackMode)
   {
      if (Session.AUTO_ACKNOWLEDGE == ackMode)
      {
         return "AUTO_ACKNOWLEDGE";
      }
      else if (Session.CLIENT_ACKNOWLEDGE == ackMode)
      {
         return "CLIENT_ACKNOWLEDGE";
      }
      else if (Session.DUPS_OK_ACKNOWLEDGE == ackMode)
      {
         return "DUPS_OK_ACKNOWLEDGE";
      }
      else if (Session.SESSION_TRANSACTED == ackMode)
      {
         return "SESSION_TRANSACTED";
      }
      else
      {
         return "UNKNOWN";
      }
   }

   // Attributes ----------------------------------------------------

   protected String executorURL;
   protected int messageCount;
   protected int messageSize;
   protected long duration;
   protected int rate;

   protected Properties jndiProperties;

   protected String destinationName;
   protected Destination destination;
   protected String connectionFactoryName;
   protected ConnectionFactory cf;

   protected boolean transacted;
   protected int acknowledgmentMode;

   // Constructors --------------------------------------------------

   JobSupport()
   {
      duration = Long.MAX_VALUE;
   }

   // Job implementation --------------------------------------------

   public final Result execute() throws Exception
   {
      try
      {
         initialize();
         return doWork();
      }
      catch(Exception e)
      {
         log.error("job failed", e);
         throw e;
      }
   }

   public String getExecutorURL()
   {
      return executorURL;
   }

   public void setExecutorURL(String executorURL)
   {
      this.executorURL = executorURL;
   }

   public int getMessageCount()
   {
      return messageCount;
   }

   public void setMessageCount(int messageCount)
   {
      this.messageCount = messageCount;
   }

   public int getMessageSize()
   {
      return messageSize;
   }

   public void setMessageSize(int messageSize)
   {
      this.messageSize = messageSize;
   }

   public long getDuration()
   {
      return duration;
   }

   public void setDuration(long duration)
   {
      this.duration = duration;
   }

   public int getRate()
   {
      return rate;
   }

   public void setRate(int rate)
   {
      this.rate = rate;
   }

   public Properties getJNDIProperties()
   {
      return jndiProperties;
   }

   public void setJNDIProperties(Properties jndiProperties)
   {
      this.jndiProperties = jndiProperties;
   }

   public String getDestinationName()
   {
      return destinationName;
   }

   public void setDestinationName(String destinationName)
   {
      this.destinationName = destinationName;
   }

   public String getConnectionFactoryName()
   {
      return connectionFactoryName;
   }

   public void setConnectionFactoryName(String connectionFactoryName)
   {
      this.connectionFactoryName = connectionFactoryName;
   }

   public boolean isTransacted()
   {
      return transacted;
   }

   public void setTransacted(boolean transacted)
   {
      this.transacted = transacted;
   }

   public int getAcknowledgmentMode()
   {
      return acknowledgmentMode;
   }

   public void setAcknowledgmentMode(int acknowledgmentMode)
   {
      this.acknowledgmentMode = acknowledgmentMode;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return getType() + "[" + JobConfiguration.executionURLToString(getExecutorURL()) + ", " +
         getDestinationName() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected abstract Result doWork() throws Exception;

   protected void initialize() throws Exception
   {
      if (jndiProperties == null)
      {
         return;
      }

      InitialContext ic = new InitialContext(jndiProperties);

      try
      {
         destination = (Destination)ic.lookup(destinationName);
         log.debug(this + " looking up destination " + destinationName + " and got " + destination);

         cf = (ConnectionFactory)ic.lookup(connectionFactoryName);
         log.debug(this + " looking up connection factory " + connectionFactoryName + " and got " +
            connectionFactoryName);
      }
      finally
      {
         ic.close();
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
