/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.messaging.core.local;


import javax.jms.JMSException;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.TransactionLog;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.util.Util;
import org.jboss.util.id.GUID;
import org.jboss.messaging.core.plugin.contract.MessageStore;

/**
 * Represents a subscription to a destination (topic or queue). It  job is to recoverably hold
 * messages in transit to consumers.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class Subscription extends Pipe
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(Subscription.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected Topic topic;
   protected String selector;
   protected boolean noLocal;
   
   // Constructors --------------------------------------------------

   public Subscription(Topic topic, String selector, boolean noLocal, MessageStore ms)
   {
      this("sub" + new GUID().toString(), topic, selector, noLocal, ms, null);
   }
   
   protected Subscription(String name, Topic topic, String selector, boolean noLocal,
                          MessageStore ms, TransactionLog tl)
   {
      // A Subscription must accept reliable messages, even if itself is non-recoverable
      super(name, ms, tl, true);
      this.topic = topic;
      this.selector = selector;
      this.noLocal = noLocal;
   }
   
   // Channel implementation ----------------------------------------

   // Public --------------------------------------------------------
   
   public void subscribe()
   {
      topic.add(this);
   }
   
   public void unsubscribe() throws JMSException
   {
      topic.remove(this);
   }
   
   public void closeConsumer() throws JMSException
   {
      unsubscribe();
      try
      {
         if (tl != null)
         {
            tl.removeAllMessageData(this.channelID);
         }
      }
      catch (Exception e)
      {
         final String msg = "Failed to remove message data for subscription";
         log.error(msg, e);
         throw new IllegalStateException(msg);
      }
   }
   
   public Topic getTopic()
   {
      return topic;
   }
   
   public String getSelector()
   {
      return selector;
   }
   
   public boolean isNoLocal()
   {
      return noLocal;
   }

   public String toString()
   {
      return "CoreSubscription[" + Util.guidToString(getChannelID()) + ", " + topic + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
