/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.messaging.ra;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueReceiver;

import org.jboss.messaging.core.logging.Logger;

/**
 * A wrapper for a queue receiver
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMQueueReceiver extends JBMMessageConsumer implements QueueReceiver
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMQueueReceiver.class);

   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();

   /**
    * Create a new wrapper
    * @param consumer the queue receiver
    * @param session the session
    */
   public JBMQueueReceiver(final QueueReceiver consumer, final JBMSession session)
   {
      super(consumer, session);

      if (trace)
      {
         log.trace("constructor(" + consumer + ", " + session + ")");
      }
   }

   /**
    * Get queue
    * @return The queue
    * @exception JMSException Thrown if an error occurs
    */
   public Queue getQueue() throws JMSException
   {
      if (trace)
      {
         log.trace("getQueue()");
      }

      checkState();
      return ((QueueReceiver)consumer).getQueue();
   }
}
