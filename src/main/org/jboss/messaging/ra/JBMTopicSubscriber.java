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
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.jboss.messaging.core.logging.Logger;

/**
 * A wrapper for a topic subscriber
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMTopicSubscriber extends JBMMessageConsumer implements TopicSubscriber
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMTopicSubscriber.class);

   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();

   /**
    * Create a new wrapper
    * @param consumer the topic subscriber
    * @param session the session
    */
   public JBMTopicSubscriber(final TopicSubscriber consumer, final JBMSession session)
   {
      super(consumer, session);

      if (trace)
      {
         log.trace("constructor(" + consumer + ", " + session + ")");
      }
   }

   /**
    * Get the no local value
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public boolean getNoLocal() throws JMSException
   {
      if (trace)
      {
         log.trace("getNoLocal()");
      }

      checkState();
      return ((TopicSubscriber)consumer).getNoLocal();
   }

   /**
    * Get the topic
    * @return The topic
    * @exception JMSException Thrown if an error occurs
    */
   public Topic getTopic() throws JMSException
   {
      if (trace)
      {
         log.trace("getTopic()");
      }

      checkState();
      return ((TopicSubscriber)consumer).getTopic();
   }
}
