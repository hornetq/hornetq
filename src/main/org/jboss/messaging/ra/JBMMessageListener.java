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

import javax.jms.Message;
import javax.jms.MessageListener;

import org.jboss.messaging.core.logging.Logger;

/**
 * A wrapper for a message listener
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMMessageListener implements MessageListener
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMMessageListener.class);
   
   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The message listener */
   private MessageListener listener;
   
   /** The consumer */
   private JBMMessageConsumer consumer;

   /**
    * Create a new wrapper
    * @param listener the listener
    * @param consumer the consumer
    */
   public JBMMessageListener(MessageListener listener, JBMMessageConsumer consumer)
   {
      if (trace)
         log.trace("constructor(" + listener + ", " + consumer + ")");

      this.listener = listener;
      this.consumer = consumer;
   }

   /**
    * On message
    * @param message The message
    */
   public void onMessage(Message message)
   {
      if (trace)
         log.trace("onMessage(" + message + ")");

      message = consumer.wrapMessage(message);
      listener.onMessage(message);
   }
}
