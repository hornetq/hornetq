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

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.jboss.messaging.core.logging.Logger;

/**
 * A wrapper for a message
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMObjectMessage extends JBMMessage implements ObjectMessage
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMObjectMessage.class);

   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();

   /**
    * Create a new wrapper
    * @param message the message
    * @param session the session
    */
   public JBMObjectMessage(final ObjectMessage message, final JBMSession session)
   {
      super(message, session);

      if (trace)
      {
         log.trace("constructor(" + message + ", " + session + ")");
      }
   }

   /**
    * Get the object
    * @return The object
    * @exception JMSException Thrown if an error occurs
    */
   public Serializable getObject() throws JMSException
   {
      if (trace)
      {
         log.trace("getObject()");
      }

      return ((ObjectMessage)message).getObject();
   }

   /**
    * Set the object
    * @param object The object
    * @exception JMSException Thrown if an error occurs
    */
   public void setObject(final Serializable object) throws JMSException
   {
      if (trace)
      {
         log.trace("setObject(" + object + ")");
      }

      ((ObjectMessage)message).setObject(object);
   }
}
