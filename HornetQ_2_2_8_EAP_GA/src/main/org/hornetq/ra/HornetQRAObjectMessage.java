/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.ra;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.hornetq.core.logging.Logger;

/**
 * A wrapper for a message
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class HornetQRAObjectMessage extends HornetQRAMessage implements ObjectMessage
{
   /** The logger */
   private static final Logger log = Logger.getLogger(HornetQRAObjectMessage.class);

   /** Whether trace is enabled */
   private static boolean trace = HornetQRAObjectMessage.log.isTraceEnabled();

   /**
    * Create a new wrapper
    * @param message the message
    * @param session the session
    */
   public HornetQRAObjectMessage(final ObjectMessage message, final HornetQRASession session)
   {
      super(message, session);

      if (HornetQRAObjectMessage.trace)
      {
         HornetQRAObjectMessage.log.trace("constructor(" + message + ", " + session + ")");
      }
   }

   /**
    * Get the object
    * @return The object
    * @exception JMSException Thrown if an error occurs
    */
   public Serializable getObject() throws JMSException
   {
      if (HornetQRAObjectMessage.trace)
      {
         HornetQRAObjectMessage.log.trace("getObject()");
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
      if (HornetQRAObjectMessage.trace)
      {
         HornetQRAObjectMessage.log.trace("setObject(" + object + ")");
      }

      ((ObjectMessage)message).setObject(object);
   }
}
