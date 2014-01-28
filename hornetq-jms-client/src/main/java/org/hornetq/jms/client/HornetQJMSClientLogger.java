/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.jms.client;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Logger;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/15/12
 *
 * Logger Code 12
 *
 * each message id must be 6 digits long starting with 12, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 121000 to 121999
 */
@MessageLogger(projectCode = "HQ")
public interface HornetQJMSClientLogger extends BasicLogger
{
   /**
    * The default logger.
    */
   HornetQJMSClientLogger LOGGER = Logger.getMessageLogger(HornetQJMSClientLogger.class, HornetQJMSClientLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122000, value = "I''m closing a JMS connection you left open. Please make sure you close all JMS connections explicitly before letting them go out of scope! see stacktrace to find out where it was created" , format = Message.Format.MESSAGE_FORMAT)
   void connectionLeftOpen(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122001, value = "Unhandled exception thrown from onMessage" , format = Message.Format.MESSAGE_FORMAT)
   void onMessageError(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124000, value = "Failed to call JMS exception listener" , format = Message.Format.MESSAGE_FORMAT)
   void errorCallingExcListener(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124001, value = "Consumer Failed to prepare message for delivery", format = Message.Format.MESSAGE_FORMAT)
   void errorPreparingMessage(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124002, value = "Queue Browser failed to create message" , format = Message.Format.MESSAGE_FORMAT)
   void errorCreatingMessage(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124003, value = "Message Listener failed to prepare message for receipt" , format = Message.Format.MESSAGE_FORMAT)
   void errorPreparingMessageForReceipt(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124004, value = "Message Listener failed to process message" , format = Message.Format.MESSAGE_FORMAT)
   void errorProcessingMessage(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124005, value = "Message Listener failed to recover session" , format = Message.Format.MESSAGE_FORMAT)
   void errorRecoveringSession(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124006, value = "Failed to call Failover listener" , format = Message.Format.MESSAGE_FORMAT)
   void errorCallingFailoverListener(@Cause Exception e);

}
