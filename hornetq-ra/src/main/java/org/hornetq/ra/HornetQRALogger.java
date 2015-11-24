/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
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
package org.hornetq.ra;

import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Logger;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

/**
 *
 * Logger Code 15
 * <p>
 * each message id must be 6 digits long starting with 15, the 3rd digit donates
 * the level so
 *
 * <pre>
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 * </pre>
 *
 * so an INFO message would be 151000 to 151999
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a> 3/15/12
 */
@MessageLogger(projectCode = "HQ")
public interface HornetQRALogger extends BasicLogger
{
   /**
    * The default logger.
    */
   HornetQRALogger LOGGER = Logger.getMessageLogger(HornetQRALogger.class, HornetQRALogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151000, value = "awaiting topic/queue creation {0}", format = Message.Format.MESSAGE_FORMAT)
   void awaitingTopicQueueCreation(String destination);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151001, value = "Attempting to reconnect {0}", format = Message.Format.MESSAGE_FORMAT)
   void attemptingReconnect(HornetQActivationSpec spec);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151002, value = "Reconnected with HornetQ", format = Message.Format.MESSAGE_FORMAT)
   void reconnected();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151003, value = "HornetQ resource adaptor stopped", format = Message.Format.MESSAGE_FORMAT)
   void raStopped();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151004, value = "Instantiating {0} \"{1}\" directly since UseJNDI=false.", format = Message.Format.MESSAGE_FORMAT)
   void instantiatingDestination(String destinationType, String destination);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151005, value = "awaiting HornetQ Server availability", format = Message.Format.MESSAGE_FORMAT)
   void awaitingJMSServerCreation();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151006, value = "Cluster topology change detected. Re-balancing connections on even {0}.", format = Message.Format.MESSAGE_FORMAT)
   void rebalancingConnections(String event);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152000, value = "It wasn't possible to lookup for a Transaction Manager through the configured properties TransactionManagerLocatorClass and TransactionManagerLocatorMethod" +
         "\nHornetQ Resource Adapter won't be able to set and verify transaction timeouts in certain cases.", format = Message.Format.MESSAGE_FORMAT)
   void noTXLocator();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152001, value = "problem resetting HornetQ xa session after failure" , format = Message.Format.MESSAGE_FORMAT)
   void problemResettingXASession(@Cause Throwable t);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152002, value = "Unable to roll local transaction back" , format = Message.Format.MESSAGE_FORMAT)
   void unableToRollbackTX();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152003, value = "unable to reset session after failure, we will place the MDB Inflow now in setup mode for activation={0}" , format = Message.Format.MESSAGE_FORMAT)
   void unableToResetSession(String spec, @Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152004, value = "Handling JMS exception failure" , format = Message.Format.MESSAGE_FORMAT)
   void handlingJMSFailure(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152005, value = "Failure in HornetQ activation {0}" , format = Message.Format.MESSAGE_FORMAT)
   void failureInActivation(@Cause Throwable t, HornetQActivationSpec spec);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152006, value = "Unable to call after delivery" , format = Message.Format.MESSAGE_FORMAT)
   void unableToCallAfterDelivery(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152007, value = "unable to clear the transaction" , format = Message.Format.MESSAGE_FORMAT)
   void unableToClearTX(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 154000, value = "Error while creating object Reference." , format = Message.Format.MESSAGE_FORMAT)
   void errorCreatingReference(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 154001, value = "Unable to stop HornetQ resource adapter." , format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingRA(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 154002, value = "Could not create session", format = Message.Format.MESSAGE_FORMAT)
   void errorCreatingSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 154003, value = "Unable to reconnect {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorReconnecting(@Cause Throwable t, HornetQActivationSpec spec);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 154004, value = "Failed to deliver message", format = Message.Format.MESSAGE_FORMAT)
   void errorDeliveringMessage(@Cause Throwable t);
}
