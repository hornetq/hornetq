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
package org.hornetq.rest;

import org.hornetq.api.core.HornetQException;
import org.hornetq.rest.queue.push.xml.XmlLink;
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
 * Logger Code 19
 *
 * each message id must be 6 digits long starting with 19, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 191000 to 191999
 */
@MessageLogger(projectCode = "HQ")
public interface HornetQRestLogger extends BasicLogger
{
   /**
    * The twitter logger.
    */
   HornetQRestLogger LOGGER = Logger.getMessageLogger(HornetQRestLogger.class, HornetQRestLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 181001, value = "Loading REST push store from: {0}", format = Message.Format.MESSAGE_FORMAT)
   void loadingRestStore(String path);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 181002, value = "adding REST push registration: {0}", format = Message.Format.MESSAGE_FORMAT)
   void addingPushRegistration(String id);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 181003, value = "Push consumer started for: {0}", format = Message.Format.MESSAGE_FORMAT)
   void startingPushConsumer(XmlLink link);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 182001, value = "shutdown REST consumer because of timeout for: {0}", format = Message.Format.MESSAGE_FORMAT)
   void shutdownRestConsumer(String id);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 182002, value = "shutdown REST subscription because of timeout for: {0}", format = Message.Format.MESSAGE_FORMAT)
   void shutdownRestSubscription(String id);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 184001, value = "Failed to load push store {0}, it is probably corrupted", format = Message.Format.MESSAGE_FORMAT)
   void errorLoadingStore(@Cause Exception e, String name);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 184002, value = "Error updating store", format = Message.Format.MESSAGE_FORMAT)
   void errorUpdatingStore(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 184003, value = "Failed to push message to {0} disabling push registration...", format = Message.Format.MESSAGE_FORMAT)
   void errorPushingMessage(XmlLink link);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 184004, value = "Error deleting Subscriber queue", format = Message.Format.MESSAGE_FORMAT)
   void errorDeletingSubscriberQueue(@Cause HornetQException e);
}
