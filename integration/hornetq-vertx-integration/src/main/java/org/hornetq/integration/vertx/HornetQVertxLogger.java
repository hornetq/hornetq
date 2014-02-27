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
package org.hornetq.integration.vertx;

import org.hornetq.core.server.ServerMessage;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Logger;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

/**
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
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
public interface HornetQVertxLogger extends BasicLogger
{
   /**
    * The vertx logger.
    */
   HornetQVertxLogger LOGGER = Logger.getMessageLogger(HornetQVertxLogger.class, HornetQVertxLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 192001, value = "Non vertx message: {0}", format = Message.Format.MESSAGE_FORMAT)
   void nonVertxMessage(ServerMessage message);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 192002, value = "Invalid vertx type: {0}", format = Message.Format.MESSAGE_FORMAT)
   void invalidVertxType(Integer type);

}
