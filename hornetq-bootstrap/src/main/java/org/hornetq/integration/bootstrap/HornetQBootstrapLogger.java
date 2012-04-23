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
package org.hornetq.integration.bootstrap;

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
 * Logger Code 10
 *
 * each message id must be 6 digits long starting with 10, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 101000 to 101999
 */
@MessageLogger(projectCode = "HQ")
public interface HornetQBootstrapLogger extends BasicLogger
{
   /**
    * The default logger.
    */
   HornetQBootstrapLogger LOGGER = Logger.getMessageLogger(HornetQBootstrapLogger.class, HornetQBootstrapLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 101001, value = "Starting HornetQ Server", format = Message.Format.MESSAGE_FORMAT)
   void serverStarting();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 101002, value = "Stopping HornetQ Server", format = Message.Format.MESSAGE_FORMAT)
   void serverStopping();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 102001, value = "Error during undeployment: {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorDuringUndeployment(@Cause Throwable t, String name);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 104001, value = "Failed to delete file {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorDeletingFile(String name);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 104002, value = "Failed to start server", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingServer(@Cause Exception e);
}
