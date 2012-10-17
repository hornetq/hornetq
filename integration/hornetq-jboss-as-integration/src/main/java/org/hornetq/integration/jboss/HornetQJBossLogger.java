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
package org.hornetq.integration.jboss;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Logger;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import javax.security.auth.Subject;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/15/12
 *
 * Logger Code 13
 *
 * each message id must be 6 digits long starting with 13, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 131000 to 131999
 */
@MessageLogger(projectCode = "HQ")
public interface HornetQJBossLogger extends BasicLogger
{
   /**
    * The jboss integration logger.
    */
   HornetQJBossLogger LOGGER = Logger.getMessageLogger(HornetQJBossLogger.class, HornetQJBossLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 131001, value = "Security Context Setting Subject = {0}",
         format = Message.Format.MESSAGE_FORMAT)
   void settingSecuritySubject(Subject subject);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 132001, value = "An error happened while setting the context",
         format = Message.Format.MESSAGE_FORMAT)
   void errorSettingSecurityContext(@Cause Throwable Throwable);
}
