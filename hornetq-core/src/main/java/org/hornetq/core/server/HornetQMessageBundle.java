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
package org.hornetq.core.server;

import org.hornetq.api.core.HornetQException;
import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;
import org.jboss.logging.Property;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/12/12
 *
 * Logger Code 11
 *
 * each message id must be 6 digits long starting with 10, the 3rd digit should be 9
 *
 * so 119000 to 119999
 */
@MessageBundle(projectCode = "HQ")
public interface HornetQMessageBundle
{
   HornetQMessageBundle MESSAGES = Messages.getBundle(HornetQMessageBundle.class);

   @Message(id = 119001, value = "Generating thread dump because - {0}", format = Message.Format.MESSAGE_FORMAT)
   String generatingThreadDump(String reason);

   @Message(id = 119002, value = "End Thread dump", format = Message.Format.MESSAGE_FORMAT)
   String endThreadDump();

   @Message(id = 119003, value = "Thread {0} name {1} id {2} group {3}", format = Message.Format.MESSAGE_FORMAT)
   String threadInfo(Thread key, String name, Long id, ThreadGroup group);

   @Message(id = 119004, value = "Connected server is not a backup server", format = Message.Format.MESSAGE_FORMAT)
   HornetQException notABackupServer(@Property Integer code);

   @Message(id = 119005, value = "Backup replication server is already connected to another server", format = Message.Format.MESSAGE_FORMAT)
   String backupServerAlreadyConnectingToLive();
}
