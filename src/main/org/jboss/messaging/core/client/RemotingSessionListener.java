/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.jboss.messaging.core.client;

import org.jboss.messaging.core.exception.MessagingException;

/**
 * 
 * A RemotingSessionListener
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 */
public interface RemotingSessionListener
{
   /**
    * This method is called when a remoting session is destroyed. It can be
    * destroyed as part of its normal lifecycle after a clean close or if there
    * has been a problem (e.g. network failure).
    * 
    * If the session was closed properly, <code>me</code> is <code>null</code>,
    * otherwise <code>me</code> contains the exception which caused the
    * abnormal close.
    * 
    * @param sessionID
    *           the ID of the session
    * @param me
    *           <code>null</code> if the session was closed properly
    */
   void sessionDestroyed(long sessionID, MessagingException me);
}
