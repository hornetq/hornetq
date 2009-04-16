/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.server;

import org.jboss.messaging.core.journal.EncodingSupport;
import org.jboss.messaging.core.message.Message;

/**
 * 
 * A ServerMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface ServerMessage extends Message, EncodingSupport
{
   void setMessageID(long id);

   MessageReference createReference(Queue queue);

   int incrementRefCount();

   int incrementDurableRefCount();

   int decrementDurableRefCount();

   int decrementRefCount();

   ServerMessage copy(long newID) throws Exception;
   
   ServerMessage copy() throws Exception;

   int getMemoryEstimate();

   void setStored();

   boolean isStored();

   int getRefCount();
   
   //TODO - we might be able to put this in a better place
}
