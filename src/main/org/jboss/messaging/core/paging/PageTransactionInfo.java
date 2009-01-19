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

package org.jboss.messaging.core.paging;

import org.jboss.messaging.core.journal.EncodingSupport;

/**
 * 
 * <p>Look at the <a href="http://wiki.jboss.org/wiki/JBossMessaging2Paging">WIKI</a> for more information.</p>
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface PageTransactionInfo extends EncodingSupport
{
   boolean waitCompletion(int timeoutMilliSeconds) throws Exception;

   boolean isCommit();
   
   boolean isRollback();
   
   void commit();

   void rollback();

   long getRecordID();

   void setRecordID(long id);

   long getTransactionID();

   int increment();

   int decrement();

   int getNumberOfMessages();

   void markIncomplete();
}
