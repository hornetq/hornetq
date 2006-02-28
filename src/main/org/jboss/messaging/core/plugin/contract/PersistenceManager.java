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
package org.jboss.messaging.core.plugin.contract;

import java.io.Serializable;
import java.util.List;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.tx.Transaction;

/**
 * The interface to the persistence manager
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.1</tt>
 *
 * PersistenceManager.java,v 1.1 2006/02/22 17:33:42 timfox Exp
 */
public interface PersistenceManager extends ServerPlugin
{
   /*
    * Currently unused but will be used for XA recovery
    */
   List retrievePreparedTransactions() throws Exception;
   
   /**
    * TODO Do we really need this method?
    */
   void removeAllMessageData(long channelID) throws Exception;
   
   void addReference(long channelID, MessageReference ref, Transaction tx) throws Exception;

   void removeReference(long channelID, MessageReference ref, Transaction tx) throws Exception;

   /**
    * @return a List of StorageIdentifiers for all messages whose delivery hasn't been attempted yet.
    */
   List messageRefs(long channelID) throws Exception;

   Message getMessage(Serializable messageID) throws Exception;
   
   /*
    * FIXME
    * Only used in testing - remove this
    * Commented out until 1.2
    */
   //int getMessageReferenceCount(Serializable messageID) throws Exception;
}
