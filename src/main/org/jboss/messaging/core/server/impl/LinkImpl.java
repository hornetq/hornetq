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

package org.jboss.messaging.core.server.impl;

import static org.jboss.messaging.core.message.impl.MessageImpl.HDR_DUPLICATE_DETECTION_ID;
import static org.jboss.messaging.core.message.impl.MessageImpl.HDR_ORIGIN_QUEUE;

import java.nio.ByteBuffer;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Link;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.util.SimpleString;

/**
 * A LinkImpl simply makes a copy of a message and redirects it to another address
 * 
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 19 Dec 2008 10:57:49
 *
 *
 */
public class LinkImpl implements Link
{
   private static final Logger log = Logger.getLogger(LinkImpl.class);
   
   private final PostOffice postOffice;

   private final SimpleString address;
   
   private final boolean duplicateDetection;

   private final StorageManager storageManager;

   private volatile long id;

   private final Filter filter;

   private final boolean durable;

   private final SimpleString name;

   public LinkImpl(final SimpleString name,
                   final boolean durable,
                   final Filter filter,
                   final SimpleString address,
                   final boolean useDuplicateDetection,
                   final PostOffice postOffice,
                   final StorageManager storageManager)
   {
      this.name = name;

      this.durable = durable;

      this.filter = filter;

      this.address = address;
      
      this.duplicateDetection = useDuplicateDetection;

      this.postOffice = postOffice;

      this.storageManager = storageManager;
   }

   public boolean route(final ServerMessage message, final Transaction tx) throws Exception
   {     
      if (filter != null && !filter.match(message))
      {
         return false;
      }
      
      Integer iMaxHops = (Integer)message.getProperty(MessageImpl.HDR_MAX_HOPS);
      
     // log.info("IN LinkIMpl::route imaxhops is " + iMaxHops);
      
      if (iMaxHops != null)
      {
         int maxHops = iMaxHops.intValue();
         
         if (maxHops <= 0)
         {
            return false;
         }
      }
      
      ServerMessage copy = message.copy();
      
      copy.setMessageID(storageManager.generateUniqueID());

      SimpleString originalDestination = copy.getDestination();
      
      copy.setDestination(address);
      
      copy.putStringProperty(HDR_ORIGIN_QUEUE, originalDestination);
      
      if (duplicateDetection)
      {
         //We put the duplicate detection id in
         
         byte[] bytes = new byte[8];
         
         ByteBuffer bb = ByteBuffer.wrap(bytes);
         
         bb.putLong(copy.getMessageID());
         
         SimpleString duplID = new SimpleString(bytes).concat(name);
         
         copy.putStringProperty(HDR_DUPLICATE_DETECTION_ID, duplID);
      }
      
      postOffice.route(copy, tx);
      
      return true;
   }

   public Filter getFilter()
   {
      return filter;
   }

   public long getPersistenceID()
   {
      return id;
   }

   public SimpleString getName()
   {
      return name;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public void setPersistenceID(long id)
   {
      this.id = id;
   }

   public SimpleString getLinkAddress()
   {
      return address;
   }

}
