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
package org.jboss.jms.message;

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;

import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.logging.Logger;

/**
 * This class manages instances of MessageIdGenerator. It ensures there is one instance per instance
 * of JMS server as specified by the server id.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * MessageIdGeneratorFactory.java,v 1.1 2006/03/07 17:11:14 timfox Exp
 */
public class MessageIdGeneratorFactory
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MessageIdGeneratorFactory.class);

   public static MessageIdGeneratorFactory instance = new MessageIdGeneratorFactory();

   //TODO Make configurable
   private static final int BLOCK_SIZE = 256;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private Map holders;

   // Constructors --------------------------------------------------

   private MessageIdGeneratorFactory()
   {
      holders = new HashMap();
   }

   // Public --------------------------------------------------------

   public synchronized boolean containsMessageIdGenerator(int serverId)
   {
      return holders.containsKey(new Integer(serverId));
   }

   public synchronized MessageIdGenerator checkOutGenerator(int serverId,
                                                            ConnectionFactoryDelegate cfd)
      throws JMSException
   {
      Integer in = new Integer(serverId);
      
      Holder h = (Holder)holders.get(in);

      if (h == null)
      {
         h = new Holder(new MessageIdGenerator(cfd, BLOCK_SIZE));
         holders.put(in, h);
      }
      else
      {
         h.refCount++;
      }

      log.debug("checked out MessageIdGenerator for " + serverId +
                ", reference count is " + h.refCount);


      return h.generator;
   }

   public synchronized void checkInGenerator(int serverId)
   {
      Integer in = new Integer(serverId);
      
      Holder h = (Holder)holders.get(in);

      if (h == null)
      {
         throw new IllegalArgumentException("Cannot find generator for serverid:" + serverId);
      }

      h.refCount--;

      if (h.refCount == 0)
      {
         holders.remove(in);
         log.debug("checked in and removed MessageIdGenerator for " + serverId);
      }
      else
      {
         log.debug("checked in MessageIdGenerator for " + serverId +
                   ", reference count is " + h.refCount);
      }
   }

   public synchronized void clear()
   {
      holders.clear();
      log.debug("cleared MessageIdGeneratorFactory");
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class Holder
   {
      private Holder(MessageIdGenerator gen)
      {
         this.generator = gen;
      }

      MessageIdGenerator generator;

      int refCount = 1;
   }

}
