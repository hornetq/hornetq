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

import javax.jms.JMSException;

import org.jboss.jms.delegate.ConnectionEndpoint;
import org.jboss.jms.delegate.IDBlock;
import org.jboss.logging.Logger;

/**
 * 
 * A MessageIdGenerator.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessageIdGenerator
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MessageIdGenerator.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   protected long high;
   protected long nextID;
   protected int blockSize;

   // Constructors --------------------------------------------------

   public MessageIdGenerator(int blockSize)  throws JMSException
   {
      this.blockSize = blockSize;
   }

   // Public --------------------------------------------------------

   protected void getNextBlock(ConnectionEndpoint connection) throws JMSException
   {
      IDBlock block = connection.getIdBlock(blockSize);

      nextID = block.getLow();
      high = block.getHigh();

      if (trace) { log.trace("Got block of IDs from server, low=" + nextID + " high=" + high); }
   }

   public synchronized long getId(ConnectionEndpoint connection) throws JMSException
   {
      if (nextID == high)
      {
         getNextBlock(connection);
      }

      long id = nextID;
      
      if (log.isTraceEnabled()) { log.trace("Getting next message id=" + id); }
      
      nextID++;

      return id;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
}
