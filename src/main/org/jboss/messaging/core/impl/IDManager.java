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
package org.jboss.messaging.core.impl;

import org.jboss.jms.delegate.IDBlock;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.MessagingComponent;
import org.jboss.messaging.core.contract.PersistenceManager;

/**
 * 
 * A IDManager.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 2686 $</tt>
 *
 * $Id: IDManager.java 2686 2007-05-15 08:47:20Z timfox $
 */
public class IDManager implements MessagingComponent
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(IDManager.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   private boolean started;

   private String counterName;

   private int bigBlockSize;
   private long high;
   private long low;

   private PersistenceManager pm;

   // Constructors --------------------------------------------------

   public IDManager(String counterName, int bigBlockSize, PersistenceManager pm) throws Exception
   {
      this.counterName = counterName;
      this.bigBlockSize = bigBlockSize;
      this.pm = pm;
   }

   // MessagingComponent implementation -----------------------------

   public synchronized void start() throws Exception
   {
      getNextBigBlock();
      started = true;
   }

   public synchronized void stop() throws Exception
   {
      started = false;
   }

   // Public --------------------------------------------------------

   protected void getNextBigBlock() throws Exception
   {
      low = pm.reserveIDBlock(counterName, bigBlockSize);
      high = low + bigBlockSize - 1;
      if (trace) { log.trace(this + " retrieved next block of size " + bigBlockSize + " from PersistenceManager, starting at " + low); }
   }

   public synchronized IDBlock getIDBlock(int size) throws Exception
   {
      if (!started)
      {
         throw new IllegalStateException(this + " is not started");
      }

      if (size <= 0)
      {
         throw new IllegalArgumentException("block size must be > 0");
      }

      if (size > bigBlockSize)
      {
         throw new IllegalArgumentException("block size must be <= bigBlockSize");
      }

      if (size > high - low + 1)
      {
         getNextBigBlock();
      }

      long low = this.low;

      this.low += size;

      return new IDBlock(low, this.low - 1);
   }

   public synchronized long getID() throws Exception
   {
      return getIDBlock(1).getLow();
   }

   public String toString()
   {
      return "IDManager[" + counterName + ", " + low + "-" + high + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
