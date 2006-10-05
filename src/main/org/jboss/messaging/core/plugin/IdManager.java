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
package org.jboss.messaging.core.plugin;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessagingComponent;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;

/**
 * 
 * A IdManager.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * IdManager.java,v 1.1 2006/03/07 17:11:15 timfox Exp
 */
public class IdManager implements MessagingComponent
{
   private static final Logger log = Logger.getLogger(IdManager.class);   
   
   private boolean trace = log.isTraceEnabled();
   
   protected int bigBlockSize;
   
   protected long high;
   
   protected long nextBlock;
         
   protected PersistenceManager pm;
   
   protected String counterName;
   
   private boolean started;
   
   public IdManager(String counterName, int bigBlockSize, PersistenceManager pm) throws Exception
   {
      this.bigBlockSize = bigBlockSize;
      
      this.pm = pm;
      
      this.counterName = counterName;           
   }
   
   public synchronized void start() throws Exception
   {
      getNextBigBlock();
      
      started = true;
   }
   
   public synchronized void stop() throws Exception
   {
      started = false;
   }
   
   public synchronized IdBlock getIdBlock(int size) throws Exception
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
      
      if (size > high - nextBlock + 1)
      {
         getNextBigBlock();
      }
      
      long low = nextBlock;
      
      nextBlock += size;
      
      IdBlock block = new IdBlock(low, nextBlock - 1);
                      
      return block;
   }
   
   public synchronized long getId() throws Exception
   {
      return getIdBlock(1).low;
   }
   
   protected void getNextBigBlock() throws Exception
   {
      nextBlock = pm.reserveIDBlock(counterName, bigBlockSize);
      
      if (trace) { log.trace("Retrieved next block of size " + bigBlockSize + " from pm starting at " + nextBlock); }
      
      high = nextBlock + bigBlockSize - 1;
   }
}
