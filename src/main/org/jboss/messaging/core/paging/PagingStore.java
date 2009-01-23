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

import java.util.concurrent.Executor;

import org.jboss.messaging.core.server.MessagingComponent;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * <p>The implementation will take care of details such as PageSize.</p>
 * <p>The producers will write directly to PagingStore and that will decide what
 * Page file should be used based on configured size</p>
 * 
 * <p>Look at the <a href="http://wiki.jboss.org/wiki/JBossMessaging2Paging">WIKI</a> for more information.</p>
 * @see PagingManager

 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface PagingStore extends MessagingComponent
{
   int getNumberOfPages();

   SimpleString getStoreName();

   /** Maximum number of bytes allowed in memory */
   long getMaxSizeBytes();

   boolean isDropWhenMaxSize();

   long getPageSizeBytes();

   long getAddressSize();

   /** @return true if paging was started, or false if paging was already started before this call */
   boolean startPaging() throws Exception;

   boolean isPaging();

   void sync() throws Exception;

   boolean page(PagedMessage message, boolean sync, boolean duplicateDetection) throws Exception;
   
   public boolean readPage() throws Exception;

   /**
    * 
    * @return false if a thread was already started, or if not in page mode
    * @throws Exception 
    */
   boolean startDepaging();

   /** When start depaging from a global perspective, we don't want all the stores depaging at once what could saturate the servers */
   boolean startDepaging(Executor executor);

   /**
    * @param memoryEstimate
    * @return
    * @throws Exception 
    */
   void addSize(long memoryEstimate) throws Exception;
}
