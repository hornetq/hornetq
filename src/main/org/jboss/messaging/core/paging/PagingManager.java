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

import java.util.Collection;

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessagingComponent;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * <p>Look at the <a href="http://wiki.jboss.org/wiki/JBossMessaging2Paging">WIKI</a> for more information.</p>
 * 
<PRE>

+------------+      1  +-------------+       N +------------+       N +-------+       1 +----------------+
| {@link PostOffice} |-------&gt; |PagingManager|-------&gt; |{@link PagingStore} | ------&gt; | {@link Page}  | ------&gt; | {@link SequentialFile} |
+------------+         +-------------+         +------------+         +-------+         +----------------+
                              |                       1 ^
                              |                         |
                              |                         |
                              |                         | 1
                              |        N +-------------------+
                              +--------&gt; | DestinationAdress |
                                         +-------------------+   

</PRE>

 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 *
 */
public interface PagingManager extends MessagingComponent
{
   void activate();

   boolean isBackup();

   /** The system is paging because of global-page-mode */
   boolean isGlobalPageMode();

   /** During startup PostOffice may set GlobalPageMode as true */
   void setGlobalPageMode(boolean globalMode);

   /**
    * @param destination
    * @return
    * @throws Exception 
    */
   PagingStore createPageStore(SimpleString destination) throws Exception;

   /** To return the PageStore associated with the address */
   PagingStore getPageStore(SimpleString address) throws Exception;

   /** An injection point for the PostOffice to inject itself */
   void setPostOffice(PostOffice postOffice);

   /**
    * To be used by transactions only.
    * If you're sure you will page if isPaging, just call the method page and look at its return. 
    * @param destination
    * @return
    */
   boolean isPaging(SimpleString destination) throws Exception;

   /**
    * Page, only if destination is in page mode.
    * @param message
    * @param sync - Sync should be called right after the write
    * @return false if destination is not on page mode
    */

   // FIXME - why are these methods still on PagingManager???
   // The current code is doing a lookup every time through this class just to call page store!!
   boolean page(ServerMessage message, boolean duplicateDetection) throws Exception;

   /**
    * Page, only if destination is in page mode.
    * @param message
    * @return false if destination is not on page mode
    */

   // FIXME - why are these methods still on PagingManager???
   // The current code is doing a lookup every time through this class just to call page store!!
   boolean page(ServerMessage message, long transactionId, boolean duplicateDetection) throws Exception;

   /**
    * Point to inform/restoring Transactions used when the messages were added into paging
    * */
   void addTransaction(PageTransactionInfo pageTransaction);

   /**
    * Point to inform/restoring Transactions used when the messages were added into paging
    * */
   PageTransactionInfo getTransaction(long transactionID);

   /** Sync current-pages on disk for these destinations */
   void sync(Collection<SimpleString> destinationsToSync) throws Exception;

   /**
    * @return
    */
   long getDefaultPageSize();

   /**
    * @param transactionID
    */
   void removeTransaction(long transactionID);

   /**
    * @return
    */
   long getMaxGlobalSize();

   /**
    * @return
    */
   long getGlobalSize();

   /**
    * @param size
    * @return
    */
   long addGlobalSize(long size);

   /**
    * 
    */
   void startGlobalDepage();

   /**
    * Reload previously created PagingStores into memory
    * @throws Exception 
    */
   void reloadStores() throws Exception;

}
