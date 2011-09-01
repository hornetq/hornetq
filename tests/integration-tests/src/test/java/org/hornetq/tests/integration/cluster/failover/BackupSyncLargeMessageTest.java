package org.hornetq.tests.integration.cluster.failover;

import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ServerLocatorInternal;

public class BackupSyncLargeMessageTest extends BackupSyncJournalTest
{

   /**
    * @param i
    * @param message
    */
   @Override
   protected void assertMessageBody(final int i, final ClientMessage message)
   {
      assertLargeMessageBody(i, message);
   }

   @Override
   protected ServerLocatorInternal getServerLocator() throws Exception
   {
      ServerLocator locator = super.getServerLocator();
      locator.setMinLargeMessageSize(MIN_LARGE_MESSAGE);
      return (ServerLocatorInternal)locator;
   }

   /**
    * @param i
    * @param message
    */
   @Override
   protected void setBody(final int i, final ClientMessage message) throws Exception
   {
      setLargeMessageBody(i, message);
   }
}
