package org.hornetq.core.journal.impl;

public final class JournalConstants
{

   public static final int DEFAULT_JOURNAL_BUFFER_SIZE_AIO = 490 * 1024;
   public static final int DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO = (int)(1000000000d / 2000);
   public static final int DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO = (int)(1000000000d / 300);
   public static final int DEFAULT_JOURNAL_BUFFER_SIZE_NIO = 490 * 1024;

}
