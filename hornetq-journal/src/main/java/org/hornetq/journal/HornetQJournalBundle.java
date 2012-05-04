package org.hornetq.journal;


import org.hornetq.api.core.IOErrorException;
import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/12/12
 *
 * Logger Code 14
 *
 * each message id must be 6 digits long starting with 14, the 3rd digit should be 9
 *
 * so 149000 to 149999
 */
@MessageBundle(projectCode = "HQ")
public interface HornetQJournalBundle
{
   HornetQJournalBundle BUNDLE = Messages.getBundle(HornetQJournalBundle.class);

   @Message(id = 149001, value =  "failed to rename file {0} to {1}", format = Message.Format.MESSAGE_FORMAT)
   IOErrorException ioRenameFileError(String name, String newFileName);

   @Message(id = 149002, value =  "Journal data belong to a different version", format = Message.Format.MESSAGE_FORMAT)
   IOErrorException journalDifferentVersion();

   @Message(id = 149003, value =  "Journal files version mismatch. You should export the data from the previous version and import it as explained on the user's manual",
         format = Message.Format.MESSAGE_FORMAT)
   IOErrorException journalFileMisMatch();

   @Message(id = 149004, value =   "File not opened", format = Message.Format.MESSAGE_FORMAT)
   IOErrorException fileNotOpened();
}
