/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
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
package org.hornetq.core.server;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/8/12
 *
 * Logger Code 11
 *
 * each message id must be 6 digits long starting with 10, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 101000 to 101999
 */
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Logger;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import java.util.concurrent.ExecutorService;

@MessageLogger(projectCode = "HQ")
public interface HornetQLogger extends BasicLogger
{
   /**
     * The default logger.
     */
    HornetQLogger LOGGER = Logger.getMessageLogger(HornetQLogger.class, HornetQLogger.class.getPackage().getName());

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 111001, value = "{0} server is starting with configuration {1}", format = Message.Format.MESSAGE_FORMAT)
    void serverStarting(String type, Configuration configuration);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 111002, value = "{0} is already started, ignoring the call to start..", format = Message.Format.MESSAGE_FORMAT)
    void serverAlreadyStarted(String type);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 111003, value = "HornetQ Server version {0} [{1}] {2}", format = Message.Format.MESSAGE_FORMAT)
    void serverStarted(String fullVersion, SimpleString nodeId, String identity);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 111004, value = "HornetQ Server version {0} [{1}] stopped", format = Message.Format.MESSAGE_FORMAT)
    void serverStopped(String version, SimpleString nodeId);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 111005, value = "trying to deploy queue {0}", format = Message.Format.MESSAGE_FORMAT)
    void deployQueue(SimpleString queueName);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 111006, value = "{0}", format = Message.Format.MESSAGE_FORMAT)
    void dumpServerInfo(String serverInfo);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 111007, value = "Deleting pending large message as it wasn't completed: {0}", format = Message.Format.MESSAGE_FORMAT)
    void deletingPendingMessage(Pair<Long, Long> msgToDelete);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 111008, value = "Waiting to obtain live lock", format = Message.Format.MESSAGE_FORMAT)
    void awaitingLiveLock();

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 111009, value = "Server is now live", format = Message.Format.MESSAGE_FORMAT)
    void serverIsLive();

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 111010, value = "live server wants to restart, restarting server in backup" , format = Message.Format.MESSAGE_FORMAT)
    void awaitFailBack();

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 1111, value = "HornetQ Backup Server version {0} [{1}] started, waiting live to fail before it gets active",
          format = Message.Format.MESSAGE_FORMAT)
    void backupServerStarted(String version, SimpleString nodeID);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 111012, value = "Backup Server is now live", format = Message.Format.MESSAGE_FORMAT)
    void backupServerIsLive();

   @LogMessage(level = Logger.Level.INFO)
    @Message(id = 111013, value = "Server {0} is now live", format = Message.Format.MESSAGE_FORMAT)
    void serverIsLive(String identity);

   @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112001, value = "HornetQServer is being finalized and has not been stopped. Please remember to stop the server before letting it go out of scope" ,
          format = Message.Format.MESSAGE_FORMAT)
    void serverFinalisedWIthoutBeingSTopped();

   @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112002, value = "Error closing sessions while stopping server" , format = Message.Format.MESSAGE_FORMAT)
    void errorClosingSessionsWhileStoppingServer(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112003, value = "Timed out waiting for pool to terminate {0}. Interrupting all its threads!", format = Message.Format.MESSAGE_FORMAT)
    void timedOutStoppingThreadpool(ExecutorService service);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112004, value = "Must specify a name for each divert. This one will not be deployed." , format = Message.Format.MESSAGE_FORMAT)
    void divertWithNoName();

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112005, value = "Must specify an address for each divert. This one will not be deployed." , format = Message.Format.MESSAGE_FORMAT)
    void divertWithNoAddress();

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112006, value = "Must specify a forwarding address for each divert. This one will not be deployed." , format = Message.Format.MESSAGE_FORMAT)
    void divertWithNoForwardingAddress();

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112007, value = "Binding already exists with name {0}, divert will not be deployed", format = Message.Format.MESSAGE_FORMAT)
    void divertBindingNotExists(SimpleString bindingName);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112008, value = "Security risk! HornetQ is running with the default cluster admin user and default password. "
                  + "Please see the HornetQ user guide, cluster chapter, for instructions on how to change this." , format = Message.Format.MESSAGE_FORMAT)
    void clusterSecurityRisk();

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112009, value = "unable to restart server, please kill and restart manually", format = Message.Format.MESSAGE_FORMAT)
    void serverRestartWarning();

    @LogMessage(level = Logger.Level.WARN)
    void serverRestartWarning(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112010, value = "Unable to announce backup for replication. Trying to stop the server.", format = Message.Format.MESSAGE_FORMAT)
    void replicationStartProblem(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112011, value = "Critical IO Error, shutting down the server. code={0}, message={1}", format = Message.Format.MESSAGE_FORMAT)
    void ioErrorShutdownServer(int code, String message);

   @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112012, value = "Error stopping server", format = Message.Format.MESSAGE_FORMAT)
    void errorStoppingServer(@Cause Exception e);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112013, value = "Timed out waiting for backup activation to exit", format = Message.Format.MESSAGE_FORMAT)
    void backupActivationProblem();

   @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112014, value = "Error when trying to start replication", format = Message.Format.MESSAGE_FORMAT)
    void errorStartingReplication(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112015, value = "Error when trying to stop replication", format = Message.Format.MESSAGE_FORMAT)
    void errorStoppingReplication(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
    @Message(id = 112016, value = "{0}", format = Message.Format.MESSAGE_FORMAT)
    void warn(String message);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 113001, value = "Server already started!", format = Message.Format.MESSAGE_FORMAT)
    void serverAlreadyStarted();

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 113002, value = "Starting server {0}", format = Message.Format.MESSAGE_FORMAT)
    void startingServer(HornetQServer server);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 113003, value = "Cancelled the execution of {0}", format = Message.Format.MESSAGE_FORMAT)
    void cancelExecution(Runnable runnable);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 113004, value = "First part initialization on  {0}", format = Message.Format.MESSAGE_FORMAT)
    void initializeFirstPart(Runnable runnable);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 113005, value = "announcing backup to the former live {0}", format = Message.Format.MESSAGE_FORMAT)
    void announceBackupToFormerLive(Runnable runnable);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 113006, value = "{0} ::Stopping live node in favor of failback", format = Message.Format.MESSAGE_FORMAT)
    void stoppingLiveNodeInFavourOfFailback(HornetQServerImpl server);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 113007, value = "{0} ::Starting backup node now after failback", format = Message.Format.MESSAGE_FORMAT)
    void startingBackupAfterFailure(HornetQServerImpl server);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 114001, value = "Failure in initialisation", format = Message.Format.MESSAGE_FORMAT)
    void initializationError(@Cause Throwable e);
}
