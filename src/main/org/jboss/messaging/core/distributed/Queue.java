/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.core.local.LocalQueue;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.util.RpcServer;
import org.jboss.messaging.core.util.RpcServerCall;
import org.jboss.messaging.core.util.ServerResponse;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.Address;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

/**
 * A distributed queue "representative" on a peer VM.
 *
 * TODO The distributed queue is broken. Review the implementation to use an AbstractDestination
 *      instead of a LocalPipe and a Router. See how LocalQueue is implemented.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Queue extends LocalQueue implements QueueServerDelegate
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Queue.class);

   // Attributes ----------------------------------------------------

   protected boolean started;

   protected Serializable distributedQueueID;
   protected Serializable peerID;
   protected RpcDispatcher dispatcher;
   protected RpcServer rpcServer;
   protected org.jgroups.Channel jChannel;

   /** the ID of the pipe used by this peer to receive incoming messages from the other peers */
   protected Serializable pipeID;


   // Constructors --------------------------------------------------

   /**
    * @param dispatcher - the dispatcher to listen on. The underlying JChannel doesn't necessarily
    *        have to be connected at the time the Queue instance is created.
    * @param distributedQueueID - the id of the distributed queue. It must match the id used to
    *        instantiate the other peers.
    * @exception IllegalStateException - thrown if the RpcDispatcher does not come pre-configured
    *            with an RpcServer, so this instance cannot register itself to field distributed
    *            calls.
    *
    * @see org.jboss.messaging.core.distributed.Queue#start()
    */
   public Queue(RpcDispatcher dispatcher, Serializable distributedQueueID)
   {
      super(distributedQueueID);
      Object serverObject = dispatcher.getServerObject();
      if (!(serverObject instanceof RpcServer))
      {
         throw new IllegalStateException("The RpcDispatcher does not have an RpcServer installed");
      }
      rpcServer = (RpcServer)serverObject;

      this.dispatcher = dispatcher;
      this.distributedQueueID = distributedQueueID;
      this.peerID = "queuePeer"+getUniqueID().toString();
      jChannel = dispatcher.getChannel();
      started = false;
      pipeID = distributedQueueID.toString() + "." + peerID.toString() + "-pipe" +
               PipeOutput.getUniqueID().toString();
   }


   // Public --------------------------------------------------------


   /**
    * Lifecycle method. Starts the peer by connecting it to the distributed queue. The underlying
    * JChannel must be connected when this method is invoked.
    *
    * @exception DistributedException - a wrapper for the exception thrown by the distributed layer
    *            (JGroups). The original exception, if any, is nested.
    */
   public synchronized void start() throws DistributedException
   {
      if(started)
      {
         return;
      }

      if (!jChannel.isConnected())
      {
         throw new DistributedException("The underlying JGroups channel not connected");
      }

      log.debug(this + " starting");

      // announce myself to the other peers and wait for their acknowledgment
      RpcServerCall rpcServerCall =
            new RpcServerCall(distributedQueueID,
                              "peerJoins",
                              new Object[] {jChannel.getLocalAddress(), pipeID},
                              new String[] {"org.jgroups.Address", "java.io.Serializable"});

      // TODO use the timout when I'll change the send() signature or deal with the timeout
      Collection responses = rpcServerCall.remoteInvoke(dispatcher, 30000);

      log.debug(this + " received " + responses.size() + " responses");

      ServerResponse response = null;
      try {
         // all peers must acknowledge
         for(Iterator i = responses.iterator(); i.hasNext(); )
         {
            response = (ServerResponse)i.next();

            log.debug(this + " received: " + response);

            Object result = response.getInvocationResult();
            if (result instanceof Throwable)
            {
               throw (Throwable)result;
            }
            linkToPeer((QueueJoinAcknowledgment)result);
         }
      }
      catch(Throwable t)
      {
         String msg = "One of the peers (" +
                      RpcServer.serverDelegateToString(response.getAddress(),
                                                  response.getCategory(),
                                                  response.getServerDelegateID()) +
                      ") prevented this peer (" + this + ") from joining the queue";
         log.error(msg, t);
         throw new DistributedException(msg, t);
      }


      // register the server objects with the RpcServer

      //
      // TODO FIX THIS and then enable the tests!
      //
//      PipeOutput pipeOutput = new PipeOutput(pipeID, router);
      PipeOutput pipeOutput = null;
      //
      //
      //

      if (!rpcServer.registerUnique(pipeID, pipeOutput))
      {
         // the pipe output server delegate not unique under category
         throw new IllegalStateException("The category " + pipeID +
                                         "has already a server delegate registered");
      }
      rpcServer.register(distributedQueueID, this);
      started = true;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   /**
    * Lifecycle method. Stops the peer and disconnects it from the distributed queue.
    *
    * @exception DistributedException - a wrapper for the exception thrown by the distributed layer
    *            (JGroups). The original exception, if any, is nested.
    */
   public synchronized void stop() throws DistributedException
   {
      throw new NotYetImplementedException();
   }

   // QueueServerDelegate implementation -----------------------------

   public Serializable getID()
   {
      return peerID;
   }

   public QueueJoinAcknowledgment peerJoins(Address joiningPeerAddress,
                                            Serializable joiningPeerPipeID)
         throws Exception
   {
      // I will never receive my own call, since the server objects are not registered
      // at the time of call
      log.debug(this + ".peerJoins(" + joiningPeerAddress + ", " + joiningPeerPipeID + ")");

      // create a distributed pipe to the new peer; don't use this pipe yet, as its output is
      // not registered with the joining peer's RpcServer.
      Pipe pipeToPeer =  new Pipe(Channel.SYNCHRONOUS, dispatcher,
                                  joiningPeerAddress, joiningPeerPipeID);

      // add it as a router's receiver
      // TODO what happens if this peer receives in this very moment a message to be
      // TODO delivered to the queue? Seding to the joining peer will fail, since its distributed
      // TODO pipe isn't completely functional yet. To add test case.
      add(pipeToPeer);

      return new QueueJoinAcknowledgment(jChannel.getLocalAddress(), pipeID);
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer();
      sb.append(distributedQueueID);
      sb.append('.');
      sb.append(peerID);
      return sb.toString();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   /**
    * Create the distributed pipe to the peer that just acknowledged. All unckecked exceptions
    * thrown by this methods are taken care of by the caller method.
    */
   private void linkToPeer(QueueJoinAcknowledgment ack)
   {
      // I will never receive an acknowledgment from myself, since my server objects are not
      // registered yet, so I can safely link to peer.

      Pipe pipeToPeer = new Pipe(Channel.SYNCHRONOUS, dispatcher,
                                 ack.getAddress(), ack.getPipeID());
      add(pipeToPeer);
   }

   // Static --------------------------------------------------------

   /** access it only from getUniqueID() */
   private static int sequence = 0;

   /**
    * Returns runtime DistributeQueuePeer IDs that are unique per classloading domain.
    * @return an unique Integer.
    */
   private synchronized static Integer getUniqueID()
   {
      return new Integer(sequence++);
   }

}
