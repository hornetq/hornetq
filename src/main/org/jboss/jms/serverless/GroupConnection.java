/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.ConnectionMetaData;
import javax.jms.ExceptionListener;
import javax.jms.ConnectionConsumer;
import javax.jms.ServerSessionPool;
import javax.jms.Destination;
import javax.jms.Topic;
import javax.jms.Session;
import org.jgroups.JChannel;
import org.jgroups.ChannelListener;
import org.jgroups.Channel;
import org.jgroups.Address;
import org.jgroups.ChannelException;
import java.io.Serializable;
import java.net.URL;
import javax.jms.Queue;
import org.jgroups.SetStateEvent;
import org.jgroups.util.Util;
import org.jgroups.GetStateEvent;
import org.jgroups.View;
import org.jgroups.SuspectEvent;
import org.jgroups.ChannelClosedException;
import org.jgroups.ChannelNotConnectedException;

/**
 * The main piece of the JMS client runtime. Sits in top of a JChannel and mainains the "server
 * group" state. Delegates the session management to the SessionManager instance. Deals with 
 * message delivery to and from sessions. Implements the Connection interface.
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class GroupConnection implements Connection, Runnable {

    private static final Logger log = Logger.getLogger(GroupConnection.class);

    private static final String DEFAULT_SERVER_GROUP_NAME = "serverGroup";

    private URL serverChannelConfigURL;

    private SessionManager sessionManager;
    private org.jgroups.util.Queue deliveryQueue;
    private ConnectionState connState;

//     private ChannelState channelState;
    private GroupState groupState;
    private Thread connManagementThread;
    private JChannel serverChannel;

    /**
     * The constructor leaves the Connection in a DISCONNECTED state.
     *
     * @param serverChannelConfigURL the URL of the XML file containing channel configuration.
     **/
    GroupConnection(URL serverChannelConfigURL) {

        this.serverChannelConfigURL = serverChannelConfigURL;

        deliveryQueue = new org.jgroups.util.Queue();
        sessionManager = new SessionManager(this, deliveryQueue);
        groupState = new GroupState();
        connManagementThread = new Thread(this, "Connection Management Thread");
        connState = new ConnectionState();

    }

    /**
     * Initalizes the connection, by connecting the channel to the server group. Should be called
     * only once, when the Connection instance is created.
     **/
    void connect() throws JMSException {

        // TO_DO: if is already connected (stopped), just return

        try {

            serverChannel = new JChannel(serverChannelConfigURL);
            serverChannel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
            serverChannel.setChannelListener(new ChannelListener() {
                    
                    public void  channelClosed(Channel channel) {
                        log.debug("channelClosed("+channel+")");
                    }
           
                    public void channelConnected(Channel channel) {
                        log.debug("channelConnected() to group ["+
                                 channel.getChannelName()+"]");
                    }
                    
                    public void channelDisconnected(Channel channel) {
                        log.debug("channelDisconnected("+channel+")");
                    }
           
                    public void channelReconnected(Address addr) {
                        log.debug("channelReconnected("+addr+")");
                    }
           
                    public void channelShunned() {
                        log.debug("channelShunned()");
                    }
                });
            
            log.debug("channel created");
            serverChannel.connect(DEFAULT_SERVER_GROUP_NAME);
            log.debug("channel connected");
            connState.setStopped();
            connManagementThread.start();
            log.debug("Connection Management Thread started");
            boolean getStateOK = serverChannel.getState(null, 0);
            log.debug("getState(): "+getStateOK);
        }
        catch(ChannelException e) {
            String msg = "Failed to create an active connection";
            log.error(msg, e);
            JMSException jmse = new JMSException(msg);
            jmse.setLinkedException(e);
            throw jmse;
        }
    }


    // TO_DO: deal with situation when this method is accessed concurrently from different threads
    void send(javax.jms.Message m) throws JMSException {

        try {
            // the Destination is already set for the message
            if (m.getJMSDestination() instanceof Topic) {
                // for topics, multicast
                serverChannel.send(null, null, (Serializable)m);
            }
            else {
                // for queues, unicast to the coordinator

                // TO_DO: optimization, if I am the only on in group, don't send the messages
                // down the stack anymore
                org.jgroups.Message jgmsg = 
                    new org.jgroups.Message((Address)serverChannel.getView().getMembers().get(0),
                                            null, new QueueCarrier(m));
                serverChannel.send(jgmsg);
            }
        }
        catch(Exception e) {
            String msg = "Failed to send message";
            log.error(msg, e);
            JMSException jmse = new JMSException(msg);
            jmse.setLinkedException(e);
            throw jmse;
        }

    }

    //
    // Runnable INTERFACE IMPLEMENTATION
    //

    /**
     * Code executed on the Connection Management Thread thread. It synchronously pulls JG
     * message and events from the channel. 
     **/
    public void run() {

        Object incoming = null;

        while(true) {

            try {
                incoming = serverChannel.receive(0);
            }
            catch(ChannelClosedException e) {
                log.debug("Channel closed, exiting");
                break;
            }
            catch(ChannelNotConnectedException e) {
                log.warn("TO_DO: Channel not connected, I should block the thread ...");
                continue;
            }
            catch(Exception e) {
                // TO_DO: use a JMS ExceptionListener and do some other things as well ....
                log.error("Failed to synchronously read from the channel", e);
            }

            try {
                dispatch(incoming);
            }
            catch(Exception e) {
                // TO_DO: I don't want that poorly written client code (dispatch() ends running
                // MessageListener code) to throw RuntimeException and terminate this thread
                // use the ExceptionListener and do some other things as well ....
                log.error("Dispatching failed", e);
            }
        }
    }

    //
    //
    //

    private void dispatch(Object o) throws Exception {

        log.debug("dispatching "+o);

        if (o instanceof SetStateEvent) {
            byte[] buffer = ((SetStateEvent)o).getArg();
            if (buffer == null) {
                // that's ok if I am the coordinator, just ignore it
                log.debug("null group state, ignoring ...");
            }
            else {
                // update my group state
                groupState.fromByteBuffer(buffer);
            }
            return;
        }
        else if (o instanceof GetStateEvent) {
            // somebody is requesting the group state
            serverChannel.returnState(groupState.toByteBuffer());
            return;
        }
        else if (o instanceof View) {
            // no use for it for the time being
            return;
        }
        else if (o instanceof SuspectEvent) {
            // no use for it for the time being
            return;
        }
        else if (!(o instanceof org.jgroups.Message)) {
            // ignore it for the time being
            log.warn("Ignoring "+o);
            return;
        }

        org.jgroups.Message jgmsg = (org.jgroups.Message)o;
        Object payload = jgmsg.getObject();
        if (payload instanceof ServerAdminCommand) {
            // ADD_QUEUE_RECEIVER, aso
            handleServerAdminCommand(jgmsg.getSrc(), (ServerAdminCommand)payload);
        }
        else if (payload instanceof QueueCarrier) {
            QueueCarrier qc = (QueueCarrier)payload;
            String sessionID = qc.getSessionID();
            // this is either an initial queue carrier that forwards the message from its
            // source to the coordinator, or a final queue carrier that forwards the message
            // from the coordinator to its final destination.
            if (sessionID == null) {
                queueForward(qc);
            }
            else {
                deliveryQueue.add(qc);
            }
        }
        else if (payload instanceof javax.jms.Message) { 
            // deliver only if the connection is started, discard otherwise
            if (connState.isStarted()) {
                deliveryQueue.add((javax.jms.Message)payload);
            }
        }
        else {
            log.warn("JG Message with a payload something else than a JMS Message: "+
                     (payload == null ? "null" : payload.getClass().getName()));
        }
    }

    private void handleServerAdminCommand(Address src, ServerAdminCommand c) {
        //log.debug("Handling "+c.getCommand());
        String comm = c.getCommand();
        if (ServerAdminCommand.ADD_QUEUE_RECEIVER.equals(comm)) {
            String queueName = (String)c.get(0);
            String sessionID = (String)c.get(1);
            String queueReceiverID = (String)c.get(2);
            groupState.addQueueReceiver(queueName, src, sessionID, queueReceiverID);
        }
        else if (ServerAdminCommand.REMOVE_QUEUE_RECEIVER.equals(comm)) {
            String queueName = (String)c.get(0);
            String sessionID = (String)c.get(1);
            String queueReceiverID = (String)c.get(2);
            groupState.removeQueueReceiver(queueName, src, sessionID, queueReceiverID);
        }
        else {
            log.error("Unknown server administration command: "+comm);
        }
    }

    void advertiseQueueReceiver(String queueName, String sessionID, 
                                String queueReceiverID, boolean isOn) throws ProviderException {

        try {
            // multicast the change, this will update my own state as well
            String cs = isOn ? 
                ServerAdminCommand.ADD_QUEUE_RECEIVER :
                ServerAdminCommand.REMOVE_QUEUE_RECEIVER;
            ServerAdminCommand comm = 
                new ServerAdminCommand(cs, queueName, sessionID, queueReceiverID);
            serverChannel.send(null, null, comm);
        }
        catch(ChannelException e) {
            throw new ProviderException("Failed to advertise the queue receiver", e);
        }
    }

    private void queueForward(QueueCarrier qc) throws Exception {

        Queue destQueue = (Queue)qc.getJMSMessage().getJMSDestination();
        QueueReceiverAddress ra = groupState.selectReceiver(destQueue.getQueueName());
        if (ra == null) {
            // TO_DO: no receivers for this queue, discard it for the time being
            log.warn("Discarding message for queue "+destQueue.getQueueName()+"!");
            return;
        }
        Address destAddress = ra.getAddress();
        qc.setSessionID(ra.getSessionID());
        qc.setReceiverID(ra.getReceiverID());
        
        // forward it to the final destination
        serverChannel.send(destAddress, null, qc);
        
    }

    //
    // Connection INTERFACE IMPLEMENTATION
    //

    public void start() throws JMSException {

        // makes sense to call it only a connection that is stopped. If called on a started 
        // connection, the call is ignored. If called on a closed connection: TO_DO
        // TO_DO: throw apropriate exceptions for illegal transitions
        if (connState.isStarted()) {
            return;
        }
        synchronized(connState) {
            connState.setStarted();
            connState.notify();
        }

    }

    public void stop() throws JMSException {

        // TO_DO: throw apropriate exceptions for illegal transitions
        connState.setStopped();
    }

    public void close() throws JMSException {

        // TO_DO: throw apropriate exceptions for illegal transitions
        // TO_DO: read the rest of specs and make sure I comply; tests
        if (connState.isClosed()) {
            return;
        }
        connState.setClosed();
        serverChannel.close();

    }

    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {

        return sessionManager.createSession(transacted, acknowledgeMode);

    }
    
    public String getClientID() throws JMSException {
        throw new NotImplementedException();
    }

    public void setClientID(String clientID) throws JMSException {

        // Once the connection has been initialized, the runtime provides a ClientID, that cannot
        // be changed by the user; according to JMS1.1 specs, the method should throw 
        // IllegalStateException
        String msg = "ClientID ("+""+") cannot be modified";
        throw new IllegalStateException(msg);
    }

    public ConnectionMetaData getMetaData() throws JMSException {
        throw new NotImplementedException();
    }

    public ExceptionListener getExceptionListener() throws JMSException {
        throw new NotImplementedException();
    }

    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        throw new NotImplementedException();
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination,
                                                       String messageSelector,
                                                       ServerSessionPool sessionPool,
                                                       int maxMessages)
        throws JMSException {
        throw new NotImplementedException();
    }


    public ConnectionConsumer createDurableConnectionConsumer(Topic topic,
                                                              String subscriptionName,
                                                              String messageSelector,
                                                              ServerSessionPool sessionPool,
                                                              int maxMessages)
        throws JMSException {
        throw new NotImplementedException();
    }

    //
    // END OF Connection INTERFACE IMPLEMENTATION
    //


    /**
     * Debugging only
     **/
    public static void main(String[] args) throws Exception {

        GroupConnection c = new GroupConnection(new URL(args[0]));
        c.connect();
    }



}
