/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless.client;


import org.jboss.logging.Logger;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.jms.Session;
import java.util.Map;
import java.util.HashMap;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.Set;

/**
 * An interactive JMS client. Run it with org.clester.Main.
 *
 * @see org.clester.Main
 *
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 **/
public class Interactive {

    private static final Logger log = Logger.getLogger(Interactive.class);

    // the JNDI runtime

    private Context initialContext;

    //
    // the JMS runtime - a snapshot of it can be obtained running runtime()
    //

    private Map connectionFactories; // (JNDI Name - ConnectionFactory instance)
    private Map destinations; // (JNDI Name - Destination instance)
    private List connectionHolders; // List of ConnectionHolder instances

    //
    // end of the JMS runtime
    //

    public Interactive() throws Exception {

        connectionFactories = new HashMap();
        destinations = new HashMap();
        connectionHolders = new ArrayList();
        initJNDI();
    }

    //
    // Command line interface
    //

    public void exit() {

        // close all connections
        int exitValue = 0;
        for(Iterator i = connectionHolders.iterator(); i.hasNext(); ) {
            Connection c = ((ConnectionHolder)i.next()).getConnection();
            try {
                c.close();
            }
            catch(Exception e) {
                exitValue ++;
                log.warn("Trouble closing connection "+c, e);
            }
        }
        System.exit(exitValue);
    }

    /**
     * Displays a snapshot of the JMS runtime
     **/
    public void runtime() throws Exception {

        System.out.println();
        System.out.println("JMS Runtime: ");
        System.out.println();

        System.out.print("Connection Factories: ");
        if (connectionFactories.size() == 0) {
            System.out.println("No Known ConnectionFactories");
        }
        else {
            System.out.println();
            //System.out.println(connectionFactory.toString()+" ("+connectionFactoryJNDIName+")");

            for(Iterator i = connectionFactories.keySet().iterator(); i.hasNext(); ) {
                String jndiName = (String)i.next();
                ConnectionFactory cf = (ConnectionFactory)connectionFactories.get(jndiName);
                System.out.println("\t"+jndiName+" - "+cf);
            }
        }
        System.out.print("Destinations: ");
        if (destinations.size() == 0) {
            System.out.println("No Known Destinations");
        }
        else {
            System.out.println();
            for(Iterator i = destinations.keySet().iterator(); i.hasNext(); ) {
                String jndiName = (String)i.next();
                Destination d = (Destination)destinations.get(jndiName);
                System.out.println("\t"+jndiName+" - "+d.getClass().getName());
            }
        }
        System.out.println();
        System.out.print("Connections");
        if (connectionHolders.size() == 0) {
            System.out.println(": No Active Connections");
        }
        else {
            System.out.println(": ");
            int idx = 0;
            for(Iterator ci = connectionHolders.iterator(); ci.hasNext(); idx++) {
                ConnectionHolder ch = (ConnectionHolder)ci.next();
                Connection c = ch.getConnection();
                ConnectionFactory cf = ch.getConnectionFactory();
                String cfJNDIName = getConnectionFactoryJNDIName(cf);
                List sessionHolders = ch.getSessionHolders();
                System.out.println("\t" + idx + " " + c + " produced by '" + cfJNDIName + "'");
                System.out.print("\t\tSessions: ");
                if (sessionHolders.isEmpty()) {
                    System.out.println("No Active Sessions");
                }
                else {
                    System.out.println();
                    int sidx = 0;
                    for(Iterator i = sessionHolders.iterator(); i.hasNext(); sidx++) {
                        SessionHolder h = (SessionHolder)i.next();	
                        Session s = h.getSession();
                        System.out.println("\t\tSession "+idx+"."+sidx+" ("+
                                           transactedToString(s.getTransacted())+", "+
                                           acknowledgeModeToString(s.getAcknowledgeMode())+"): ");
                        List producers = h.getProducers();
                        if (producers.size() == 0) {
                            System.out.println("\t\t\tNo Producers");
                        }
                        else {
                            int pidx = 0;
                            for(Iterator j = producers.iterator(); j.hasNext(); pidx++) { 
                                MessageProducer p = (MessageProducer)j.next();
                                System.out.println("\t\t\tProducer "+idx+"."+sidx+"."+pidx+" for "+
                                                   getDestinationJNDIName(p.getDestination()));
                            }
                        }
                        List consumers = h.getConsumers();
                        if (consumers.size() == 0) {
                            System.out.println("\t\t\tNo Consumers");
                        }
                        else {
                            int cidx = 0;
                            for(Iterator j = consumers.iterator(); j.hasNext(); cidx++) { 
                                MessageConsumer mc = (MessageConsumer)j.next();
                                System.out.print("\t\t\tConsumer " +idx+"."+sidx+"."+cidx+" "+mc);
                                if (mc.getMessageListener() != null) {
                                    System.out.println(", MessageListener ON");
                                }
                                else {
                                    System.out.println(", MessageListener OFF");
                                }
                            }
                        }
                    }
                }
            }
        }
        System.out.println();
        System.out.println();
    }

    /**
     *
     **/
    public void lookupConnectionFactory(String name) throws Exception {

        ConnectionFactory cf  = (ConnectionFactory)initialContext.lookup(name);
        connectionFactories.put(name, cf);
    }

    /**
     * Performs a JNDI lookup for the specified destination, overwritting the local cache if
     * the destination is found.
     **/
    public void lookupDestination(String destinationJNDIName) throws Exception {

        Destination d  = (Destination)initialContext.lookup(destinationJNDIName);
        destinations.put(destinationJNDIName, d);
    }

    public void createConnection(String connectionFactoryJNDIName) throws Exception {

        lookupConnectionFactory(connectionFactoryJNDIName);
        ConnectionFactory cf = 
            (ConnectionFactory)connectionFactories.get(connectionFactoryJNDIName);
        Connection c = cf.createConnection();
        ConnectionHolder ch = new ConnectionHolder(c, cf, new ArrayList());
        connectionHolders.add(ch);
    }

    // Overloaded createConnection; works when there is only one ConnectionFactory in cache
    public void createConnection() throws Exception {

        Set names = connectionFactories.keySet();
        if (names.isEmpty()) {
            log.error("No ConnectionFactory has been looked up yet!");
            return;
        }
        if (names.size() > 1) {
            String msg = 
                "There is more than one ConnectionFactory available. Specify the JNDI name when "+
                "creating a connection";
            log.error(msg);
            return;
        }
        createConnection((String)(names.toArray()[0]));
    }


    public void start(int index) throws Exception {

        try {
            connectionOK(index);
        }
        catch(Exception e) {
            log.error(e.getMessage());
            return;
        }
        Connection c = ((ConnectionHolder)connectionHolders.get(index)).getConnection();
        c.start();
    }

    // Overloaded method; work when there is only one connection
    public void start() throws Exception {
        if (connectionHolders.size() == 0) {
            log.error("No Connection has been created yet.");
            return;
        }
        if (connectionHolders.size() > 1) {
            log.error("There are more than one active Connections. Use start(index).");
            return;
        }
        start(0);
    }


    public void stop(int index) throws Exception {

        try {
            connectionOK(index);
        }
        catch(Exception e) {
            log.error(e.getMessage());
            return;
        }
        Connection c = ((ConnectionHolder)connectionHolders.get(index)).getConnection();
        c.stop();
    }

    public void close(int index) throws Exception {

        try {
            connectionOK(index);
        }
        catch(Exception e) {
            log.error(e.getMessage());
            return;
        }
        ConnectionHolder ch = (ConnectionHolder)connectionHolders.get(index);
        Connection c = ch.getConnection();
        c.close();
        ch.destroy();
        connectionHolders.remove(index);
        
    }

    



    /**
     * Creates a session using the active connection.
     *
     * @param index - the index of the Connection this Session will be created on.
     * @param transacted - a boolean indicating whether the session to be created is 
     *        transacted or not.
     * @param acknowledgeModeString  - The string representation of the acknowledgement mode for 
     *        the session to be created. One of "AUTO_ACKNOWLEDGE", "CLIENT_ACKNOWLEDGE", 
     *        "DUPS_OK_ACKNOWLEDGE".
     **/
    public void createSession(int index, boolean transacted, String acknowledgeModeString) 
        throws Exception {

        try {
            connectionOK(index);
        }
        catch(Exception e) {
            log.error(e.getMessage());
            return;
        }

        int acknowledgeMode = -1;

        try {
           acknowledgeMode = parseAcknowledgeModeString(acknowledgeModeString);
        }
        catch(Exception e) {
            // an error message has already been displayed
            return;
        }

        ConnectionHolder ch = (ConnectionHolder)connectionHolders.get(index);
        List sessionHolders = ch.getSessionHolders();
        Session s = ch.getConnection().createSession(transacted, acknowledgeMode);
        sessionHolders.add(new SessionHolder(s, new ArrayList(), new ArrayList()));
        
    } 

    /**
     * Creates a Producer associated with the session whose index is specified as the first
     * parameter. 
     *
     * @param sessionID - A "<connection_index>.<session_index>" string.
     **/
    public void createProducer(String sessionID, String destinationJNDIName) throws Exception {

        int[] indices = parseCompositeID2(sessionID);
        int connIdx = indices[0];
        int sessionIdx = indices[1];

        try {
            connectionOK(connIdx);
        }
        catch(Exception e) {
            log.error(e.getMessage());
            return;
        }

        List sessionHolders = 
            ((ConnectionHolder)connectionHolders.get(connIdx)).getSessionHolders();

        if (sessionIdx >= sessionHolders.size()) {
            String msg = 
                "There is no Session with the index "+sessionIdx+". Currently there are "+
                sessionHolders.size()+" active Sessions for this Connection.";
            log.error(msg);
            return;
        }

        SessionHolder h = (SessionHolder)sessionHolders.get(sessionIdx);
        Session s = h.getSession();
        Destination d = getDestination(destinationJNDIName);
        MessageProducer p = s.createProducer(d);
        h.getProducers().add(p);
    }

    /**
     * Creates a Consumer associated with the session whose index is specified as the first 
     * parameter.
     *
     * @param sessionID - A "<connection_index>.<session_index>" string.
     **/
    public void createConsumer(String sessionID, String destinationJNDIName) throws Exception {

        int[] indices = parseCompositeID2(sessionID);
        int connIdx = indices[0];
        int sessionIdx = indices[1];

        try {
            connectionOK(connIdx);
        }
        catch(Exception e) {
            log.error(e.getMessage());
            return;
        }

        List sessionHolders =
            ((ConnectionHolder)connectionHolders.get(connIdx)).getSessionHolders();

        if (sessionIdx >= sessionHolders.size()) {
            String msg = 
                "There is no Session with the index "+sessionIdx+". Currently there are "+
                sessionHolders.size()+" active Sessions for this Connection.";
            log.error(msg);
            return;
        }

        SessionHolder h = (SessionHolder)sessionHolders.get(sessionIdx);
        Session s = h.getSession();
        Destination d = getDestination(destinationJNDIName);
        MessageConsumer c = s.createConsumer(d);
        h.getConsumers().add(c);
    }

    /**
     * Equivalent with calling JMS API method close() on the consumer instance.
     * 
     * @param consumerID - A "<connection_index>.<session_index>.<consumer_index>" string.
     **/
    public void closeConsumer(String consumerID) throws Exception {

        MessageConsumer c = null;
        try {
            c = (MessageConsumer)getSessionChild(consumerID, false);
        }
        catch(Exception e) {
            log.error(e.getMessage());
            return;
        }
        c.close();
        getSessionHolder(consumerID).getConsumers().remove(c);
    }


    /**
     * Attaches a message listener to the specified consumer, possibly replacing the current one.
     * The message listener just displays the string representation of the messages it receives.
     * 
     * @param consumerID - A "<connection_index>.<session_index>.<consumer_index>" string.
     **/
    public void setMessageListener(String consumerID) throws Exception {

        MessageConsumer c = null;
        try {
            c = (MessageConsumer)getSessionChild(consumerID, false);
        }
        catch(Exception e) {
            log.error(e.getMessage());
            return;
        }

        // The listener keeps a reference to its consumer, for reporting purposes; please note
        // that is very likely the IDs will change dynamically during the life of the client
        final MessageConsumer myConsumer = c;
        c.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    try {
                        String myConsumersID = getSessionChildID(myConsumer);
                        String output = "Consumer "+myConsumersID+": ";
                        if (message instanceof TextMessage) {
                            output += ((TextMessage)message).getText();
                        }
                        else {
                            output += message.toString();
                        }
                        System.out.println(output);
                    }
                    catch(Exception e) {
                        log.error("Failed to process message", e);
                    }
                }
            });
    }


    /**
     * @param producerID - A "<connection_index>.<session_index>.<consumer_index>" string.
     **/
    public void send(String producerID, String payload) throws Exception {

        TextMessage tm = getSession(producerID).createTextMessage();
        tm.setText(payload);
        MessageProducer p = (MessageProducer)getSessionChild(producerID, true);
        p.send(tm);
    }

    //
    // EXPERIMENTAL METHODS
    //

    /**
     * The method creates a "bridge" between a consumer and a producer: Every message received
     * by the consumer is automatically forwarded to the producer and sent on the producer's 
     * destination.
     **/
    public void forward(String consumerID, String producerID) throws Exception {
        
        final MessageConsumer c = (MessageConsumer)getSessionChild(consumerID, false);
        final MessageProducer p = (MessageProducer)getSessionChild(producerID, true);
        MessageListener l = new MessageListener() {
                public void onMessage(Message message) {
                    try {
                        String consumerID = getSessionChildID(c);
                        String producerID = getSessionChildID(p);
                        p.send(message);
                        String msg = 
                            "Consumer "+consumerID+" forwarded message to producer "+producerID;
                        System.out.println(msg);
                    }
                    catch(Exception e) {
                        log.error("Failed to process message", e);
                    }
                }
            };
        c.setMessageListener(l);
    }

    //
    // End of command line interface
    //


    //
    // PRIVATE METHODS - not exercisable by Tester
    //

    private void initJNDI() throws Exception {

        initialContext = new InitialContext();
    }

    /**
     * In case the connection is not OK, throws an exception with a displayable message.
     **/
    private void connectionOK(int index) throws Exception {

        int size = connectionHolders.size();
        String msg = null;
        if (size == 0) {
            msg = "No active Connection created yet!";
        }
        else if (index < 0 || index >= size) {
            msg =
                "No such Connection index. Valid indexes are 0"+
                (size == 0 ? "":" ... "+(size - 1))+".";
        }
        
        if (msg != null) {
            throw new Exception(msg);
        }
    }


    private int parseAcknowledgeModeString(String s) throws Exception {
        
        s = s.toUpperCase();
        if ("AUTO_ACKNOWLEDGE".equals(s)) {
            return Session.AUTO_ACKNOWLEDGE;
        }
        else if ("CLIENT_ACKNOWLEDGE".equals(s)) {
            return Session.CLIENT_ACKNOWLEDGE;
        }
        else if ("DUPS_OK_ACKNOWLEDGE".equals(s)) {
            return Session.DUPS_OK_ACKNOWLEDGE;
        }
        else {
            log.error("Unknow session acknowledment type: "+s);
            throw new Exception();
        }
    }


    private String acknowledgeModeToString(int a) {
        if (a == Session.AUTO_ACKNOWLEDGE) {
            return "AUTO_ACKNOWLEDGE";
        }
        else if (a == Session.CLIENT_ACKNOWLEDGE) {
            return "CLIENT_ACKNOWLEDGE";
        }
        else if (a == Session.DUPS_OK_ACKNOWLEDGE) {
            return "DUPS_OK_ACKNOWLEDGE";
        }
        else {
            return "UNKNOWN_ACKNOWLEDGE_TYPE";
        }
    }


    private String transactedToString(boolean t) {
        if (t) {
            return "TRANSACTED";
        }
        return "NON TRANSACTED";
    }



    /**
     * Tries to get the destination from the local cache. If the destination is not cached,
     * it is looked up for in JNDI and the cache is updated.
     **/
    private Destination getDestination(String destinationJNDIName) throws Exception {

        Destination d = (Destination)destinations.get(destinationJNDIName);
        if (d == null) {
            lookupDestination(destinationJNDIName);
            d = (Destination)destinations.get(destinationJNDIName);
        }
        return d;
    }



    /**
     * Returns the JNDI name this destination was found under. If no such destination is found
     * in the local cache, the method returns null
     **/
    private String getDestinationJNDIName(Destination d) throws Exception {
        
        for(Iterator i = destinations.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if (d.equals(destinations.get(name))) {
                return name;
            }
        }
        return null;
    }

    /**
     * Returns the JNDI name this ConnectionFactory was found under. If no such factory is found
     * in the local cache, the method returns null.
     **/
    private String getConnectionFactoryJNDIName(ConnectionFactory cf) throws Exception {
        for(Iterator i = connectionFactories.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if (cf.equals(connectionFactories.get(name))) {
                return name;
            }
        }
        return null;
    }



    /**
     * Parses a two-component string ID. Expects an "int1.int2"-formatted string. Throws an 
     * exception with a displayable message in case of invalid format.
     * @return int[2]
     **/
    private int[] parseCompositeID2(String compositeID) throws Exception {

        try {
            int first, last;
            int i = compositeID.indexOf('.');
            first = Integer.parseInt(compositeID.substring(0, i));
            last = Integer.parseInt(compositeID.substring(i+1));
            return new int[] { first, last };
        }
        catch(Exception e) {
            String msg = "Invalid ID format: "+compositeID;
            throw new Exception(msg);
        }
    }

    /**
     * Parses a three-component string ID. Expects an "int1.int2.int3"-formatted string. Throws an 
     * exception with a displayable message in case of invalid format.
     * @return int[3]
     **/
    private int[] parseCompositeID3(String compositeID) throws Exception {

        try {
            int i1;
            int i = compositeID.indexOf('.');
            i1 = Integer.parseInt(compositeID.substring(0, i));
            int[] c = parseCompositeID2(compositeID.substring(i+1));
            return new int[] { i1, c[0], c[1] };
        }
        catch(Exception e) {
            String msg = "Invalid ID format: "+compositeID;
            throw new Exception(msg);
        }
    }


    /**
     * Throws an exception with a displayable message in case of invalid format or in case the 
     * indices are invalid for the current configuration. 
     *
     * @param compositeID - A "<conection_index>.<session_index>.<consumer_index>" string.
     **/
    private SessionHolder getSessionHolder(String compositeID) throws Exception {

        int[] indices = parseCompositeID3(compositeID);
        int connIdx = indices[0];
        int sessionIdx = indices[1];

        connectionOK(connIdx);
        
        List sHolders = ((ConnectionHolder)connectionHolders.get(connIdx)).getSessionHolders();
        if (sessionIdx < 0 || sessionIdx >= sHolders.size()) {
            String msg = "Invalid Session index: "+sessionIdx;
            throw new Exception(msg);
        }
        return (SessionHolder)sHolders.get(sessionIdx);
    }


    /**
     * Throws an exception with a displayable message in case of invalid format or in case the 
     * indices are invalid for the current configuration. 
     *
     * @param compositeID - A "<connection_index>.<session_index>.<consumer_index>" string.
     **/
    private Session getSession(String compositeID) throws Exception {

        return getSessionHolder(compositeID).getSession();
    }

    /**
     * Throws an exception with a displayable message in case of invalid format or in case the 
     * indices are invalid for the current configuration.
     *
     * @param compositeID - A "<connection_index>.<session_index>.<consumer_index>" string.
     * @param isProducer - true if the ID represents a producer, false for a consumer.
     *
     * @return a MessageProducer or a MessageConsumer
     **/
    private Object getSessionChild(String compositeID, boolean isProducer) throws Exception {

        SessionHolder h = getSessionHolder(compositeID);
        int[] indices = parseCompositeID3(compositeID);
        int childIdx = indices[2];
        List l = isProducer ? h.getProducers() : h.getConsumers();
        if (childIdx < 0 || childIdx >= l.size()) {
            String msg = "Invalid "+(isProducer?"producer":"consumer")+" index: "+childIdx;
            throw new Exception(msg);
        }
        return l.get(childIdx);
    }

    /**
     * @return null if not found
     **/
    private String getSessionChildID(Object sessionChild) {

        String id = null;
        int cidx = 0;
        for(Iterator ci = connectionHolders.iterator(); ci.hasNext(); cidx++) {
            List sh = ((ConnectionHolder)ci.next()).getSessionHolders();
            int sidx = 0;
            for(Iterator i = sh.iterator(); i.hasNext(); sidx++) {
                SessionHolder h = (SessionHolder)i.next();
                int idx = h.getIndex(sessionChild);
                if (idx == -1) {
                    continue;
                }
                return 
                    Integer.toString(cidx)+"."+Integer.toString(sidx)+"."+Integer.toString(idx);
            }
        }
        return id;
    }

    //
    //
    //

    /**
     * A binder for a Connection and its Sessions.
     **/
    private class ConnectionHolder {

        private Connection c;
        private ConnectionFactory cf;
        private List sessionHolders;
        
        public ConnectionHolder(Connection c, ConnectionFactory cf, List sessionHolders) {
            
            this.c = c;
            this.cf = cf;
            this.sessionHolders = sessionHolders;
        }

        public Connection getConnection() {
            return c;
        }

        public ConnectionFactory getConnectionFactory() {
            return cf;
        }

        /**
         * Returns the backing storage itself, not a clone.
         **/
        public List getSessionHolders() {
            return sessionHolders;
        }

        /**
         * It does not JMS-close the Connection or Sessions, it only tears down the client-level
         * fixtures. To properly close the JMS objects, use their own close() methods.
         **/
        public void destroy() {
            c = null;
            cf = null;
            for(Iterator i = sessionHolders.iterator(); i.hasNext(); ) {
                SessionHolder h = (SessionHolder)i.next();
                h.destroy();
            }
            sessionHolders.clear();
            sessionHolders = null;
        }
    }


    /**
     * A binder for a Session and its Producers and Consumer lists.
     **/
    private class SessionHolder {

        private Session s;
        private List producers; // List of MessageConsumer instances
        private List consumers; // List of MessageProducer instances
        
        public SessionHolder(Session s, List producers, List consumers) {
            
            this.s = s;
            this.producers = producers;
            this.consumers = consumers;
        }

        public Session getSession() {
            return s;
        }

        /**
         * Returns the backing storage itself, not a clone.
         **/
        public List getProducers() {
            return producers;
        }

        /**
         * Returns the backing storage itself, not a clone.
         **/
        public List getConsumers() {
            return consumers;
        }

        /**
         * Returns the backing storage itself, not a clone. Returns null if likeThis is not a
         * MessageProducer or a MessageConsumer.
         **/
        public List getChildren(Object likeThis) {
            if (likeThis instanceof MessageProducer) {
                return producers;
            }
            else if (likeThis instanceof MessageConsumer) {
                return consumers;
            }
            return null;
        }

        /**
         * @return the index of the child, if it is a children of the associated session, or -1
         *         otherwise.
         **/
        public int getIndex(Object sessionChild) {

            List l = getChildren(sessionChild);
            if (l == null) {
                // something wrong with sessionChild, bail out
                return -1;
            }
            return l.indexOf(sessionChild);
        }

        /**
         * It does not JMS-close the Sessions or their children, it only tears down the 
         * client-level fixtures. To properly close the JMS objects, use their own close() methods.
         **/
        public void destroy() {
            s = null;
            producers.clear();
            consumers.clear();
            producers = null;
            consumers = null;
        }


    }

    //
    //
    //

//     public void t() {
//         System.out.println("JChannel.TEST_VARIABLE = "+org.jgroups.JChannel.TEST_VARIABLE);
//     }

        

}



