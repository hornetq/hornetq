/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

/**
 * TO_DO: change the name to GroupStateChange ?
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class ServerAdminCommand implements Serializable {

    static final long serialVersionUID = 33480310721131848L;

    public static final String ADD_QUEUE_RECEIVER = "ADD_QUEUE_RECEIVER";
    public static final String REMOVE_QUEUE_RECEIVER = "REMOVE_QUEUE_RECEIVER";

    private String comm;
    private List args;

    public ServerAdminCommand(String comm, List args) {
        this.comm = comm;
        this.args = args;
    }

    public ServerAdminCommand(String comm, String arg1, String arg2, String arg3) {
        this(comm, new ArrayList());
        args.add(arg1);
        args.add(arg2);
        args.add(arg3);
    }

    public String getCommand() {
        return comm;
    }

    public Object get(int i) {
        return args.get(i);
    }

}
