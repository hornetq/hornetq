/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import javax.jms.Queue;
import javax.jms.JMSException;


/**
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
public class GroupQueue implements Queue {

    private static final Logger log = Logger.getLogger(GroupQueue.class);

    private String name;

    public GroupQueue(String name) {
        this.name = name;
    }

    public String getQueueName() throws JMSException {
        return name;
    }

    public String toString() {
        try {
            return Destinations.stringRepresentation(this);
        }
        catch(JMSException e) {
            return "Invalid GroupQueue";
        }
    }

    public boolean equals(Object o) {
       
        if (this == o) {
            return true;
        }
        if (!(o instanceof GroupQueue)) {
            return false;
        }

        GroupQueue that = (GroupQueue)o;

        if (name == null) {
            return false;
        }
        return name.equals(that.name);
    }

    public int hashCode() {

        // TO_DO: review this

        if (name == null) {
            return 0;
        }
        return name.hashCode();
    }

}
