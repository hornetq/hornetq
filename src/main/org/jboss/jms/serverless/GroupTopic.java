/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import javax.jms.Topic;
import javax.jms.JMSException;


/**
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
public class GroupTopic implements Topic {

    private static final Logger log = Logger.getLogger(GroupTopic.class);

    private String name;

    public GroupTopic(String name) {
        this.name = name;
    }

    public String getTopicName() throws JMSException {
        return name;
    }

    public String toString() {
        try {
            return Destinations.stringRepresentation(this);
        }
        catch(JMSException e) {
            return "Invalid GroupTopic";
        }
    }

    public boolean equals(Object o) {
       
        if (this == o) {
            return true;
        }
        if (!(o instanceof GroupTopic)) {
            return false;
        }

        GroupTopic that = (GroupTopic)o;

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
