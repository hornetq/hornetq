package org.jboss.messaging.util;


/**
 * allows objects to be mapped against a regex pattern and held in order in a list
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface HierarchicalRepository<E>
{
   /**
    * Add a new match to the repository
    * @param match The regex to use to match against
    * @param value the value to hold agains the match
    */
    void addMatch(String match, E value);

   /**
    * return the value held against the nearest match
    * @param match the match to look for
    * @return the value
    */
   E getMatch(String match);

   /**
    * set the default value to fallback to if none found
    * @param defaultValue the value
    */
   void setDefault(E defaultValue);

   /**
    * remove a match from the repository
    * @param match the match to remove
    */
   void removeMatch(String match);
}
