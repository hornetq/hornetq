/*
 * Written by Doug Lea and Josh Bloch with assistance from members of
 * JCP JSR-166 Expert Group and released to the public domain, as explained
 * at http://creativecommons.org/licenses/publicdomain
 */

package org.hornetq.utils.concurrent; // XXX This belongs in java.util!!! XXX

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Stack;

/**
 * A linear collection that supports element insertion and removal at
 * both ends.  The name <i>deque</i> is short for "double ended queue"
 * and is usually pronounced "deck".  Most <tt>Deque</tt>
 * implementations place no fixed limits on the number of elements
 * they may contain, but this interface supports capacity-restricted
 * deques as well as those with no fixed size limit.
 *
 * <p>This interface defines methods to access the elements at both
 * ends of the deque.  Methods are provided to insert, remove, and
 * examine the element.  Each of these methods exists in two forms:
 * one throws an exception if the operation fails, the other returns a
 * special value (either <tt>null</tt> or <tt>false</tt>, depending on
 * the operation).  The latter form of the insert operation is
 * designed specifically for use with capacity-restricted
 * <tt>Deque</tt> implementations; in most implementations, insert
 * operations cannot fail.
 *
 * <p>The twelve methods described above are are summarized in the 
 * follwoing table:<p>
 * 
 * <table BORDER CELLPADDING=3 CELLSPACING=1>
 *  <tr>
 *    <td></td>
 *    <td ALIGN=CENTER COLSPAN = 2> <b>First Element (Head)</b></td>
 *    <td ALIGN=CENTER COLSPAN = 2> <b>Last Element (Tail)</b></td>
 *  </tr>
 *  <tr>
 *    <td></td>
 *    <td ALIGN=CENTER><em>Throws exception</em></td>
 *    <td ALIGN=CENTER><em>Returns special value</em></td>
 *    <td ALIGN=CENTER><em>Throws exception</em></td>
 *    <td ALIGN=CENTER><em>Returns special value</em></td>
 *  </tr>
 *  <tr>
 *    <td><b>Insert</b></td>
 *    <td>{@link #addFirst addFirst(e)}</td>
 *    <td>{@link #offerFirst offerFirst(e)}</td>
 *    <td>{@link #addLast addLast(e)}</td>
 *    <td>{@link #offerLast offerLast(e)}</td>
 *  </tr>
 *  <tr>
 *    <td><b>Remove</b></td>
 *    <td>{@link #removeFirst removeFirst()}</td>
 *    <td>{@link #pollFirst pollFirst()}</td>
 *    <td>{@link #removeLast removeLast()}</td>
 *    <td>{@link #pollLast pollLast()}</td>
 *  </tr>
 *  <tr>
 *    <td><b>Examine</b></td>
 *    <td>{@link #getFirst getFirst()}</td>
 *    <td>{@link #peekFirst peekFirst()}</td>
 *    <td>{@link #getLast getLast()}</td>
 *    <td>{@link #peekLast peekLast()}</td>
 *  </tr>
 * </table>
 *
 * <p>This interface extends the {@link Queue} interface.  When a deque is
 * used as a queue, FIFO (First-In-First-Out) behavior results.  Elements are
 * added to the end of the deque and removed from the beginning.  The methods
 * inherited from the <tt>Queue</tt> interface are precisely equivalent to
 * <tt>Deque</tt> methods as indicated in the following table:<p>
 *
 * <table BORDER CELLPADDING=3 CELLSPACING=1>
 *  <tr>
 *    <td ALIGN=CENTER> <b><tt>Queue</tt> Method</b></td>
 *    <td ALIGN=CENTER> <b>Equivalent <tt>Deque</tt> Method</b></td>
 *  </tr>
 *  <tr>
 *   <tr>
 *    <td>{@link java.util.Queue#offer offer(e)}</td>
 *    <td>{@link #offerLast offerLast(e)}</td>
 *   </tr>
 *   <tr>
 *    <td>{@link java.util.Queue#add add(e)}</td>
 *    <td>{@link #addLast addLast(e)}</td>
 *   </tr>
 *   <tr>
 *    <td>{@link java.util.Queue#poll poll()}</td>
 *    <td>{@link #pollFirst pollFirst()}</td>
 *   </tr>
 *   <tr>
 *    <td>{@link java.util.Queue#remove remove()}</td>
 *    <td>{@link #removeFirst removeFirst()}</td>
 *   </tr>
 *   <tr>
 *    <td>{@link java.util.Queue#peek peek()}</td>
 *    <td>{@link #peek peekFirst()}</td>
 *   </tr>
 *   <tr>
 *    <td>{@link java.util.Queue#element element()}</td>
 *    <td>{@link #getFirst getFirst()}</td>
 *   </tr>
 * </table>
 *
 * <p>Deques can also be used as LIFO (Last-In-First-Out) stacks.  This
 * interface should be used in preference to the legacy {@link Stack} class.
 * When a dequeue is used as a stack, elements are pushed and popped from the
 * beginning of the deque.  Stack methods are precisely equivalent to
 * <tt>Deque</tt> methods as indicated in the table below:<p>
 *
 * <table BORDER CELLPADDING=3 CELLSPACING=1>
 *  <tr>
 *    <td ALIGN=CENTER> <b>Stack Method</b></td>
 *    <td ALIGN=CENTER> <b>Equivalent <tt>Deque</tt> Method</b></td>
 *  </tr>
 *  <tr>
 *   <tr>
 *    <td>{@link #push push(e)}</td>
 *    <td>{@link #addFirst addFirst(e)}</td>
 *   </tr>
 *   <tr>
 *    <td>{@link #pop pop()}</td>
 *    <td>{@link #removeFirst removeFirst()}</td>
 *   </tr>
 *   <tr>
 *    <td>{@link #peek peek()}</td>
 *    <td>{@link #peekFirst peekFirst()}</td>
 *   </tr>
 * </table>
 *
 * <p>Note that the {@link #peek peek} method works equally well when
 * a deque is used as a queue or a stack; in either case, elements are
 * drawn from the beginning of the deque.
 *
 * <p>This inteface provides two methods to to remove interior
 * elements, {@link #removeFirstOccurrence removeFirstOccurrence} and
 * {@link #removeLastOccurrence removeLastOccurrence}.  Unlike the
 * {@link List} interface, this interface does not provide support for
 * indexed access to elements.
 *
 * <p>While <tt>Deque</tt> implementations are not strictly required
 * to prohibit the insertion of null elements, they are strongly
 * encouraged to do so.  Users of any <tt>Deque</tt> implementations
 * that do allow null elements are strongly encouraged <i>not</i> to
 * take advantage of the ability to insert nulls.  This is so because
 * <tt>null</tt> is used as a special return value by various methods
 * to indicated that the deque is empty.
 * 
 * <p><tt>Deque</tt> implementations generally do not define
 * element-based versions of the <tt>equals</tt> and <tt>hashCode</tt>
 * methods, but instead inherit the identity-based versions from class
 * <tt>Object</tt>.
 *
 * <p>This interface is a member of the <a
 * href="{@docRoot}/../guide/collections/index.html"> Java Collections
 * Framework</a>.
 *
 * @author Doug Lea
 * @author Josh Bloch
 * @since  1.6
 * @param <E> the type of elements held in this collection
 */
public interface Deque<E> extends Queue<E>
{
   /**
    * Inserts the specified element to the front this deque unless it would
    * violate capacity restrictions.  When using a capacity-restricted deque,
    * this method is generally preferable to method <tt>addFirst</tt>, which
    * can fail to insert an element only by throwing an exception.
    *
    * @param e the element to insert
    * @return <tt>true</tt> if it was possible to insert the element,
    *     else <tt>false</tt>
    * @throws NullPointerException if <tt>e</tt> is null and this
    *     deque does not permit null elements
    */
   boolean offerFirst(E e);

   /**
    * Inserts the specified element to the end of this deque unless it would
    * violate capacity restrictions.  When using a capacity-restricted deque,
    * this method is generally preferable to method <tt>addLast</tt> which
    * can fail to insert an element only by throwing an exception.
    *
    * @param e the element to insert
    * @return <tt>true</tt> if it was possible to insert the element,
    *     else <tt>false</tt>
    * @throws NullPointerException if <tt>e</tt> is null and this
    *     deque does not permit null elements
    */
   boolean offerLast(E e);

   /**
    * Inserts the specified element to the front of this deque unless it
    * would violate capacity restrictions.
    *
    * @param e the element to insert
    * @throws IllegalStateException if it was not possible to insert
    *    the element due to capacity restrictions
    * @throws NullPointerException if <tt>e</tt> is null and this
    *     deque does not permit null elements
    */
   void addFirst(E e);

   /**
    * Inserts the specified element to the end of this deque unless it would
    * violate capacity restrictions.
    *
    * @param e the element to insert
    * @throws IllegalStateException if it was not possible to insert
    *    the element due to capacity restrictions
    * @throws NullPointerException if <tt>e</tt> is null and this
    *     deque does not permit null elements
    */
   void addLast(E e);

   /**
    * Retrieves and removes the first element of this deque, or
    * <tt>null</tt> if this deque is empty.
    *
    * @return the first element of this deque, or <tt>null</tt> if
    *     this deque is empty
    */
   E pollFirst();

   /**
    * Retrieves and removes the last element of this deque, or
    * <tt>null</tt> if this deque is empty.
    *
    * @return the last element of this deque, or <tt>null</tt> if
    *     this deque is empty
    */
   E pollLast();

   /**
    * Removes and returns the first element of this deque.  This method
    * differs from the <tt>pollFirst</tt> method only in that it throws an
    * exception if this deque is empty.
    *
    * @return the first element of this deque
    * @throws NoSuchElementException if this deque is empty
    */
   E removeFirst();

   /**
    * Retrieves and removes the last element of this deque.  This method
    * differs from the <tt>pollLast</tt> method only in that it throws an
    * exception if this deque is empty.
    *
    * @return the last element of this deque
    * @throws NoSuchElementException if this deque is empty
    */
   E removeLast();

   /**
    * Retrieves, but does not remove, the first element of this deque,
    * returning <tt>null</tt> if this deque is empty.
    *
    * @return the first element of this deque, or <tt>null</tt> if
    *     this deque is empty
    */
   E peekFirst();

   /**
    * Retrieves, but does not remove, the last element of this deque,
    * returning <tt>null</tt> if this deque is empty.
    *
    * @return the last element of this deque, or <tt>null</tt> if this deque
    *     is empty
    */
   E peekLast();

   /**
    * Retrieves, but does not remove, the first element of this
    * deque.  This method differs from the <tt>peek</tt> method only
    * in that it throws an exception if this deque is empty.
    *
    * @return the first element of this deque
    * @throws NoSuchElementException if this deque is empty
    */
   E getFirst();

   /**
    * Retrieves, but does not remove, the last element of this
    * deque.  This method differs from the <tt>peek</tt> method only
    * in that it throws an exception if this deque is empty.
    *
    * @return the last element of this deque
    * @throws NoSuchElementException if this deque is empty
    */
   E getLast();

   /**
    * Removes the first occurrence of the specified element in this
    * deque.  If the deque does not contain the element, it is
    * unchanged.  More formally, removes the first element <tt>e</tt>
    * such that <tt>(o==null ? e==null : o.equals(e))</tt> (if
    * such an element exists).
    *
    * @param e element to be removed from this deque, if present
    * @return <tt>true</tt> if the deque contained the specified element
    * @throws NullPointerException if the specified element is <tt>null</tt>
    */
   boolean removeFirstOccurrence(Object e);

   /**
    * Removes the last occurrence of the specified element in this
    * deque.  If the deque does not contain the element, it is
    * unchanged.  More formally, removes the last element <tt>e</tt>
    * such that <tt>(o==null ? e==null : o.equals(e))</tt> (if
    * such an element exists).
    *
    * @param e element to be removed from this deque, if present
    * @return <tt>true</tt> if the deque contained the specified element
    * @throws NullPointerException if the specified element is <tt>null</tt>
    */
   boolean removeLastOccurrence(Object e);

   // *** Queue methods ***

   /**
    * Inserts the specified element into the queue represented by this deque
    * unless it would violate capacity restrictions.  In other words, inserts
    * the specified element to the end of this deque.  When using a
    * capacity-restricted deque, this method is generally preferable to the
    * {@link #add} method, which can fail to insert an element only by
    * throwing an exception.
    *
    * <p>This method is equivalent to {@link #offerLast}.
    *
    * @param e the element to insert
    * @return <tt>true</tt> if it was possible to insert the element,
    *     else <tt>false</tt>
    * @throws NullPointerException if <tt>e</tt> is null and this
    *     deque does not permit null elements
    */
   boolean offer(E e);

   /**
    * Inserts the specified element into the queue represented by this
    * deque unless it would violate capacity restrictions.  In other words,
    * inserts the specified element as the last element of this deque. 
    *
    * <p>This method is equivalent to {@link #addLast}.
    *
    * @param e the element to insert
    * @return <tt>true</tt> (as per the spec for {@link Collection#add})
    * @throws IllegalStateException if it was not possible to insert
    *    the element due to capacity restrictions
    * @throws NullPointerException if <tt>e</tt> is null and this
    *     deque does not permit null elements
    */
   boolean add(E e);

   /**
    * Retrieves and removes the head of the queue represented by
    * this deque, or <tt>null</tt> if this deque is empty.  In other words,
    * retrieves and removes the first element of this deque, or <tt>null</tt>
    * if this deque is empty.
    *
    * <p>This method is equivalent to {@link #pollFirst()}.
    *
    * @return the first element of this deque, or <tt>null</tt> if
    *     this deque is empty
    */
   E poll();

   /**
    * Retrieves and removes the head of the queue represented by this deque.
    * This method differs from the <tt>poll</tt> method only in that it
    * throws an exception if this deque is empty.
    *
    * <p>This method is equivalent to {@link #removeFirst()}.
    *
    * @return the head of the queue represented by this deque
    * @throws NoSuchElementException if this deque is empty
    */
   E remove();

   /**
    * Retrieves, but does not remove, the head of the queue represented by
    * this deque, returning <tt>null</tt> if this deque is empty.
    *
    * <p>This method is equivalent to {@link #peekFirst()}
    *
    * @return the head of the queue represented by this deque, or
    *     <tt>null</tt> if this deque is empty
    */
   E peek();

   /**
    * Retrieves, but does not remove, the head of the queue represented by
    * this deque.  This method differs from the <tt>peek</tt> method only in
    * that it throws an exception if this deque is empty.
    *
    * <p>This method is equivalent to {@link #getFirst()}
    *
    * @return the head of the queue represented by this deque
    * @throws NoSuchElementException if this deque is empty
    */
   E element();

   // *** Stack methods ***

   /**
    * Pushes an element onto the stack represented by this deque.  In other
    * words, inserts the element to the front this deque unless it would
    * violate capacity restrictions.
    *
    * <p>This method is equivalent to {@link #addFirst}.
    *
    * @throws IllegalStateException if it was not possible to insert
    *    the element due to capacity restrictions
    * @throws NullPointerException if <tt>e</tt> is null and this
    *     deque does not permit null elements
    */
   void push(E e);

   /**
    * Pops an element from the stack represented by this deque.  In other
    * words, removes and returns the the first element of this deque.
    *
    * <p>This method is equivalent to {@link #removeFirst()}.
    *
    * @return the element at the front of this deque (which is the top
    *     of the stack represented by this deque)
    * @throws NoSuchElementException if this deque is empty
    */
   E pop();

   // *** Collection Method ***

   /**
    * Returns an iterator over the elements in this deque.  The elements
    * will be ordered from first (head) to last (tail).
    * 
    * @return an <tt>Iterator</tt> over the elements in this deque
    */
   Iterator<E> iterator();
}
