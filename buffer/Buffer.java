package simpledb.buffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.*;

import simpledb.file.*;
import simpledb.log.LogMgr;

/**
 * An individual buffer. A databuffer wraps a page 
 * and stores information about its status,
 * such as the associated disk block,
 * the number of times the buffer has been pinned,
 * whether its contents have been modified,
 * and if so, the id and lsn of the modifying transaction.
 * @author Edward Sciore
 */
public class Buffer {
   public FileMgr fm;
   public LogMgr lm;
   private Page contents;
   private BlockId blk = null;
   private int pins = 0;
   private int txnum = -1;
   private int lsn = -1;
   public int buffer_id;
   static ArrayList<Integer> lsnList = new ArrayList<Integer>();
   static LinkedHashMap<Integer, Integer> block_lsn = new LinkedHashMap<Integer, Integer>();

   public Buffer(FileMgr fm, LogMgr lm, int buffer_id) {
      this.fm = fm;
      this.lm = lm;
      this.buffer_id = buffer_id;
      contents = new Page(fm.blockSize());
   }
   
   public int getId()
   {
      return buffer_id;
   }

   public Page contents() {
      return contents;
   }

   /**
    * Returns a reference to the disk block
    * allocated to the buffer.
    * @return a reference to a disk block
    */
   public BlockId block() {
      return blk;
   }

   public void setModified(int txnum, int lsn) {
      this.txnum = txnum;
      if (lsn >= 0)
         this.lsn = lsn;
      if(block_lsn.containsKey(getId()))
      {
         block_lsn.remove(getId());
         block_lsn.put(getId(), this.lsn);
      }
      else
         block_lsn.put(getId(), this.lsn);
   }

   public void setLsn(int lsn)
   {
      this.lsn = lsn;
   }

   public int getLsn()
   {
      return lsn;
   }

   public void setHashMap(HashMap<Integer, Integer> temp_map)
   {
      if(block_lsn.size() < temp_map.size())
      {
         block_lsn.put(block().number(), temp_map.get(temp_map.size() - 1));
      }
      else{
         block_lsn.clear();
         block_lsn.putAll(temp_map);
      }
   }

   public HashMap<Integer, Integer> getHashMap()
   {
      return block_lsn;
   }
    
   public void setLsnList(ArrayList<Integer> lsn_list)
   {
      if(lsnList.size() < lsn_list.size())
      {
         lsnList.add(lsn_list.get(lsn_list.size() - 1));
      }
      else
      {
         lsnList.clear();
         lsnList.addAll(lsn_list);
      }
      
   }

   public ArrayList<Integer> getLsnList()
   {
      return lsnList;
   }

   /**
    * Return true if the buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the buffer is pinned
    */
   public boolean isPinned() {
      return pins > 0;
   }
   
   public int modifyingTx() {
      return txnum;
   }

   /**
    * Reads the contents of the specified block into
    * the contents of the buffer.
    * If the buffer was dirty, then its previous contents
    * are first written to disk.
    * @param b a reference to the data block
    */
   void assignToBlock(BlockId b) {
      flush();
      blk = b;
      fm.read(blk, contents);
      pins = 0;
   }
   
   /**
    * Write the buffer to its disk block if it is dirty.
    */
   void flush() {
      if (txnum >= 0) {
         lm.flush(lsn);
         fm.write(blk, contents);
         txnum = -1;
      }
   }

   /**
    * Increase the buffer's pin count.
    */
   void pin() {
      pins++;
   }

   /**
    * Decrease the buffer's pin count.
    */
   void unpin() {
      pins--;
   }
}