package simpledb.buffer;

import java.util.*;
import simpledb.file.*;
import simpledb.log.LogMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
public class BufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private static final long MAX_TIME = 10000; // 10 seconds
   static LinkedHashMap<Buffer, Integer> hashmap = new LinkedHashMap<>();
   LinkedHashMap<BlockId, Buffer> bufferPoolMap = new LinkedHashMap<>();
   static LinkedHashMap<Integer, Integer> lsn_block = new LinkedHashMap<>();
   LinkedHashMap<Integer, Buffer> removed = new LinkedHashMap<>();
   LinkedHashMap<String, Integer> list = new LinkedHashMap<>();
   static ArrayList<Buffer> empty_buffer = new ArrayList<Buffer>();
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on a {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} object.
    * @param numbuffs the number of buffer slots to allocate
    */
   public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {

      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++)
      {
         empty_buffer.add(new Buffer(fm, lm, i));
      }
            
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   public synchronized int available() {
      return numAvailable;
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   public synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
      {
         if (buff.modifyingTx() == txnum)
         buff.flush();
        
      }
         
   }
   
   
   /**
    * Unpins the specified data buffer. If its pin count
    * goes to zero, then notify any waiting threads.
    * @param buff the buffer to be unpinned
    */
   public synchronized void unpin(Buffer buff) {
      buff.unpin();

      if(!buff.isPinned())
      {
         numAvailable++;
         empty_buffer.add(buff);
         
         if(!lsn_block.isEmpty())
         {
            lsn_block.clear();
            lsn_block.putAll(empty_buffer.get(empty_buffer.size()-1).getHashMap());
         }

         notifyAll();
      }
   
   }
   
   /**
    * Pins a buffer to the specified block, potentially
    * waiting until a buffer becomes available.
    * If no buffer becomes available within a fixed 
    * time period, then a {@link BufferAbortException} is thrown.
    * @param blk a reference to a disk block
    * @return the buffer pinned to that block
    */
   public synchronized Buffer pin(BlockId blk) {
      try {
         long timestamp = System.currentTimeMillis();
         Buffer buff = tryToPin(blk);
         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = tryToPin(blk);
         }
         if (buff == null)
            throw new BufferAbortException();
         return buff;
      }
      catch(InterruptedException e) {
         throw new BufferAbortException();
      }
   }  
   
   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }
   
   /**
    * Tries to pin a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   private Buffer tryToPin(BlockId blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if(!empty_buffer.isEmpty())
            empty_buffer.remove(buff);
         if (buff == null)
            return null;
         if(hashmap.containsKey(buff))
         {
            hashmap.replace(buff, blk.number());
            buff.setLsn(-1);
            if(bufferPoolMap.containsValue(buff))
            {
               BlockId temp;
               for(Map.Entry<BlockId, Buffer> entry : bufferPoolMap.entrySet())
               {
                  if(entry.getValue() == buff)
                  {
                     temp = entry.getKey();
                     bufferPoolMap.remove(temp);
                     bufferPoolMap.put(blk, buff);
                     break;
                  }
               }
            }
         }  
         else
         {
            hashmap.put(buff, blk.number());
            bufferPoolMap.put(blk, buff);
            list.put("Pinned: ", buff.getLsn());
         }
         buff.assignToBlock(blk);
        
      }

      if (!buff.isPinned())
      {
         numAvailable--;
      }
         
      buff.pin();
     
      return buff;
   }
   
   private Buffer findExistingBuffer(BlockId blk) {

      if(bufferPoolMap.containsKey(blk))
      {
         return bufferPoolMap.get(blk);
      }

      return null;
   }
   
   private Buffer chooseUnpinnedBuffer() {
      
      if(!empty_buffer.isEmpty())
      {
         for(Buffer buff : empty_buffer)
         {
            if(!buff.getHashMap().isEmpty() && buff.getHashMap().containsKey(buff.getId()))
            {
               lsn_block.clear();
               lsn_block.putAll(empty_buffer.get(empty_buffer.size()-1).getHashMap());

               ArrayList<Integer> buffer_list = new ArrayList<>();
               ArrayList<Integer> bufferlsn_list = new ArrayList<>();
               for(Map.Entry<Integer,Integer> entry : lsn_block.entrySet())
               {
                  buffer_list.add(entry.getKey());
                  bufferlsn_list.add(entry.getValue());
               }

               for(int i=0; i<lsn_block.size(); i++)
               {
                  for(int j=0; j<empty_buffer.size(); j++)
                  {
                     if(buffer_list.get(i) == empty_buffer.get(j).getId())
                     {
                        lsn_block.remove(buffer_list.get(i));
                        buff.setHashMap(lsn_block);
                        removed.put(bufferlsn_list.get(i), empty_buffer.get(j));
                        return empty_buffer.get(j);
                     }
                  }
               }
               
            }
            
            return empty_buffer.get(0);
            
         }               
           
      }
      return null;  
   }

   void printStatus()
   {
      System.out.println("\nAllocated Buffers:");
      int i =0;
      
      ArrayList<Buffer> buffer_list = new ArrayList<>();
      for(Map.Entry<Buffer,Integer> entry : hashmap.entrySet())
      {
         buffer_list.add(entry.getKey());
      }

      ArrayList<Integer> unpinned_list_key = new ArrayList<>();
      ArrayList<Integer> unpinned_list_value = new ArrayList<>();
      LinkedHashMap<Integer, Integer> unpinnedLRM = new LinkedHashMap<>();
      LinkedHashMap<Integer, Buffer> unpinnedLSN = new LinkedHashMap<>();
      int k=0;
      int l=0;
      unpinnedLRM.clear();
      unpinnedLSN.clear();
      for(Map.Entry<Buffer,Integer> entry : hashmap.entrySet())
      {
         if(!buffer_list.get(k).isPinned())
         {
            if(buffer_list.get(k).getLsn() >= 1)
               unpinnedLRM.put(entry.getKey().getLsn(), entry.getKey().getId());
            else
            {
               unpinnedLSN.put(l, entry.getKey());
               //System.out.println(unpinnedLSN.get()));
               l++;
            }
         }
         k++;
      }

      TreeMap<Integer, Integer> sorted = new TreeMap<>();
      sorted.putAll(unpinnedLRM);
      for(Map.Entry<Integer,Integer> entry : sorted.entrySet())
      {
         unpinned_list_key.add(entry.getKey());
         unpinned_list_value.add(entry.getValue());
      }



      for (Buffer buff : buffer_list) {
         if (buff != null) 
         {
            if(buff.isPinned())
               System.out.println("Buffer " + i + ": " + buff.block() + " Pinned lsn" + buff.getLsn());
            else
               System.out.println("Buffer " + i + ": " + buff.block() + " Unpinned lsn" + buff.getLsn());
            i++;
         }
      }
      if(!unpinned_list_key.isEmpty() || !unpinnedLSN.isEmpty())
         System.out.print("\nUnpinned Buffers in LRM order: ");
      for(int j=0; j<unpinned_list_key.size(); j++)
      {
         System.out.print(unpinned_list_value.get(j) + "(lsn" + unpinned_list_key.get(j) + ") ");
      }
      for(int j=0; j<unpinnedLSN.size(); j++)
      {
         System.out.print(unpinnedLSN.get(j).getId() + "(lsn" + unpinnedLSN.get(j).getLsn() + ") ");
      }
      System.out.println("\n");
   }

}