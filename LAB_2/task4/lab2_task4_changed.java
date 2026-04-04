
package comm.dbms.lab;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class lab2_task4_changed {

    public static final int PAGE_SIZE = 512;
    private static final int HEADER_SIZE = 12;
    public static final int SLOT_SIZE = 8;

    private final int pageId;
    private final byte[] data;

    private static final int OFF_NUM_SLOTS = 0;
    private static final int OFF_FREE_START = 4;
    private static final int OFF_SLOT_END = 8;

    public lab2_task4_changed(int pageId) {
        this.pageId = pageId;
        this.data = new byte[PAGE_SIZE];

        setInt(OFF_NUM_SLOTS, 0);
        setInt(OFF_FREE_START, HEADER_SIZE);
        setInt(OFF_SLOT_END, PAGE_SIZE);
    }

    private void setInt(int pos, int val) {
        ByteBuffer.wrap(data, pos, 4)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(val);
    }

    public int getInt(int pos) {
        return ByteBuffer.wrap(data, pos, 4)
                .order(ByteOrder.BIG_ENDIAN)
                .getInt();
    }

    public int getPageId() { return pageId; }
    public int getNumSlots() { return getInt(OFF_NUM_SLOTS); }
    public int getFreeSpaceStart() { return getInt(OFF_FREE_START); }
    public int getSlotDirEnd() { return getInt(OFF_SLOT_END); }
    public int getFreeSpaceBytes() { return getSlotDirEnd() - getFreeSpaceStart(); }
    public byte[] getData() { return data; }

    public int insertRecord(byte[] record) {
        if (record == null || record.length == 0) {
            System.out.println("Error: Cannot insert null or empty record");
            return -1;
        }

        int recLen = record.length;
        int needed = recLen + SLOT_SIZE;

        if (getFreeSpaceBytes() < needed) {
            System.out.println("Page full (needed " + needed + ", free " + getFreeSpaceBytes() + ")");
            return -1;
        }

        int recOffset = getFreeSpaceStart();
        System.arraycopy(record, 0, data, recOffset, recLen);

        int slotNum = getNumSlots();
        int slotPos = getSlotDirEnd() - SLOT_SIZE;

        setInt(slotPos, recOffset);
        setInt(slotPos + 4, recLen);

        setInt(OFF_NUM_SLOTS, slotNum + 1);
        setInt(OFF_FREE_START, recOffset + recLen);
        setInt(OFF_SLOT_END, slotPos);

        System.out.println("Inserted slot " + slotNum + " | offset=" + recOffset + " len=" + recLen);
        return slotNum;
    }

    public byte[] getRecord(int slotNum) {
        if (slotNum < 0 || slotNum >= getNumSlots()) {
            System.out.println("Error: Invalid slot number " + slotNum);
            return null;
        }

        int slotPos = PAGE_SIZE - (slotNum + 1) * SLOT_SIZE;
        int offset = getInt(slotPos);
        int length = getInt(slotPos + 4);

        if (offset == -1 && length == 0) {
            return null;
        }

        if (offset < HEADER_SIZE || offset + length > getSlotDirEnd()) {
            System.out.println("Error: Invalid record bounds");
            return null;
        }

        byte[] rec = new byte[length];
        System.arraycopy(data, offset, rec, 0, length);
        return rec;
    }

    public boolean deleteRecord(int slotNum) {
        if (slotNum < 0 || slotNum >= getNumSlots()) {
            System.out.println("Error: Invalid slot number " + slotNum +
                    " (valid: 0 to " + (getNumSlots() - 1) + ")");
            return false;
        }

        int slotPos = PAGE_SIZE - (slotNum + 1) * SLOT_SIZE;

        int currentOffset = getInt(slotPos);
        int currentLength = getInt(slotPos + 4);

        if (currentOffset == -1 && currentLength == 0) {
            System.out.println("Warning: Slot " + slotNum + " is already deleted");
            return false;
        }

        setInt(slotPos, -1);
        setInt(slotPos + 4, 0);

        System.out.println("Deleted slot " + slotNum + " (tombstone: offset=-1, len=0)");
        return true;
    }

    public boolean isDeleted(int slotNum) {
        if (slotNum < 0 || slotNum >= getNumSlots()) return true;

        int slotPos = PAGE_SIZE - (slotNum + 1) * SLOT_SIZE;
        int offset = getInt(slotPos);
        int length = getInt(slotPos + 4);

        return (offset == -1 && length == 0);
    }

    public void printPageInfo() {
        System.out.println("\n=== Page " + pageId + " State ===");
        System.out.println("Num Slots     : " + getNumSlots());
        System.out.println("Free Start    : " + getFreeSpaceStart());
        System.out.println("Slot Dir End  : " + getSlotDirEnd());
        System.out.println("Free Bytes    : " + getFreeSpaceBytes());
        System.out.println("Tombstones    : " + countTombstones());

        System.out.println("\nSlot Directory:");
        for (int i = 0; i < getNumSlots(); i++) {
            if (isDeleted(i)) {
                System.out.println("  Slot " + i + ": [DELETED]");
            } else {
                byte[] rec = getRecord(i);
                if (rec != null) {
                    System.out.println("  Slot " + i + ": \"" + new String(rec) + "\"");
                }
            }
        }
        System.out.println("=======================\n");
    }

    private int countTombstones() {
        int count = 0;
        for (int i = 0; i < getNumSlots(); i++) {
            if (isDeleted(i)) count++;
        }
        return count;
    }

    public static void main(String[] args) {

        System.out.println("Testing lab2_task4: Tombstone Deletion");

        lab2_task4_changed page = new lab2_task4_changed(1);

        System.out.println("\nStep 1: Insert Records");
        page.insertRecord("CS".getBytes());      
        page.insertRecord("Math".getBytes());    
        page.insertRecord("Physics".getBytes()); 
        page.insertRecord("Chem".getBytes());    
        page.insertRecord("Biology".getBytes()); 
        page.insertRecord("English".getBytes()); 

        System.out.println("\nStep 2: Verify Records");
        for (int i = 0; i < page.getNumSlots(); i++) {
            byte[] rec = page.getRecord(i);
            System.out.println("Slot " + i + ": " + (rec != null ? new String(rec) : "null"));
        }

        System.out.println("\nStep 3: Delete Slots 0, 2, 4");
        page.deleteRecord(0);
        page.deleteRecord(2);
        page.deleteRecord(4);

        System.out.println("\nStep 4: Verify After Deletion");
        for (int i = 0; i < page.getNumSlots(); i++) {
            byte[] rec = page.getRecord(i);
            System.out.println("Slot " + i + ": " + (rec != null ? new String(rec) : "null"));
        }

        System.out.println("\nFinal Page State");
        System.out.println(" Task 4 Demo Complete!");
        System.out.println(" Summary:");
        System.out.println("   • Total slots: " + page.getNumSlots());
        System.out.println("   • Active records: " + (page.getNumSlots() - page.countTombstones()));
        System.out.println("   • Tombstones: " + page.countTombstones());
        System.out.println("   • RID stability: Slot numbers never change after delete ");
    }
}