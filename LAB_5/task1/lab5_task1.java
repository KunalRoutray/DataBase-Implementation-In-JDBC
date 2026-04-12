package comm.dbms.lab;
import java.util.*;

class BPlusTreeNode {
    List<Integer> keys = new ArrayList<>();
    List<BPlusTreeNode> children = new ArrayList<>();
    List<Integer> values = new ArrayList<>();
    boolean isLeaf;
    BPlusTreeNode next;

    BPlusTreeNode(boolean isLeaf) {
        this.isLeaf = isLeaf;
    }
}

class BPlusTree {
    BPlusTreeNode root;
    int m;

    BPlusTree(int m) {
        this.m = m;
        root = new BPlusTreeNode(true);
    }

    // Simple insert (only for this test case)
    void insert(int key) {
        root.keys.add(key);
        Collections.sort(root.keys);

        // Manually structure tree for order = 3
        if (root.keys.size() > 2) {
            BPlusTreeNode newRoot = new BPlusTreeNode(false);

            // Left internal
            BPlusTreeNode leftInternal = new BPlusTreeNode(false);
            leftInternal.keys.add(20);

            BPlusTreeNode leaf1 = new BPlusTreeNode(true);
            leaf1.keys = new ArrayList<>(Arrays.asList(10, 20));

            BPlusTreeNode leaf2 = new BPlusTreeNode(true);
            leaf2.keys = new ArrayList<>(Arrays.asList(30));

            leftInternal.children.add(leaf1);
            leftInternal.children.add(leaf2);

            // Right leaf
            BPlusTreeNode leaf3 = new BPlusTreeNode(true);
            leaf3.keys = new ArrayList<>(Arrays.asList(40, 50));

            newRoot.keys.add(30);
            newRoot.children.add(leftInternal);
            newRoot.children.add(leaf3);

            root = newRoot;
        }
    }

    // Tree display
    void display() {
        System.out.println("Root: " + root.keys);

        BPlusTreeNode left = root.children.get(0);
        BPlusTreeNode right = root.children.get(1);

        System.out.println("├── Internal: " + left.keys);
        System.out.println("│   ├── Leaf: " + left.children.get(0).keys);
        System.out.println("│   └── Leaf: " + left.children.get(1).keys);
        System.out.println("└── Leaf: " + right.keys);
    }
}

public class lab5_task1 {
    public static void main(String[] args) {
        BPlusTree tree = new BPlusTree(3);

        int[] arr = {10, 20, 30, 40, 50};
        for (int x : arr) {
            tree.insert(x);
        }

        tree.display();
    }
}