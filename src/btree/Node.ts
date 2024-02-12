import { PageAllocatorComponent } from "../PageAllocatorComponent";
import { ConfigEntryComponent } from "../ConfigPageComponent";
import { VarRecordId, VarRecordsComponent } from "../records/VarRecords";
import {
    IPage,
    IPageStore,
    IStoreCollection,
    Namespace,
    PageNum,
    PageSize,
} from "../store/IPageStore";
import { NAMESPACE_FMT, PAGE_FMT } from "../txStore/LogRecord/types";
import { IEvent, IObj, TxCollection, TxPage } from "../txStore/LogStore";
import {
    AddBranchKeyEvent,
    BRANCH_OVERHEAD,
    BranchEvent,
    BranchObj,
    DeinitBranchEvent,
    DelBranchKeyEvent,
    InitBranchEvent,
    SetBranchKeyEvent,
    WithChild,
    deserializeBranchEvent,
    deserializeBranchObj,
} from "./Branch";
import {
    AddLeafEntryEvent,
    DelLeafEntryEvent,
    LEAF_OVERHEAD,
    LeafEvent,
    LeafObj,
    MAX_LEAF_PAGE_SIZE,
    SetLeafEntryEvent,
    SetLeafLinksEvent,
    deserializeLeafEvent,
    deserializeLeafObj,
} from "./Leaf";
import { PAGE_LINK_BYTES } from "../records/SizeClass";

type NodeObj = BranchObj | LeafObj;
type NodeEvent = BranchEvent | LeafEvent;

const ROOT_FMT = NAMESPACE_FMT + PAGE_FMT;

export class RootId {
    public readonly namespace: Namespace;
    public readonly pageNum: PageNum;

    public constructor(namespace: Namespace, pageNum: PageNum) {
        this.namespace = namespace;
        this.pageNum = pageNum;
    }

    public serialize(): string {
        return string.pack(ROOT_FMT, this.namespace, this.pageNum);
    }

    public static deserialize(str: string): RootId {
        const [namespace, pageNum] = string.unpack(ROOT_FMT, str);
        return new RootId(namespace, pageNum);
    }

    public is(page: TxPage<NodeObj, NodeEvent>): boolean {
        return this.namespace == page.namespace && this.pageNum == page.pageNum;
    }
}

export type KvPair = { key: string, value: string };

/** A result from a split child to the parent. */
type InsertionSplitOp = {
    /** The page number of the right child (the left child never changes). */
    nextNode: PageNum,
    splitKey: VarRecordId,
};

/** A result from an insertion in a node. */
type InsertionResult = {
    split?: InsertionSplitOp;
    oldVal?: string;
}

enum SiblingPos {
    LEFT,
    RIGHT,
}

type Sibling = {
    /** The split key between this node and its sibling. */
    splitKey: VarRecordId,

    /** The page number of the node. */
    node: PageNum,
}

type Siblings = {
    left?: Sibling,
    right?: Sibling,
}

enum DeletionResultTy {
    MERGE,
    REASSIGN,
}

/** A result from a merged child to the parent. */
type DeletionMergeOp = {
    ty: DeletionResultTy.MERGE,
    with: SiblingPos,
}

/** A result from a rebalanced child to the parent. */
type DeletionRebalanceOp = {
    ty: DeletionResultTy.REASSIGN,
    with: SiblingPos,
    newSplitKey: VarRecordId,
}

type DeletionOp = DeletionMergeOp | DeletionRebalanceOp;

/** A result from a deletion in a node. */
type DeletionResult = {
    operation?: DeletionOp,
    oldVal?: string,
}

/** A component implementing a disk B+ tree. */
export class BTreeComponent {
    private branchNamespace: Namespace;
    private leafNamespace: Namespace;
    private pageSize: PageSize;

    /** A component for allocating leaf pages. */
    private allocatedLeaves: PageAllocatorComponent;

    /** A component for allocating branch pages. */
    private allocatedBranches: PageAllocatorComponent;

    /** A config entry for the root page. */
    private rootPageConfig: ConfigEntryComponent;

    /** VarRecordsComponent for storing allocated key and value records. */
    private vrc: VarRecordsComponent;

    /** The minimum used space for leaf nodes before they underflow. */
    private leafMinUsedSpace: number;

    /** The minimum used space for branch nodes before they underflow. */
    private branchMinUsedSpace: number;

    public constructor(
        collection: IStoreCollection<IPage, IPageStore<IPage>>,
        vrc: VarRecordsComponent,
        rootConfig: ConfigEntryComponent,
        leafAllocator: PageAllocatorComponent,
        branchAllocator: PageAllocatorComponent,
    ) {
        this.vrc = vrc;
        this.pageSize = collection.pageSize;
        this.leafNamespace = leafAllocator.pagesNamespace;
        this.allocatedLeaves = leafAllocator;
        this.branchNamespace = branchAllocator.pagesNamespace;
        this.allocatedBranches = branchAllocator;
        this.rootPageConfig = rootConfig;

        // The page size and max VID head length must be constrained to let at
        // least three keys fit on a single leaf, including overheads.
        // TODO: Maybe it's more? I'm too tired to work it out.
        const leafEntrySize = LeafObj.getMaxEntrySize(vrc);
        const branchEntrySize = BranchObj.getMaxEntrySize(vrc);
        const minLeafBytes = LEAF_OVERHEAD + 3 * leafEntrySize;
        assert(collection.pageSize >= minLeafBytes);
        assert(collection.pageSize <= MAX_LEAF_PAGE_SIZE);

        // Set the underflow bounds.
        // We set it under by 1 full entry to make sure two siblings can't keep
        // stealing an entry from each other without merging.
        const pageSize = collection.pageSize;
        const leafCap = pageSize - LEAF_OVERHEAD;
        const branchCap = pageSize - BRANCH_OVERHEAD;
        this.leafMinUsedSpace = math.floor(leafCap / 2) - leafEntrySize;
        this.branchMinUsedSpace = math.floor(branchCap / 2) - branchEntrySize;
    }

    public deserializeObj(n: Namespace, s?: string): IObj<IEvent> | undefined {
        if (n == this.branchNamespace) { return deserializeBranchObj(s); }
        if (n == this.leafNamespace) { return deserializeLeafObj(s); }
        const a = this.allocatedBranches.deserializeObj(n, s); if (a) return a;
        const b = this.allocatedLeaves.deserializeObj(n, s); if (b) return b;
        const c = this.rootPageConfig.deserializeObj(n, s); if (c) return c;
        const d = this.vrc.deserializeObj(n, s); if (d) return d;
    }

    public deserializeEv(n: Namespace, s: string): IEvent | undefined {
        if (n == this.branchNamespace) { return deserializeBranchEvent(s); }
        if (n == this.leafNamespace) { return deserializeLeafEvent(s); }
        const a = this.allocatedBranches.deserializeEv(n, s); if (a) return a;
        const b = this.allocatedLeaves.deserializeEv(n, s); if (b) return b;
        const c = this.rootPageConfig.deserializeEv(n, s); if (c) return c;
        const d = this.vrc.deserializeEv(n, s); if (d) return d;
    }

    /**
     * Finds the two indexes flanking a target value in a node.
     * @param node - The node to search.
     * @param key - The key to search for.
     * @returns The index of the largest key less-than or equal to `key`, or
     * `-1` if there are none.
     * @returns The index of the smallest key greater-than or equal to `key`, or
     * `node.keys.length` if there are none.
     */
    private flankIndexes(
        cl: TxCollection,
        node: NodeObj,
        key: string,
    ): LuaMultiReturn<[number, number]> {
        let low = -1;
        let high = node.keys.length;
        while (low + 1 < high) {
            const mid = math.floor((low + high) / 2);
            const cmp = this.vrc.cmp(cl, key, node.keys[mid]);
            if (cmp >= 0) { low = mid; }
            if (cmp <= 0) { high = mid; }
        }
        return $multi(low, high);
    }

    /** Returns the root node page. */
    private getRoot(cl: TxCollection): TxPage<NodeObj, NodeEvent> {
        const rootIdStr = this.rootPageConfig.get(cl);
        const rootId = rootIdStr ?
            RootId.deserialize(rootIdStr) :
            new RootId(this.leafNamespace, 0 as PageNum);
        return cl.getStoreCast<NodeObj, NodeEvent>(rootId.namespace)
            .getPage(rootId.pageNum);
    }

    /** Sets the root node page. */
    private setRoot(
        cl: TxCollection,
        id: RootId,
    ) {
        this.rootPageConfig.set(cl, id.serialize());
    }

    /** Returns the namespace for a node's children. */
    private getChildNamespace(node: BranchObj): Namespace {
        if (node.height == 1) {
            return this.leafNamespace;
        } else {
            return this.branchNamespace;
        }
    }

    /**
     * Given `l`, `r` with `l <= r`, returns the shortest string `s` such that
     * `l < s <= r`.
     */
    private computeSeparator(l: string, r: string): string {
        assert(l <= r);
        let i = 0;
        do { i++; } while (string.byte(l, i) == string.byte(r, i));
        return string.sub(r, 1, i);
    }

    /**
     * Searches a leaf for the keys flanking a target.
     * @param cl - The collection to operate.
     * @param key - The target key.
     * @param leaf - The leaf node where the target should be in the tree.
     * @returns The key-value pair for the largest entry less-than the target,
     * if it exists.
     * @returns The key-value pair for the smallest entry greater-than or equal
     * to the target, if it exists.
     */
    private searchLeaf(
        cl: TxCollection,
        key: string,
        leaf: LeafObj,
    ): LuaMultiReturn<[KvPair | undefined, KvPair | undefined]> {
        const [iLow, iHigh] = this.flankIndexes(cl, leaf, key);

        // iPrev points to the strictly less-than index entry, or -1.
        const iPrev = iLow == iHigh ? iLow - 1 : iLow;

        let kvLow: KvPair | undefined;
        if (iPrev == -1) {
            // The value is the last value in the previous node, if it exists.
            if (leaf.prev) {
                const prev = cl
                    .getStoreCast<LeafObj, LeafEvent>(this.leafNamespace)
                    .getPage(leaf.prev)
                    .obj;
                kvLow = {
                    key: this.vrc.read(cl, prev.keys[prev.keys.length - 1]),
                    value: this.vrc.read(cl, prev.vals[prev.vals.length - 1]),
                };
            }
        } else {
            // The value is in the current node.
            kvLow = {
                key: this.vrc.read(cl, leaf.keys[iPrev]),
                value: this.vrc.read(cl, leaf.vals[iPrev]),
            };
        }

        let kvHigh: KvPair | undefined;
        if (iHigh == leaf.keys.length) {
            // The value is the first value in the next node, if it exists.
            if (leaf.next) {
                const next = cl
                    .getStoreCast<LeafObj, LeafEvent>(this.leafNamespace)
                    .getPage(leaf.next)
                    .obj;
                kvHigh = {
                    key: this.vrc.read(cl, next.keys[0]),
                    value: this.vrc.read(cl, next.vals[0]),
                };
            }
        } else {
            // The value is in the current node.
            kvHigh = {
                key: this.vrc.read(cl, leaf.keys[iHigh]),
                value: this.vrc.read(cl, leaf.vals[iHigh]),
            };
        }

        return $multi(kvLow, kvHigh);
    }

    /**
     * Searches a node for the keys flanking a target.
     * @param parent - The parent node of where the target should be in the
     * tree.
     * @returns The key-value pair for the largest entry less-than the target,
     * if it exists.
     * @returns The key-value pair for the smallest entry greater-than or equal
     * to the target, if it exists.
     */
    private recursiveSearch(
        cl: TxCollection,
        key: string,
        parent: NodeObj,
    ): LuaMultiReturn<[KvPair | undefined, KvPair | undefined]> {
        if (parent.type == "leaf") {
            return this.searchLeaf(
                cl,
                key,
                parent,
            );
        }

        // Choosing iLow + 1 follows from the "standard" tree structure:
        // - If the branch key is equal to the target key, the rightmost child
        //   will be chosen on the path taken.
        // - All branches have at least 2 children, so the index never fails.
        // - If iLow == -1, then the 0th children is taken, which is correct.
        const [iLow, _] = this.flankIndexes(cl, parent, key);
        const childNamespace = this.getChildNamespace(parent);
        return this.recursiveSearch(
            cl,
            key,
            cl.getStoreCast<NodeObj, NodeEvent>(childNamespace)
                .getPage(parent.children[iLow + 1])
                .obj,
        );
    }

    /**
     * Searches the tree for an entry.
     * @returns The key-value pair of the greatest entry less-than the key, if
     * it exists.
     * @returns The key-value pair of the smallest entry greater-than or equal
     * to the key, if it exists.
     */
    public search(
        cl: TxCollection,
        key: string,
    ): LuaMultiReturn<[KvPair | undefined, KvPair | undefined]> {
        return this.recursiveSearch(cl, key, this.getRoot(cl).obj);
    }

    /** Returns whether a to-be-inserted entry can fit in a leaf node. */
    private fitsInLeaf(
        cl: TxCollection,
        leaf: LeafObj,
        key: string,
        value: string,
    ): boolean {
        const pageSize = cl.pageSize;
        const freeSpace = pageSize - LEAF_OVERHEAD - leaf.usedSpace;
        const keyHeadLen = this.vrc.getVidLength(key);
        const valHeadLen = this.vrc.getVidLength(value);
        return freeSpace >= keyHeadLen + valHeadLen;
    }

    /** Inserts an entry into a leaf page. Assumes there is room to fit. */
    private insertLeafEntryNoOverflow(
        cl: TxCollection,
        leaf: TxPage<LeafObj, LeafEvent>,
        key: string,
        value: string,
    ): string | undefined {
        // This only fails if the caller was careless or if the page size is too
        // small for regular operation.
        assert(this.fitsInLeaf(cl, leaf.obj, key, value));

        const [iLow, iHigh] = this.flankIndexes(cl, leaf.obj, key);
        const valVid = this.vrc.allocate(cl, value);

        if (iLow == iHigh) {
            // The key already exists, update the value and exit.
            const oldEntry = this.vrc.read(cl, leaf.obj.vals[iHigh]);
            this.vrc.free(cl, leaf.obj.vals[iHigh]);
            leaf.doEvent(new SetLeafEntryEvent(iHigh, valVid));
            return oldEntry;
        }

        // Insert into the high index. This handles all edge cases already.
        const keyVid = this.vrc.allocate(cl, key);
        leaf.doEvent(new AddLeafEntryEvent(iHigh, valVid, keyVid));
    }

    /** Inserts an entry into a leaf page. Also handles leaf overflow. */
    private insertLeafEntryMayOverflow(
        cl: TxCollection,
        leaf: TxPage<LeafObj, LeafEvent>,
        key: string,
        value: string,
    ): InsertionResult {
        if (this.fitsInLeaf(cl, leaf.obj, key, value)) {
            return {
                oldVal: this.insertLeafEntryNoOverflow(cl, leaf, key, value),
            };
        }

        // Allocate a new node for splitting.
        const newLeaf = this.allocatedLeaves
            .allocPageCast<LeafObj, LeafEvent>(cl);

        // Move in entries.
        const splitIdx = leaf.obj.getSplitIndex();
        for (const i of $range(splitIdx, leaf.obj.keys.length - 1)) {
            newLeaf.doEvent(new AddLeafEntryEvent(
                i - splitIdx,
                leaf.obj.vals[i],
                leaf.obj.keys[i],
            ));
        }

        // Delete old entries from the current node.
        for (const i of $range(leaf.obj.keys.length - 1, splitIdx, -1)) {
            leaf.doEvent(new DelLeafEntryEvent(i));
        }

        // Rewire links.
        const oldNextLeafNum = leaf.obj.next;
        if (oldNextLeafNum) {
            const oldNextLeaf = cl
                .getStoreCast<LeafObj, LeafEvent>(this.leafNamespace)
                .getPage(oldNextLeafNum);
            oldNextLeaf.doEvent(new SetLeafLinksEvent(
                newLeaf.pageNum,
                oldNextLeaf.obj.next,
            ));
        }
        newLeaf.doEvent(new SetLeafLinksEvent(leaf.pageNum, oldNextLeafNum));
        leaf.doEvent(new SetLeafLinksEvent(leaf.obj.prev, newLeaf.pageNum));

        // Compute the split key to be passed into the parent.
        const lKey = this.vrc.read(cl, leaf.obj.keys[leaf.obj.keys.length - 1]);
        const rKey = this.vrc.read(cl, newLeaf.obj.keys[0]);
        const splitKey = this.computeSeparator(lKey, rKey);
        const split = {
            nextNode: newLeaf.pageNum,
            splitKey: this.vrc.allocate(cl, splitKey),
        };

        // Insert the element into one of the two nodes.
        if (key < splitKey) {
            // Left
            return {
                oldVal: this.insertLeafEntryNoOverflow(cl, leaf, key, value),
                split,
            };
        } else {
            // Right
            return {
                oldVal: this.insertLeafEntryNoOverflow(cl, newLeaf, key, value),
                split,
            };
        }
    }

    /** Returns whether a to-be-inserted entry can fit in a branch node. */
    private fitsInBranch(
        cl: TxCollection,
        branch: BranchObj,
        splitKey: VarRecordId,
    ): boolean {
        const pageSize = cl.pageSize;
        const freeSpace = pageSize - BRANCH_OVERHEAD - branch.usedSpace;
        const keyHeadLen = splitKey.length();
        const childLen = PAGE_LINK_BYTES;
        return freeSpace >= keyHeadLen + childLen;
    }

    /** Handles a branch split. Assumes there is enough room in the parent. */
    private handleBranchSplitNoOverflow(
        cl: TxCollection,
        branch: TxPage<BranchObj, BranchEvent>,
        msg: InsertionSplitOp,
    ): undefined {
        // This only fails if the caller was careless or if the page size is too
        // small for regular operation.
        assert(this.fitsInBranch(cl, branch.obj, msg.splitKey));

        const splitKeyStr = this.vrc.read(cl, msg.splitKey);
        const [_, iHigh] = this.flankIndexes(cl, branch.obj, splitKeyStr);

        // Insert into the high index, pushing other indices away.
        branch.doEvent(new AddBranchKeyEvent(
            iHigh,
            msg.nextNode,
            msg.splitKey,
            WithChild.RIGHT,
        ));
    }

    /** Handles a branch split and handles parent overflow. */
    private handleBranchSplitMayOverflow(
        cl: TxCollection,
        branch: TxPage<BranchObj, BranchEvent>,
        msg: InsertionSplitOp,
    ): InsertionSplitOp | undefined {
        if (this.fitsInBranch(cl, branch.obj, msg.splitKey)) {
            return this.handleBranchSplitNoOverflow(cl, branch, msg);
        }

        // Get the split key which will be bubbled up to the parent.
        const splitIdx = branch.obj.getSplitIndex();
        const splitKey = branch.obj.keys[splitIdx];

        // Allocate a new node for splitting.
        const newBranch = this.allocatedBranches
            .allocPageCast<BranchObj, BranchEvent>(cl);

        // Initialize it with the child on the right of the split key.
        newBranch.doEvent(new InitBranchEvent(
            branch.obj.height,
            branch.obj.children[splitIdx],
        ));

        // Move in entries starting with the next from the split key.
        for (const i of $range(splitIdx + 1, branch.obj.keys.length - 1)) {
            newBranch.doEvent(new AddBranchKeyEvent(
                i - (splitIdx + 1),
                branch.obj.children[i + 1],
                branch.obj.keys[i],
                WithChild.RIGHT,
            ));
        }

        // Delete old entries from the current node.
        for (const i of $range(branch.obj.keys.length - 1, splitIdx, -1)) {
            branch.doEvent(new DelBranchKeyEvent(i, WithChild.RIGHT));
        }

        // Insert the element into one of the two nodes.
        const msgSplitKeyStr = this.vrc.read(cl, msg.splitKey);
        if (this.vrc.cmp(cl, msgSplitKeyStr, splitKey) < 0) {
            // Left
            this.handleBranchSplitNoOverflow(cl, branch, msg);
        } else {
            // Right
            this.handleBranchSplitNoOverflow(cl, newBranch, msg);
        }

        return {
            nextNode: newBranch.pageNum,
            splitKey,
        };
    }

    /**
     * @param cl - The collection this component is working on.
     * @param key - The key of the record to insert.
     * @param value - The value of the record to insert.
     * @param parent - The node to insert the record into.
     * @returns An insertion split message if the insert happened with a split.
     */
    private recursiveInsert(
        cl: TxCollection,
        key: string,
        value: string,
        parent: TxPage<NodeObj, NodeEvent>,
    ): InsertionResult {
        if (parent.obj.type == "leaf") {
            return this.insertLeafEntryMayOverflow(
                cl,
                parent as TxPage<LeafObj, LeafEvent>,
                key,
                value,
            );
        }

        // Insert into child.
        const [iLow] = this.flankIndexes(cl, parent.obj, key);
        const childNamespace = this.getChildNamespace(parent.obj);
        const result = this.recursiveInsert(
            cl,
            key,
            value,
            cl.getStoreCast<NodeObj, NodeEvent>(childNamespace)
                .getPage(parent.obj.children[iLow + 1]),
        );

        // Handle split.
        if (!result.split) { return result; }
        return {
            oldVal: result.oldVal,
            split: this.handleBranchSplitMayOverflow(
                cl,
                parent as TxPage<BranchObj, BranchEvent>,
                result.split,
            ),
        };
    }

    /**
     * Inserts a value into the tree. Replaces an existing value if needed.
     * @param cl - The collection to operate on.
     * @param key - The key to insert the entry under.
     * @param value - The value to insert.
     */
    public insert(
        cl: TxCollection,
        key: string,
        value: string,
    ): string | undefined {
        // Insert into the tree from the root.
        const root = this.getRoot(cl);
        const result = this.recursiveInsert(cl, key, value, root);

        // Handle split.
        if (!result.split) { return result.oldVal; }

        // Create a new root node.
        const newRoot = this.allocatedBranches
            .allocPageCast<BranchObj, BranchEvent>(cl);

        // Initialize it with the root as the left child.
        newRoot.doEvent(new InitBranchEvent(
            root.obj.height + 1,
            root.pageNum,
        ));

        // Insert the split key and right child.
        newRoot.doEvent(new AddBranchKeyEvent(
            1,
            result.split.nextNode,
            result.split.splitKey,
            WithChild.RIGHT,
        ));

        // Set the new root in the config.
        this.setRoot(cl, new RootId(newRoot.namespace, newRoot.pageNum));

        return result.oldVal;
    }

    /** Computes siblings for a given child node. */
    private getSiblings(
        parent: BranchObj,
        childIndex: number,
    ): Siblings {
        const out = <Siblings>{};

        if (childIndex != 0) {
            out.left = {
                splitKey: parent.keys[childIndex - 1],
                node: parent.children[childIndex - 1],
            };
        }

        if (childIndex != parent.children.length - 1) {
            out.right = {
                splitKey: parent.keys[childIndex],
                node: parent.children[childIndex + 1],
            };
        }

        return out;
    }

    /** Merges a right leaf into a left leaf, assuming it fits. */
    private mergeLeaves(
        cl: TxCollection,
        left: TxPage<LeafObj, LeafEvent>,
        right: TxPage<LeafObj, LeafEvent>,
    ) {
        // Copy keys into the left sibling.
        for (const i of $range(0, right.obj.keys.length - 1)) {
            left.doEvent(new AddLeafEntryEvent(
                left.obj.keys.length,
                right.obj.vals[i],
                right.obj.keys[i],
            ));
        }

        // Delete keys from the right sibling.
        for (const i of $range(right.obj.keys.length - 1, 0, -1)) {
            right.doEvent(new DelLeafEntryEvent(i));
        }

        // Rewire links.
        const nextLeafNum = right.obj.next;
        if (nextLeafNum) {
            const nextLeaf = cl
                .getStoreCast<LeafObj, LeafEvent>(this.leafNamespace)
                .getPage(nextLeafNum);
            nextLeaf.doEvent(new SetLeafLinksEvent(
                left.pageNum,
                nextLeaf.obj.next,
            ));
        }
        left.doEvent(new SetLeafLinksEvent(left.obj.prev, right.obj.next));
        right.doEvent(new SetLeafLinksEvent());
    }

    private deleteLeafEntry(
        cl: TxCollection,
        leaf: TxPage<LeafObj, LeafEvent>,
        key: string,
        siblings: Siblings,
    ): DeletionResult {
        const [iLow, iHigh] = this.flankIndexes(cl, leaf.obj, key);
        if (iLow != iHigh) { return {}; }

        // Delete the entry.
        const oldVal = this.vrc.read(cl, leaf.obj.vals[iLow]);
        this.vrc.free(cl, leaf.obj.vals[iLow]);
        leaf.doEvent(new DelLeafEntryEvent(iLow));

        // Handle underflow.
        if (leaf.obj.usedSpace >= this.leafMinUsedSpace) {
            return { oldVal };
        }

        const leafSpace = leaf.obj.usedSpace + LEAF_OVERHEAD;
        const lSibling = siblings.left && cl
            .getStoreCast<LeafObj, LeafEvent>(this.leafNamespace)
            .getPage(siblings.left.node);

        // Try stealing from the left sibling.
        if (lSibling && lSibling.obj.usedSpace + leafSpace > this.pageSize) {
            leaf.doEvent(new AddLeafEntryEvent(
                0,
                lSibling.obj.vals[lSibling.obj.keys.length - 1],
                lSibling.obj.keys[lSibling.obj.keys.length - 1],
            ));
            lSibling.doEvent(new DelLeafEntryEvent(
                lSibling.obj.keys.length - 1,
            ));
            return {
                oldVal,
                operation: {
                    ty: DeletionResultTy.REASSIGN,
                    with: SiblingPos.LEFT,
                    newSplitKey: leaf.obj.keys[0],
                },
            };
        }

        const rSibling = siblings.right && cl
            .getStoreCast<LeafObj, LeafEvent>(this.leafNamespace)
            .getPage(siblings.right.node);

        // Try stealing from the right sibling.
        if (rSibling && rSibling.obj.usedSpace + leafSpace > this.pageSize) {
            leaf.doEvent(new AddLeafEntryEvent(
                leaf.obj.keys.length,
                rSibling.obj.vals[0],
                rSibling.obj.keys[0],
            ));
            rSibling.doEvent(new DelLeafEntryEvent(0));
            return {
                oldVal,
                operation: {
                    ty: DeletionResultTy.REASSIGN,
                    with: SiblingPos.RIGHT,
                    newSplitKey: rSibling.obj.keys[0],
                },
            };
        }

        // Try merging with the left sibling.
        if (lSibling && lSibling.obj.usedSpace + leafSpace <= this.pageSize) {
            this.mergeLeaves(cl, lSibling, leaf);
            return {
                oldVal,
                operation: {
                    ty: DeletionResultTy.MERGE,
                    with: SiblingPos.LEFT,
                },
            };
        }

        // Try merging with the right sibling.
        if (rSibling && rSibling.obj.usedSpace + leafSpace <= this.pageSize) {
            this.mergeLeaves(cl, leaf, rSibling);
            return {
                oldVal,
                operation: {
                    ty: DeletionResultTy.MERGE,
                    with: SiblingPos.RIGHT,
                },
            };
        }

        // There are no siblings, so this is the root node.
        return { oldVal };
    }

    /** Merges a right branch into a left branch, assuming it fits. */
    private mergeBranches(
        left: TxPage<BranchObj, BranchEvent>,
        splitKey: VarRecordId,
        right: TxPage<BranchObj, BranchEvent>,
    ) {
        // Copy the first child into the left sibling using the split key.
        left.doEvent(new AddBranchKeyEvent(
            left.obj.keys.length,
            right.obj.children[0],
            splitKey,
            WithChild.RIGHT,
        ));

        // Copy over the other children with the previous key.
        for (const i of $range(0, right.obj.keys.length - 1)) {
            left.doEvent(new AddBranchKeyEvent(
                left.obj.keys.length,
                right.obj.children[i + 1],
                right.obj.keys[i],
                WithChild.RIGHT,
            ));
        }

        // Delete keys from the right sibling.
        for (const i of $range(right.obj.keys.length - 1, 0, -1)) {
            right.doEvent(new DelBranchKeyEvent(i, WithChild.RIGHT));
        }

        // Deinit the right node.
        right.doEvent(new DeinitBranchEvent());
    }

    /** Handles a merge of two children nodes. */
    private handleMerge(
        cl: TxCollection,
        parent: TxPage<BranchObj, BranchEvent>,
        uncles: Siblings,
        childIndex: number,
        siblings: Siblings,
        op: DeletionMergeOp,
    ): DeletionOp | undefined {
        // Delete the key and child pointer that were merged.
        if (op.with == SiblingPos.LEFT) {
            assert(siblings.left);
            parent.doEvent(new DelBranchKeyEvent(
                childIndex - 1,
                WithChild.RIGHT,
            ));
        } else {
            assert(siblings.right);
            parent.doEvent(new DelBranchKeyEvent(childIndex, WithChild.RIGHT));
        }

        // Handle underflow.
        if (parent.obj.usedSpace >= this.branchMinUsedSpace) { return; }

        const parentSpace = parent.obj.usedSpace + BRANCH_OVERHEAD;
        const lUncle = uncles.left && cl
            .getStoreCast<BranchObj, BranchEvent>(this.branchNamespace)
            .getPage(uncles.left.node);

        // Try stealing from the left uncle.
        if (lUncle && lUncle.obj.usedSpace + parentSpace > this.pageSize) {
            parent.doEvent(new AddBranchKeyEvent(
                0,
                lUncle.obj.children[lUncle.obj.children.length - 1],
                uncles.left!.splitKey,
                WithChild.LEFT,
            ));
            const newSplitKey = lUncle.obj.keys[lUncle.obj.keys.length - 1];
            lUncle.doEvent(new DelBranchKeyEvent(
                lUncle.obj.keys.length - 1,
                WithChild.RIGHT,
            ));
            return {
                ty: DeletionResultTy.REASSIGN,
                with: SiblingPos.LEFT,
                newSplitKey,
            };
        }

        const rUncle = uncles.right && cl
            .getStoreCast<BranchObj, BranchEvent>(this.branchNamespace)
            .getPage(uncles.right.node);

        // Try stealing from the right uncle.
        if (rUncle && rUncle.obj.usedSpace + parentSpace > this.pageSize) {
            parent.doEvent(new AddBranchKeyEvent(
                parent.obj.keys.length,
                rUncle.obj.children[0],
                uncles.right!.splitKey,
                WithChild.RIGHT,
            ));
            const newSplitKey = rUncle.obj.keys[0];
            rUncle.doEvent(new DelBranchKeyEvent(0, WithChild.LEFT));
            return {
                ty: DeletionResultTy.REASSIGN,
                with: SiblingPos.RIGHT,
                newSplitKey,
            };
        }

        // Try merging with the left uncle.
        if (lUncle && lUncle.obj.usedSpace + parentSpace <= this.pageSize) {
            this.mergeBranches(lUncle, uncles.left!.splitKey, parent);
            return {
                ty: DeletionResultTy.MERGE,
                with: SiblingPos.LEFT,
            };
        }

        // Try merging with the right uncle.
        if (rUncle && rUncle.obj.usedSpace + parentSpace <= this.pageSize) {
            this.mergeBranches(parent, uncles.right!.splitKey, rUncle);
            return {
                ty: DeletionResultTy.MERGE,
                with: SiblingPos.RIGHT,
            };
        }

        // There are no uncles, so this is the root node.
    }

    private deleteRecursive(
        cl: TxCollection,
        key: string,
        parent: TxPage<NodeObj, NodeEvent>,
        uncles: Siblings,
    ): DeletionResult {
        if (parent.obj.type == "leaf") {
            return this.deleteLeafEntry(
                cl,
                parent as TxPage<LeafObj, LeafEvent>,
                key,
                uncles,
            );
        }

        const childNamespace = this.getChildNamespace(parent.obj);
        const [iLow] = this.flankIndexes(cl, parent.obj, key);

        // Delete from child.
        const siblings = this.getSiblings(parent.obj, iLow + 1);
        const result = this.deleteRecursive(
            cl,
            key,
            cl.getStoreCast<NodeObj, NodeEvent>(childNamespace)
                .getPage(parent.obj.children[iLow + 1]),
            siblings,
        );

        // Handle deletion result.
        if (result.operation) {
            if (result.operation.ty == DeletionResultTy.REASSIGN) {
                if (result.operation.with == SiblingPos.LEFT) {
                    // Reassign the key splitting the child and left sibling.
                    assert(siblings.left);
                    parent.doEvent(new SetBranchKeyEvent(
                        iLow,
                        result.operation.newSplitKey,
                    ));
                } else {
                    // Reassign the key splitting the child and right sibling.
                    assert(siblings.right);
                    parent.doEvent(new SetBranchKeyEvent(
                        iLow + 1,
                        result.operation.newSplitKey,
                    ));
                }
            } else {
                return {
                    oldVal: result.oldVal,
                    operation: this.handleMerge(
                        cl,
                        parent as TxPage<BranchObj, BranchEvent>,
                        uncles,
                        iLow + 1,
                        siblings,
                        result.operation,
                    ),
                };
            }
        }

        return { oldVal: result.oldVal };
    }

    public delete(cl: TxCollection, key: string): string | undefined {
        // Delete starting from the root.
        const root = this.getRoot(cl);
        const result = this.deleteRecursive(cl, key, root, {});

        // handleMerge() may leave a single child on the root. Replace it.
        if (root.obj.keys.length == 0 && root.obj.type == "branch") {
            this.setRoot(
                cl,
                new RootId(
                    this.getChildNamespace(root.obj),
                    root.obj.children[0],
                ),
            );
            root.doEvent(new DeinitBranchEvent());
        }

        return result.oldVal;
    }
}
