import { ConfigEntryComponent, ConfigEntryId } from "./ConfigPageComponent";
import { Namespace, PageNum } from "./store/IPageStore";
import { PAGE_FMT } from "./txStore/LogRecord/types";
import { IEvent, IObj, TxCollection, TxPage } from "./txStore/LogStore";

/**
 * A component for allocating and deallocating pages.
 *
 * Remark: this allocator treats empty page objects as "unallocated". Therefore,
 * you mustn't rely on allocated empty objects staying empty. As soon as an
 * object managed by this allocator is made empty it must be considered free.
 */
export class PageAllocatorComponent {
    private numPagesConfig: ConfigEntryComponent<number>;

    public readonly pagesNamespace: Namespace;

    public constructor(
        numPagesConfig: ConfigEntryId,
        pagesNamespace: Namespace,
    ) {
        this.pagesNamespace = pagesNamespace;
        this.numPagesConfig = new ConfigEntryComponent(
            numPagesConfig,
            "<" + PAGE_FMT,
            0,
        );
    }

    /** Allocates a new page and returns it. */
    public allocPageCast<
        T extends IObj<E>,
        E extends IEvent
    >(cl: TxCollection): TxPage<T, E> {
        const numPages = this.numPagesConfig.get(cl);
        const ns = cl.getStoreCast<T, E>(this.pagesNamespace);

        // Try a random page.
        const attempt = math.random(0, numPages) as PageNum;
        const attemptPage = ns.getPage(attempt);
        if (attemptPage.obj.isEmpty()) { return attemptPage; }

        // Allocate at the end of the block.
        this.numPagesConfig.set(cl, numPages + 1);
        return ns.getPage(numPages as PageNum);
    }

    /**
     * Shrink the page search space to match the existing pages.
     * @param cl - The collection to operate on.
     * @param hint - A hint to a page which was just freed, for performance.
     */
    public freeUnusedPages(cl: TxCollection, hint?: PageNum): void {
        let numPages = this.numPagesConfig.get(cl);
        if (hint && hint != numPages - 1) { return; }
        const ns = cl.getStoreCast<IObj<IEvent>, IEvent>(this.pagesNamespace);
        while (true) {
            const page = ns.getPage(numPages - 1 as PageNum);
            if (page.obj.isEmpty()) { numPages--; } else { break; }
        }
        this.numPagesConfig.set(cl, numPages);
    }

    public deserializeObj(n: Namespace, s?: string): IObj<IEvent> | undefined {
        return this.numPagesConfig.deserializeObj(n, s);
    }

    public deserializeEv(n: Namespace, s: string): IEvent | undefined {
        return this.numPagesConfig.deserializeObj(n, s);
    }
}
