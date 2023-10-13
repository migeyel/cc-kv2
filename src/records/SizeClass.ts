import { MAX_PAGE_NUM, PageSize } from "../store/IPageStore";
import { uIntLenBytes } from "../util";

/**
 * A class that a page belongs to, based on how full it is. This number varies
 * from 0 to MAX_SIZE_CLASS, inclusive.
 *
 * Size classes exist to solve the free space finding problem: How do you find
 * a page with enough free space to insert a record without wasting space?
 *
 * Pages are partitioned into several linked lists, one for each class. Finding
 * free space amounts to checking for any pages with a class at or below some
 * target occupancy rate.
 *
 * If the number of classes is low, performance improves at the expense of more
 * wasted space:
 * - Adding/removing records will often keep the page in the same class, which
 *   prevents expensive linked-list bookkeeping.
 * - Almost-full pages will never receive new records. This gives more space for
 *   existing records to grow in-place and prevents record pointer changes and
 *   associated costs.
 */
export type SizeClass = number & { readonly __brand: unique symbol };

/** The maximum size class we divide pages in. */
export const MAX_SIZE_CLASS = 24;

/** The byte size of a linked list link between pages. */
export const PAGE_LINK_BYTES = uIntLenBytes(MAX_PAGE_NUM);

/** How many bytes it takes to store a size class. */
export const SIZE_CLASS_BYTES = uIntLenBytes(MAX_SIZE_CLASS);

/**
 * Controls how much each size class overlaps with its neighbors.
 *
 * The overlap prevents pages being repeatedly reassigned due to just a few
 * bytes of occupancy changing. This improves performance.
 */
const CLASS_OVERLAP = 0.25;

/**
 * Gets a page's size class.
 * @param pageSize - The maximum page size.
 * @param usedSpace - The current page size.
 * @param oldClass - The old page size class.
 * @returns The new page size class.
 */
export function getSizeClass(
    pageSize: PageSize,
    usedSpace: number,
    oldClass?: SizeClass,
): SizeClass {
    // If the old class is within limits, keep it.
    if (
        oldClass &&
        usedSpace >= getMinUsedSpace(pageSize, oldClass) &&
        usedSpace <= getMaxUsedSpace(pageSize, oldClass)
    ) {
        return oldClass;
    }

    // Compute and return the nearest class.
    const scaleFactor = MAX_SIZE_CLASS / pageSize;
    const scaled = scaleFactor * usedSpace;
    return math.min(math.floor(scaled + 0.5), MAX_SIZE_CLASS) as SizeClass;
}

/** Returns the minimum used space for a page in a given size class. */
export function getMinUsedSpace(
    pageSize: PageSize,
    sizeClass: SizeClass,
): number {
    const scaleFactor = MAX_SIZE_CLASS / pageSize;
    const raw = (sizeClass - (1 + CLASS_OVERLAP) / 2) / scaleFactor;
    return math.max(0, math.ceil(raw));
}

/** Returns the maximum used space for a page in a given size class. */
export function getMaxUsedSpace(
    pageSize: PageSize,
    sizeClass: SizeClass,
): number {
    const scaleFactor = MAX_SIZE_CLASS / pageSize;
    const raw = (sizeClass + (1 + CLASS_OVERLAP) / 2) / scaleFactor;
    return math.min(pageSize, math.floor(raw));
}

/**
 * Returns the largest size class with a guaranteed amount of free space
 * @param pageSize - The maximum page size.
 * @param minFreeSpace - The smallest acceptable amount of free space.
 * @returns The matching size class, or nil if none do.
 */
export function getClassThatFits(
    pageSize: PageSize,
    minFreeSpace: number,
): SizeClass | undefined {
    const maxUsedSpace = pageSize - minFreeSpace;
    const scaleFactor = MAX_SIZE_CLASS / pageSize;

    // Use arithmetic to approximate the result.
    let minClass = maxUsedSpace * scaleFactor - (1 + CLASS_OVERLAP) / 2;
    minClass = math.min(MAX_PAGE_NUM, math.max(0, math.floor(minClass)));

    // Adjust upwards.
    while (
        minClass < MAX_PAGE_NUM &&
        getMaxUsedSpace(pageSize, minClass + 1 as SizeClass) <= maxUsedSpace
    ) {
        minClass++;
    }

    // Check if we've reached the page limit.
    if (
        minClass == MAX_PAGE_NUM &&
        getMaxUsedSpace(pageSize, minClass as SizeClass) <= maxUsedSpace
    ) {
        return;
    }

    // Adjust downwards.
    while (
        minClass > 0 &&
        getMaxUsedSpace(pageSize, minClass - 1 as SizeClass) > maxUsedSpace
    ) {
        minClass--;
    }

    return minClass as SizeClass;
}
