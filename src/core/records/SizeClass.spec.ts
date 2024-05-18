import { PageSize } from "../store/IPageStore";
import { SizeClass, getClassThatFits, getSizeClass } from "./SizeClass";

{
    // Check that getClassThatFits() never fails for reasonable page sizes.
    const pageSizes = [
        32,
        33,
        127, 128, 129,
        1003,
        1023, 1024, 1025,
        4090, 4091, 4092, 4093, 4094, 4095, 4096, 4097, 4098, 4099,
        65534, 65535, 65536,
    ];

    for (const pageSize of pageSizes) {
        for (const usedSpace of $range(0, pageSize - 1)) {
            // Request the default size class for the given usage.
            const nearestClass = getSizeClass(
                pageSize as PageSize,
                usedSpace,
            );

            // Keep decreasing the class until getSizeClass() says that the
            // usage would require a reassignment.
            let smallestClass = nearestClass;
            while (
                smallestClass > 0 && getSizeClass(
                    pageSize as PageSize,
                    usedSpace,
                    smallestClass - 1 as SizeClass,
                ) == smallestClass - 1
            ) {
                smallestClass--;
            }

            const fitClass = getClassThatFits(
                pageSize as PageSize,
                pageSize - usedSpace,
            );

            // Check if getClassThatFits returns our class or one smaller than
            // it. Smaller classes do no harm since they have more free space.
            if (fitClass) { assert(fitClass <= smallestClass); }
        }
    }
}
