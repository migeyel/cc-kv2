/**
 * Computes how many bytes it takes to store a nonnegative integer in a range.
 * @param max - The maximum value possible in the range (inclusive).
 * @returns How many bytes it takes to store an integer in that range.
 */
export function uIntLenBytes(max: number): number {
    return math.ceil(math.log(1 + max, 256));
}
