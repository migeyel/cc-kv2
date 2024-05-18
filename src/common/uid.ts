import sha256 from "./sha256";

// Gather state from local context.
let state = sha256(table.concat([
    "95dcf3246ba2a554d56b14c7804cef52",
    os.getComputerID(),
    os.epoch("ingame"),
    os.epoch("local"),
    os.epoch("utc"),
    os.clock() * 20,
    math.random(2 ** 24),
    math.random(2 ** 24),
    tostring({}),
    tostring({}),
], "|"));

// Mix tick timings in.
{
    const epoch = os.epoch;
    const times = [];
    for (const i of $range(1, 128)) {
        const t0 = epoch("utc");
        let c = 0;
        while (epoch("utc") == t0) { c++; }
        times[i - 1] = c % 2 ** 16;
    }

    const block = string.pack(string.rep("I2", times.length), ...times);
    state = sha256(state + block);
}

export function rand32(): string {
    state = sha256(state + "1");
    return sha256(state + "0");
}

let uidSuffix = string.sub(rand32(), 1, 28);
let uidCounter = 0;

/** Generates a unique 32-byte string. */
export function uid() {
    if (uidCounter == 2 ** 32) {
        uidSuffix = string.sub(rand32(), 1, 28);
        uidCounter = 0;
    }
    return string.pack(">I4c28", uidCounter++, uidSuffix);
}

const FMT = "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x";

/** Generates a new UUID4. */
export function uuid4() {
    const bytes = string.byte(rand32(), 1, -1);
    bytes[6] = bytes[7] & 0x0f | 0x40;
    bytes[8] = bytes[8] & 0x3f | 0x80;
    return string.format(FMT, ...bytes);
}

const PAT =
    "^%x%x%x%x%x%x%x%x%-%x%x%x%x%-4%x%x%x%-[89ab]%x%x%x%-%x%x%x%x%x%x%x%x%x%x%x%x$";

/** Returns whether a string is a valid UUID4. */
export function isUuid4(s: string): boolean {
    return string.find(s, PAT)[0] != undefined && s == string.lower(s);
}
