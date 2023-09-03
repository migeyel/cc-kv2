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
    fs.getFreeSpace("/"),
    string.dump(() => {}),
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

export default function uuid4() {
    state = sha256(state + "1");
    const bytes = string.byte(sha256(state + "0"), 1, -1);
    bytes[6] = bytes[7] & 0x0f | 0x40;
    bytes[8] = bytes[8] & 0x3f | 0x80;
    return string.format(
        string.gsub("xx-x-x-x-xxx", "x", "%%02x%%02x")[0],
        ...bytes,
    );
}
