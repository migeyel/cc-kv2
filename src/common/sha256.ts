const SEP = string.char(0x80);
const NUL = string.char(0x00);

const pack = string.pack;
const unpack = string.unpack;

const band = bit32.band;
const bnot = bit32.bnot;
const bxor = bit32.bxor;
const shr = bit32.rshift;
const rol = bit32.lrotate;

function primes(n: number, exp: number): number[] {
    const out = [];
    let p = 2;
    for (const i of $range(1, n)) {
        out[i - 1] = bxor(p ** exp % 1 * 2 ** 32);
        do { p++; } while (2 ** p % p != 2);
    }
    return out;
}

const H = primes(8, 1 / 2);
const K = primes(64, 1 / 3);

export default function sha256(data: string): string {
    const bitlen = data.length * 8;
    const padlen = -(data.length + 9) % 64;
    data = data + (SEP + (string.rep(NUL, padlen) + pack(">I8", bitlen)));

    let [h0, h1, h2, h3, h4, h5, h6, h7] = H;
    const k = K;

    for (const i of $range(1, data.length, 64)) {
        const w = unpack(">I4I4I4I4I4I4I4I4I4I4I4I4I4I4I4I4", data, i);

        for (const j of $range(17, 64)) {
            const wf = w[j - 15 - 1];
            const w2 = w[j - 2 - 1];
            const s0 = bxor(rol(wf, 25), rol(wf, 14), shr(wf, 3));
            const s1 = bxor(rol(w2, 15), rol(w2, 13), shr(w2, 10));
            w[j - 1] = w[j - 16 - 1] + s0 + w[j - 7 - 1] + s1;
        }

        let [a, b, c, d, e, f, g, h] = [h0, h1, h2, h3, h4, h5, h6, h7];
        for (const j of $range(1, 64)) {
            const s1 = bxor(rol(e, 26), rol(e, 21), rol(e, 7));
            const ch = bxor(band(e, f), band(bnot(e), g));
            const t1 = h + s1 + ch + k[j - 1] + w[j - 1];
            const s0 = bxor(rol(a, 30), rol(a, 19), rol(a, 10));
            const maj = bxor(band(a, b), band(a, c), band(b, c));
            const t2 = s0 + maj;

            h = g;
            g = f;
            f = e;
            e = d + t1;
            d = c;
            c = b;
            b = a;
            a = t1 + t2;
        }

        h0 = (h0 + a) % 2 ** 32;
        h1 = (h1 + b) % 2 ** 32;
        h2 = (h2 + c) % 2 ** 32;
        h3 = (h3 + d) % 2 ** 32;
        h4 = (h4 + e) % 2 ** 32;
        h5 = (h5 + f) % 2 ** 32;
        h6 = (h6 + g) % 2 ** 32;
        h7 = (h7 + h) % 2 ** 32;
    }

    return string.pack(">I4I4I4I4I4I4I4I4", h0, h1, h2, h3, h4, h5, h6, h7);
}
