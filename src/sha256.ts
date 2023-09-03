const K = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
    0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
    0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
    0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
    0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
    0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

const SEP = string.char(0x80);
const NUL = string.char(0x00);

const FMT_16x4 = ">I4I4I4I4I4I4I4I4I4I4I4I4I4I4I4I4";

const pack = string.pack;
const unpack = string.unpack;

const band = bit32.band;
const bnot = bit32.bnot;
const bxor = bit32.bxor;
const shr = bit32.rshift;
const rol = bit32.lrotate;

export default function sha256(data: string): string {
    const bitlen = data.length * 8;
    const padlen = -(data.length + 9) % 64;
    data = data + (SEP + (string.rep(NUL, padlen) + pack(">I8", bitlen)));

    let h0 = 0x6a09e667;
    let h1 = 0xbb67ae85;
    let h2 = 0x3c6ef372;
    let h3 = 0xa54ff53a;
    let h4 = 0x510e527f;
    let h5 = 0x9b05688c;
    let h6 = 0x1f83d9ab;
    let h7 = 0x5be0cd19;

    const k = K;

    for (const i of $range(1, data.length, 64)) {
        const w = unpack(FMT_16x4, data, i);

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
