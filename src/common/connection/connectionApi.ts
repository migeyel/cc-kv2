/**
 * A stable API for establishing and managing connections.
 *
 * The connection API is used to separate connection housekeeping from the kv2 server
 * logic. The inner messages are opaque to the connection layer, which makes it easier
 * to implement proxies for forwarding queries over different communication media.
 *
 * @module
 */

import sha256 from "../sha256";

/** A tag for identifying connectionApi messages. */
export const TAG = "53cbf833-8f52-48a9-b9df-38036941296c";

/** Requests a connection be established. */
export type HelloRequest = {
    tag: typeof TAG,
    ty: "hello.request",
    clientRandom: string,
};

/** Establishes a connection. */
export type HelloResponse = {
    tag: typeof TAG,
    ty: "hello.response",
    clientRandom: string,
    serverRandom: string,
};

/** Asks for an echo from the other party. */
export type EchoRequest = {
    tag: typeof TAG,
    ty: "echo.request",
    connectionId: string,
    seq: number,
};

/** Echo from the other party. */
export type EchoResponse = {
    tag: typeof TAG,
    ty: "echo.response",
    connectionId: string,
    seq: number,
}

/** A transport message. */
export type TransportMessage = {
    tag: typeof TAG,
    ty: "transport",
    connectionId: string,
    payload: any,
    seq: number,
}

/** Closes the connection. */
export type Goodbye = {
    tag: typeof TAG,
    ty: "goodbye",
    connectionId: string,
};

/** All possible messages belonging to a connection. */
export type ConnectionMessage =
    | EchoRequest
    | EchoResponse
    | TransportMessage
    | Goodbye;

/** All possible messages sent by the API. */
export type Message =
    | HelloRequest
    | HelloResponse
    | EchoRequest
    | EchoResponse
    | TransportMessage
    | Goodbye;

export function isMessage(t: any): t is Message {
    const u = t as Message;
    if (type(u) != "table") { return false; }
    if (u.tag != TAG) { return false; }
    if (u.ty == "hello.request") {
        if (type(u.clientRandom) != "string") { return false; }
        if (u.clientRandom.length != 32) { return false; }
        return true;
    } else if (u.ty == "hello.response") {
        if (type(u.clientRandom) != "string") { return false; }
        if (u.clientRandom.length != 32) { return false; }
        if (type(u.serverRandom) != "string") { return false; }
        if (u.serverRandom.length != 32) { return false; }
        return true;
    } else if (u.ty == "echo.request") {
        if (type(u.connectionId) != "string") { return false; }
        if (u.connectionId.length != 32) { return false; }
        if (type(u.seq) != "number") { return false; }
        return true;
    } else if (u.ty == "echo.response") {
        if (type(u.connectionId) != "string") { return false; }
        if (u.connectionId.length != 32) { return false; }
        if (type(u.seq) != "number") { return false; }
        return true;
    } else if (u.ty == "transport") {
        if (type(u.connectionId) != "string") { return false; }
        if (u.connectionId.length != 32) { return false; }
        if (type(u.seq) != "number") { return false; }
        return true;
    } else if (u.ty == "goodbye") {
        if (type(u.connectionId) != "string") { return false; }
        if (u.connectionId.length != 32) { return false; }
        return true;
    } else {
        return false;
    }
}

export function mkConnectionId(clientRandom: string, serverRandom: string): string {
    assert(clientRandom.length == 32 && serverRandom.length == 32);
    return sha256(clientRandom + serverRandom);
}
