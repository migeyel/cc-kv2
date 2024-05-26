import { Deque, DequeNode } from "../Deque";
import { rand32 } from "../uid";
import * as connectionApi from "./connectionApi";

/** How frequently keepalive echo requests should be sent. */
export const ECHO_FREQ_SECONDS = 5;

/** How many seconds it takes for a connection to expire. */
export const EXPIRE_SECONDS = 10;

assert(ECHO_FREQ_SECONDS < EXPIRE_SECONDS);

/** An ongoing connection between kv2 and a client. */
export class Connection {
    /** A function to reply to the connection's peer. */
    private reply: (reply: any) => void;

    /** A unique ID describing this connection. */
    public readonly connectionId: string;

    /** A hook to call when the connection closes. */
    private closeHook?: () => void;

    /** os.clock() value when the connection last received a message. */
    public lastRecv: number;

    /** Whether the connection is open. */
    private isOpen = true;

    /** Next expected seq value for incoming messages. */
    private recvSeq = 0;

    /** Next seq value for outgoing messages. */
    private sendSeq = 0;

    private state: ConnectionState;

    public constructor(
        state: ConnectionState,
        connectionId: string,
        reply: (reply: any) => void,
    ) {
        this.state = state;
        this.connectionId = connectionId;
        this.reply = reply;
        this.lastRecv = os.clock();
        this.state.add(this);
        this.state.onActivity(this);
    }

    /** Sets a hook to call when the connection closes. */
    public onClose(hook?: () => void) {
        this.closeHook = hook;
    }

    /** Handles an incoming connection message. */
    public handleMessage(
        message: connectionApi.ConnectionMessage,
    ): ConnectionTransportMessage | undefined {
        if (!this.isOpen) { return; }
        this.lastRecv = os.clock();
        this.state.onActivity(this);
        if (message.ty == "goodbye") {
            this.close();
            return;
        } else {
            if (message.seq != this.recvSeq++) {
                this.close();
                return;
            }
            if (message.ty == "echo.request") {
                this.reply(
                    <connectionApi.EchoResponse>{
                        tag: connectionApi.TAG,
                        ty: "echo.response",
                        connectionId: this.connectionId,
                        seq: this.sendSeq++,
                    },
                );
            } else if (message.ty == "echo.response") {
                // Nothing left to do.
            } else if (message.ty == "transport") {
                return {
                    connection: this,
                    payload: message.payload,
                };
            } else {
                message satisfies never;
            }
        }
    }

    /** Sends an echo request. No-op on closed connections. */
    public requestEcho() {
        if (!this.isOpen) { return; }
        this.reply(
            <connectionApi.EchoRequest>{
                tag: connectionApi.TAG,
                ty: "echo.request",
                connectionId: this.connectionId,
                seq: this.sendSeq++,
            },
        );
    }

    /** Sends a message. No-op on closed connections. */
    public send(message: any) {
        if (!this.isOpen) { return; }
        this.reply(
            <connectionApi.TransportMessage>{
                tag: connectionApi.TAG,
                ty: "transport",
                connectionId: this.connectionId,
                payload: message,
                seq: this.sendSeq++,
            },
        );
    }

    /** Closes the connection. No-op on closed connections. */
    public close() {
        if (!this.isOpen) { return; }
        this.isOpen = false;
        this.state.delete(this);
        this.reply(
            <connectionApi.Goodbye>{
                tag: connectionApi.TAG,
                ty: "goodbye",
                connectionId: this.connectionId,
            },
        );
        if (this.closeHook) { this.closeHook(); }
    }
}

/** An incoming message from a connection. */
export type ConnectionTransportMessage = {
    connection: Connection,
    payload: any
}

/** Manages all connections. */
export class ConnectionManager {
    private state = new ConnectionState();

    public closeAll() {
        this.state.closeAll();
    }

    /**
     * Receives messages and transforms them into ConnectionTransportMessages when
     * appropriate.
     */
    public onMessage(
        message: any,
        reply: (reply: any) => void,
    ): ConnectionTransportMessage | undefined {
        if (!connectionApi.isMessage(message)) { return; }
        if (message.ty == "hello.request") {
            const clientRandom = message.clientRandom;
            const serverRandom = rand32();
            const connId = connectionApi.mkConnectionId(clientRandom, serverRandom);
            new Connection(this.state, connId, reply);
            reply(<connectionApi.HelloResponse>{
                tag: connectionApi.TAG,
                ty: "hello.response",
                clientRandom,
                serverRandom,
            });
        } else if (message.ty == "hello.response") {
            // Not meant for us since we don't send hello requests out.
        } else if (message.ty == "goodbye") {
            this.state.get(message.connectionId)?.close();
        } else {
            const conn = this.state.get(message.connectionId);
            if (conn) { return conn.handleMessage(message); }
        }
    }

    /**
     * Performs connection bookkeeping.
     *
     * Users should run this function whenever a message arrives, and set a timer to run
     * it again after the number of seconds it returns. Returns nil if there are no
     * connections.
     */
    public doBookkeeping(): number | undefined {
        // Close expired connections.
        while (true) {
            const conn = this.state.getNextExpired();
            if (!conn) { break; }
            conn.close();
        }

        // Send keepalive echo requests.
        while (true) {
            const conn = this.state.popNextEcho();
            if (!conn) { break; }
            conn.requestEcho();
        }

        return this.state.getSleepPeriod();
    }
}

/** Shared state held by all connections and their manager. */
export class ConnectionState {
    /** Open connections indexed by ID. */
    private connectionsById = new LuaMap<string, Connection>();

    /** Queue of all connections waiting for the next keepalive echo. */
    private echoQueue = new Deque<Connection>();
    private echoQueueMap = new LuaMap<Connection, DequeNode<Connection>>();

    /** Queue of all connections with a sent echo, in deadline order. */
    private expireQueue = new Deque<Connection>();
    private expireQueueMap = new LuaMap<Connection, DequeNode<Connection>>();

    /** Adds a new connection. */
    public add(connection: Connection) {
        this.connectionsById.set(connection.connectionId, connection);
        this.echoQueueMap.set(connection, this.echoQueue.pushBack(connection));
    }

    /** Gets a connection from its id. */
    public get(connectionId: string): Connection | undefined {
        return this.connectionsById.get(connectionId);
    }

    /** Removes a closed connection from the state's data structures. */
    public delete(connection: Connection) {
        this.expireQueueMap.get(connection)?.pop();
        this.expireQueueMap.delete(connection);
        this.echoQueueMap.get(connection)?.pop();
        this.echoQueueMap.delete(connection);
        this.connectionsById.delete(connection.connectionId);
    }

    /** Bumps a connection to the back of the echo queue. */
    public onActivity(connection: Connection) {
        this.expireQueueMap.get(connection)?.pop();
        this.expireQueueMap.delete(connection);
        this.echoQueueMap.get(connection)?.pop();
        this.echoQueueMap.set(connection, this.echoQueue.pushBack(connection));
    }

    /**
     * Returns how many seconds remain for the next connection to need an echo request
     * or expire. Returns nil if there are no connections.
     */
    public getSleepPeriod(): number | undefined {
        const echoLastRecv = this.echoQueue.first()?.val.lastRecv || math.huge;
        const expireLastRecv = this.expireQueue.first()?.val.lastRecv || math.huge;
        const echoDeadline = echoLastRecv + ECHO_FREQ_SECONDS;
        const expireDeadline = expireLastRecv + EXPIRE_SECONDS;
        const out = math.min(echoDeadline, expireDeadline) - os.clock();
        if (out != math.huge) { return out; }
    }

    /** Returns the next connection that needs an echo request sent through, if any. */
    public popNextEcho(): Connection | undefined {
        const first = this.echoQueue.first();
        if (!first || os.clock() < first.val.lastRecv + ECHO_FREQ_SECONDS) { return; }
        const conn = first.pop();
        this.echoQueueMap.delete(conn);
        this.echoQueueMap.set(conn, this.expireQueue.pushBack(conn));
        return conn;
    }

    /** Returns the next connection that has timed out, if any. */
    public getNextExpired(): Connection | undefined {
        const conn = this.expireQueue.first()?.val;
        if (conn && os.clock() >= conn.lastRecv + EXPIRE_SECONDS) { return conn; }
    }

    /** Closes connections that have gone past the idle expiry timeout. */
    public closeExpired() {
        while (true) {
            const first = this.expireQueue.first();
            if (!first) { return; }
            if (first.val.lastRecv > os.clock()) { return; }
            first.val.close();
        }
    }

    /** Closes all connections. */
    public closeAll() {
        for (const [_, c] of this.connectionsById) { c.close(); }
    }
}
