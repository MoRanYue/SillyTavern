import { EventEmitter } from 'node:events';
import { getConfigValue } from './util.js';

/**
 * @typedef {object} RequestSessionOptions
 * @property {string} requestId - Unique request ID
 * @property {object} requestBody - Original request body
 * @property {AbortController} abortController - Controller to abort the LLM request
 * @property {string} userId - User ID who owns this request
 * @property {number} [maxBufferSize] - Max number of buffered events
 */

/**
 * Represents a single active generation session.
 * Extends EventEmitter to notify reconnected clients of new chunks.
 */
class RequestSession extends EventEmitter {
    /**
     * @param {RequestSessionOptions} options
     */
    constructor(options) {
        super();
        this.requestId = options.requestId;
        this.requestBody = options.requestBody;
        this.abortController = options.abortController;
        this.userId = options.userId;
        this.maxBufferSize = options.maxBufferSize || 10000;
        this.createdAt = Date.now();
        this.lastActiveAt = Date.now();
        this.status = 'streaming'; // 'streaming' | 'completed' | 'failed' | 'aborted'
        /** @type {string[]} */
        this.buffer = [];
        this.finalData = null;
        this.error = null;
        this.clientDisconnected = false;
    }

    /**
     * Marks the client as disconnected. The LLM request continues.
     */
    markClientDisconnected() {
        this.clientDisconnected = true;
        this.lastActiveAt = Date.now();
    }

    /**
     * Appends a chunk (SSE event string) to the buffer and notifies listeners.
     * @param {string} chunkStr
     */
    appendChunk(chunkStr) {
        this.lastActiveAt = Date.now();
        this.buffer.push(chunkStr);
        if (this.buffer.length > this.maxBufferSize) {
            this.buffer.shift();
        }
        this.emit('chunk', chunkStr);
    }

    /**
     * Returns all currently buffered events.
     * @returns {string[]}
     */
    getBufferedEvents() {
        return [...this.buffer];
    }

    /**
     * Marks the request as completed successfully.
     * @param {any} [finalData]
     */
    markComplete(finalData) {
        this.status = 'completed';
        this.finalData = finalData;
        this.lastActiveAt = Date.now();
        this.emit('complete', finalData);
        this.removeAllListeners();
    }

    /**
     * Marks the request as failed.
     * @param {any} error
     */
    markFailed(error) {
        this.status = 'failed';
        this.error = error;
        this.lastActiveAt = Date.now();
        this.emit('error', error);
        this.removeAllListeners();
    }

    /**
     * Aborts the request (user-initiated stop).
     */
    abort() {
        this.status = 'aborted';
        this.abortController.abort();
        this.emit('aborted');
        this.removeAllListeners();
    }
}

/**
 * Manages all active generation sessions.
 * Provides cleanup for expired sessions.
 */
class RequestRegistry {
    constructor() {
        /** @type {Map<string, RequestSession>} */
        this.sessions = new Map();

        /** @type {number} */
        this.maxSessions = getConfigValue('requestRegistry.maxSessions', 20, 'number');

        /** @type {number} */
        this.sessionTTL = getConfigValue('requestRegistry.sessionTTL', 5 * 60 * 1000, 'number'); // 5 min for completed/failed

        /** @type {number} */
        this.offlineSessionTTL = getConfigValue('requestRegistry.offlineSessionTTL', 30 * 60 * 1000, 'number'); // 30 min for disconnected streaming

        /** @type {number} */
        this.cleanupInterval = getConfigValue('requestRegistry.cleanupInterval', 60 * 1000, 'number'); // 1 min

        this.#startCleanup();
    }

    /**
     * Creates a new request session.
     * @param {RequestSessionOptions} options
     * @returns {RequestSession}
     */
    createSession(options) {
        if (this.sessions.size >= this.maxSessions) {
            // Try to evict oldest completed/failed sessions first
            this.#evictStaleSessions();
            if (this.sessions.size >= this.maxSessions) {
                throw new Error('Maximum number of active sessions reached. Please try again later.');
            }
        }

        const session = new RequestSession(options);
        this.sessions.set(options.requestId, session);
        return session;
    }

    /**
     * Gets a session by request ID.
     * @param {string} requestId
     * @returns {RequestSession|undefined}
     */
    getSession(requestId) {
        return this.sessions.get(requestId);
    }

    /**
     * Removes and returns a session.
     * @param {string} requestId
     * @returns {boolean}
     */
    deleteSession(requestId) {
        return this.sessions.delete(requestId);
    }

    /**
     * Starts the periodic cleanup timer.
     */
    #startCleanup() {
        setInterval(() => {
            try {
                this.#cleanup();
            } catch (error) {
                console.error('RequestRegistry cleanup error:', error);
            }
        }, this.cleanupInterval).unref();
    }

    /**
     * Removes expired sessions from the map.
     */
    #cleanup() {
        const now = Date.now();
        for (const [id, session] of this.sessions) {
            const age = now - session.lastActiveAt;
            const ttl = (session.status === 'streaming' && session.clientDisconnected)
                ? this.offlineSessionTTL
                : this.sessionTTL;

            if (age > ttl) {
                // If still streaming, abort the underlying request
                if (session.status === 'streaming') {
                    session.abortController.abort();
                }
                this.sessions.delete(id);
            }
        }
    }

    /**
     * Evicts stale sessions to make room for new ones.
     */
    #evictStaleSessions() {
        // Remove sessions that are already completed/failed/aborted first
        for (const [id, session] of this.sessions) {
            if (session.status !== 'streaming') {
                this.sessions.delete(id);
                if (this.sessions.size < this.maxSessions) return;
            }
        }
        // If still full, remove the oldest streaming sessions
        const streamingSessions = [...this.sessions.entries()]
            .filter(([, s]) => s.status === 'streaming')
            .sort(([, a], [, b]) => a.createdAt - b.createdAt);

        for (const [id] of streamingSessions) {
            this.sessions.delete(id);
            if (this.sessions.size < this.maxSessions) return;
        }
    }
}

/** @type {RequestRegistry} */
let globalRegistry = null;

/**
 * Gets or creates the global RequestRegistry instance.
 * @returns {RequestRegistry}
 */
export function getRequestRegistry() {
    if (!globalRegistry) {
        globalRegistry = new RequestRegistry();
    }
    return globalRegistry;
}

export { RequestSession, RequestRegistry };
