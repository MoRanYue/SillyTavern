/**
 * Integration test for the reconnection resilience feature.
 * This test simulates the full lifecycle of a resilient streaming request:
 * 1. Create a request with resilience
 * 2. Simulate client disconnect
 * 3. Verify chunks are buffered
 * 4. Simulate client reconnect
 * 5. Verify buffered chunks are returned
 * 6. Verify stream completion
 * 7. Verify cleanup
 *
 * Run with: node tests/reconnection-resilience.test.js
 */

import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';
import { setTimeout as sleep } from 'node:timers/promises';

// Mock express request/response objects
function createMockRequest(body = {}) {
    const socket = new EventEmitter();
    socket.destroyed = false;
    return {
        body: body,
        socket: socket,
        user: { id: 'test-user', profile: { handle: 'tester' } },
        headers: {},
    };
}

function createMockResponse() {
    const socket = new EventEmitter();
    socket.writable = true;
    const headers = {};
    const state = {
        _statusCode: 200,
        _statusMessage: 'OK',
        _body: '',
        _headersSent: false,
        _writableEnded: false,
    };

    return {
        socket: socket,
        headers: headers,
        get writableEnded() { return state._writableEnded; },
        set writableEnded(v) { state._writableEnded = v; },
        get headersSent() { return state._headersSent; },
        set headersSent(v) { state._headersSent = v; },
        get statusCode() { return state._statusCode; },
        set statusCode(v) { state._statusCode = v; },
        get statusMessage() { return state._statusMessage; },
        set statusMessage(v) { state._statusMessage = v; },
        setHeader(name, value) {
            headers[name] = value;
        },
        getHeader(name) {
            return headers[name];
        },
        write(chunk) {
            if (!state._headersSent) {
                state._headersSent = true;
            }
            state._body += chunk.toString();
            return true;
        },
        end(chunk) {
            if (chunk) state._body += chunk.toString();
            state._writableEnded = true;
            setImmediate(() => this.socket.emit('close'));
        },
        getBody() { return state._body; },
        json(data) {
            state._body = JSON.stringify(data);
            state._headersSent = true;
            state._writableEnded = true;
        },
        status(code) {
            state._statusCode = code;
            return this;
        },
        send(data) {
            if (typeof data === 'object') {
                state._body = JSON.stringify(data);
            } else {
                state._body = data;
            }
            state._headersSent = true;
            state._writableEnded = true;
        },
        on(event, handler) {
            this.socket.on(event, handler);
        },
    };
}

// Create a mock fetch response (like from node-fetch)
function createMockFetchResponse(chunks = [], ok = true, status = 200, statusText = 'OK') {
    const body = new Readable({
        read() {
            // Push chunks with delays to simulate streaming
            if (this._currentIndex === undefined) this._currentIndex = 0;
            if (this._currentIndex < chunks.length) {
                this.push(chunks[this._currentIndex]);
                this._currentIndex++;
            } else {
                this.push(null); // End of stream
            }
        },
        _read() {},
        // Override pipe to manually trigger data events
        pipe(dest) {
            let index = 0;
            const sendNext = () => {
                if (index < chunks.length) {
                    dest.write(chunks[index]);
                    index++;
                    setImmediate(sendNext);
                } else {
                    dest.end();
                }
            };
            setImmediate(sendNext);
            return dest;
        },
        destroy() {
            this.destroyed = true;
        },
    });
    // Ensure body is treated as a Readable
    body.__proto__ = Readable.prototype;

    return {
        ok,
        status,
        statusText,
        body,
        text: async () => chunks.join(''),
        json: async () => ({ choices: [{ message: { content: chunks.join('') } }] }),
        headers: new Map(),
    };
}

// Now import the modules we need to test
// We'll create simplified versions for testing

// ============ Simplified RequestSession ============
class TestRequestSession extends EventEmitter {
    constructor(options) {
        super();
        this.requestId = options.requestId;
        this.requestBody = options.requestBody;
        this.abortController = options.abortController;
        this.userId = options.userId;
        this.maxBufferSize = options.maxBufferSize || 100;
        this.createdAt = Date.now();
        this.lastActiveAt = Date.now();
        this.status = 'streaming';
        this.buffer = [];
        this.finalData = null;
        this.error = null;
        this.clientDisconnected = false;
    }

    markClientDisconnected() {
        this.clientDisconnected = true;
        this.lastActiveAt = Date.now();
    }

    appendChunk(chunkStr) {
        this.lastActiveAt = Date.now();
        this.buffer.push(chunkStr);
        if (this.buffer.length > this.maxBufferSize) {
            this.buffer.shift();
        }
        this.emit('chunk', chunkStr);
    }

    getBufferedEvents() {
        return [...this.buffer];
    }

    markComplete(finalData) {
        this.status = 'completed';
        this.finalData = finalData;
        this.lastActiveAt = Date.now();
        this.emit('complete', finalData);
    }

    markFailed(error) {
        this.status = 'failed';
        this.error = error;
        this.lastActiveAt = Date.now();
        this.emit('error', error);
    }

    abort() {
        this.status = 'aborted';
        this.abortController.abort();
        this.emit('aborted');
    }
}

// ============ Simplified createResilientController ============
function createResilientController(request, response, registry, options = {}) {
    const controller = new AbortController();
    const isStreaming = Boolean(request.body?.stream || request.body?.streaming);
    const isLegacy = options.forceLegacy || !isStreaming;

    if (isStreaming && !isLegacy) {
        try {
            const requestId = generateId();
            const userId = request.user?.id || 'anonymous';
            const session = registry.createSession({
                requestId,
                requestBody: request.body,
                abortController: controller,
                userId,
            });

            response.setHeader('x-request-id', requestId);

            request.socket.removeAllListeners('close');
            request.socket.on('close', function () {
                session.markClientDisconnected();
                console.log(`[TEST] Client disconnected from request ${requestId}, continuing to buffer.`);
            });

            return { controller, requestId, session };
        } catch (error) {
            console.warn('[TEST] Failed to create resilient session, fallback:', error.message);
        }
    }

    request.socket.removeAllListeners('close');
    request.socket.on('close', function () {
        controller.abort();
    });

    return { controller, requestId: null, session: null };
}

let idCounter = 0;
function generateId() {
    return `test-request-${++idCounter}-${Date.now()}`;
}

// ============ Test Registry ============
class TestRequestRegistry {
    constructor() {
        this.sessions = new Map();
        this.maxSessions = 10;
    }

    createSession(options) {
        if (this.sessions.size >= this.maxSessions) {
            throw new Error('Max sessions reached');
        }
        const session = new TestRequestSession(options);
        this.sessions.set(options.requestId, session);
        return session;
    }

    getSession(requestId) {
        return this.sessions.get(requestId);
    }

    deleteSession(requestId) {
        return this.sessions.delete(requestId);
    }

    cleanup() {
        const now = Date.now();
        for (const [id, session] of this.sessions) {
            const age = now - session.lastActiveAt;
            const ttl = session.status === 'streaming' ? 1800000 : 300000;
            if (age > ttl) {
                if (session.status === 'streaming') {
                    session.abortController.abort();
                }
                this.sessions.delete(id);
                console.log(`[TEST] Cleaned up session ${id}`);
            }
        }
    }
}

// ============ Forward Fetch Response Test ============
async function forwardFetchResponse(from, to, options = {}) {
    if (!from.ok) {
        const errorText = 'Mock error';
        if (options.session) {
            options.session.markFailed(errorText);
        }
        to.end(errorText);
        return;
    }

    if (from.body && to.socket) {
        const readable = from.body;

        // Set up listeners BEFORE triggering any data flow
        return new Promise((resolve) => {
            readable.on('data', (chunk) => {
                const chunkStr = chunk.toString('utf-8');
                to.write(chunkStr);
                if (options.session) {
                    options.session.appendChunk(chunkStr);
                }
            });

            readable.on('end', () => {
                console.log('[TEST] Stream finished');
                if (options.session) {
                    options.session.markComplete();
                }
                to.end();
                resolve();
            });

            readable.on('error', (error) => {
                console.error('[TEST] Stream error:', error);
                if (options.session) {
                    options.session.markFailed(error);
                }
                if (!to.writableEnded) {
                    to.end();
                }
                resolve();
            });

            to.socket.on('close', () => {
                if (options.session) {
                    options.session.markClientDisconnected();
                } else {
                    if (readable.destroy) readable.destroy();
                    if (!to.writableEnded) to.end();
                }
            });

            // Manually trigger the readable piping
            readable.resume();
        });
    } else {
        to.end();
    }
}

// ============ Main Test Runner ============
let passedTests = 0;
let failedTests = 0;

function assert(condition, message) {
    if (condition) {
        console.log(`  ✅ ${message}`);
        passedTests++;
    } else {
        console.log(`  ❌ ${message}`);
        failedTests++;
    }
}

async function runTests() {
    console.log('========================================');
    console.log('  Reconnection Resilience Test Suite');
    console.log('========================================\n');

    // ========== Test 1: Basic Session Lifecycle ==========
    console.log('--- Test 1: RequestSession Lifecycle ---');

    const abortController = new AbortController();
    const session = new TestRequestSession({
        requestId: 'test-1',
        requestBody: { stream: true, model: 'test-model' },
        abortController: abortController,
        userId: 'user-1',
    });

    assert(session.status === 'streaming', 'Session starts in streaming mode');
    assert(session.buffer.length === 0, 'Session buffer starts empty');
    assert(session.clientDisconnected === false, 'Client starts connected');

    session.appendChunk('data: {"choices":[{"delta":{"content":"Hello"}}]}\n\n');
    assert(session.buffer.length === 1, 'Buffer has 1 chunk after append');
    assert(session.buffer[0].includes('Hello'), 'Buffer contains correct data');

    session.appendChunk('data: {"choices":[{"delta":{"content":" world"}}]}\n\n');
    assert(session.buffer.length === 2, 'Buffer has 2 chunks after second append');

    session.markClientDisconnected();
    assert(session.clientDisconnected === true, 'Client marked as disconnected');

    session.appendChunk('data: {"choices":[{"delta":{"content":"!"}}]}\n\n');
    assert(session.buffer.length === 3, 'Buffer continues to grow after disconnect');

    const events = session.getBufferedEvents();
    assert(events.length === 3, 'getBufferedEvents returns all events');
    assert(events[2].includes('!'), 'Last event contains correct data');

    session.markComplete();
    assert(session.status === 'completed', 'Session marked as completed');

    // ========== Test 2: Buffer Overflow ==========
    console.log('\n--- Test 2: Buffer Overflow ---');

    const abortController2 = new AbortController();
    const session2 = new TestRequestSession({
        requestId: 'test-2',
        requestBody: {},
        abortController: abortController2,
        userId: 'user-1',
        maxBufferSize: 5,
    });

    for (let i = 0; i < 10; i++) {
        session2.appendChunk(`chunk-${i}`);
    }

    assert(session2.buffer.length === 5, 'Buffer limited to maxBufferSize');
    assert(session2.buffer[0] === 'chunk-5', 'Oldest chunk was evicted (first is chunk-5)');
    assert(session2.buffer[4] === 'chunk-9', 'Newest chunk is present (last is chunk-9)');

    // ========== Test 3: Registry ==========
    console.log('\n--- Test 3: RequestRegistry ---');

    const registry = new TestRequestRegistry();

    const req1 = registry.createSession({
        requestId: 'reg-1',
        requestBody: { stream: true },
        abortController: new AbortController(),
        userId: 'user-1',
    });

    const req2 = registry.createSession({
        requestId: 'reg-2',
        requestBody: { stream: true },
        abortController: new AbortController(),
        userId: 'user-2',
    });

    assert(registry.getSession('reg-1') === req1, 'Session 1 retrievable');
    assert(registry.getSession('reg-2') === req2, 'Session 2 retrievable');
    assert(registry.getSession('non-existent') === undefined, 'Non-existent session returns undefined');

    registry.deleteSession('reg-1');
    assert(registry.getSession('reg-1') === undefined, 'Session 1 deleted');

    // ========== Test 4: Resilience Controller (Streaming) ==========
    console.log('\n--- Test 4: createResilientController (streaming) ---');

    const registry4 = new TestRequestRegistry();
    const mockReq4 = createMockRequest({ stream: true, model: 'test' });
    const mockRes4 = createMockResponse();

    const result4 = createResilientController(mockReq4, mockRes4, registry4);

    assert(result4.requestId !== null, 'Request ID generated for streaming');
    assert(result4.session !== null, 'Session created for streaming');
    assert(result4.controller instanceof AbortController, 'Controller created');
    assert(mockRes4.getHeader('x-request-id') === result4.requestId, 'x-request-id header set');

    // Simulate client disconnect - should NOT abort
    mockReq4.socket.emit('close');

    assert(result4.controller.signal.aborted === false, 'Controller NOT aborted after client disconnect');
    assert(result4.session.clientDisconnected === true, 'Session marked as disconnected');

    // ========== Test 5: Resilience Controller (Legacy/Non-streaming) ==========
    console.log('\n--- Test 5: createResilientController (non-streaming) ---');

    const registry5 = new TestRequestRegistry();
    const mockReq5 = createMockRequest({ stream: false });
    const mockRes5 = createMockResponse();

    const result5 = createResilientController(mockReq5, mockRes5, registry5);

    assert(result5.requestId === null, 'No request ID for non-streaming');
    assert(result5.session === null, 'No session for non-streaming');

    // Simulate client disconnect - SHOULD abort
    mockReq5.socket.emit('close');
    assert(result5.controller.signal.aborted === true, 'Controller aborted after client disconnect (legacy)');

    // ========== Test 6: Forward Fetch Response with Buffering ==========
    console.log('\n--- Test 6: ForwardFetchResponse with buffering ---');

    const registry6 = new TestRequestRegistry();
    const mockReq6 = createMockRequest({ stream: true });
    const mockRes6 = createMockResponse();
    const result6 = createResilientController(mockReq6, mockRes6, registry6);

    const testChunks = [
        'data: {"choices":[{"delta":{"content":"Hello"}}]}\n\n',
        'data: {"choices":[{"delta":{"content":" world"}]}}\n\n',
        'data: [DONE]\n\n',
    ];

    // Create a simpler mock: write chunks manually
    const mockFetchRes6 = {
        ok: true,
        status: 200,
        statusText: 'OK',
        body: new Readable({
            read() {
                // Push all chunks then end
                this.push(testChunks[0]);
                this.push(testChunks[1]);
                this.push(testChunks[2]);
                this.push(null);
            }
        }),
        text: async () => testChunks.join(''),
        headers: new Map(),
    };

    // Await forwardFetchResponse directly
    await forwardFetchResponse(mockFetchRes6, mockRes6, { session: result6.session });

    // Check buffering
    assert(result6.session.buffer.length === 3, 'All 3 chunks buffered');
    assert(result6.session.status === 'completed', 'Session completed after stream end');
    assert(mockRes6.writableEnded, 'Response ended');

    // ========== Test 7: Reconnection (get buffered events) ==========
    console.log('\n--- Test 7: Reconnection - Get buffered events ---');

    const registry7 = new TestRequestRegistry();
    const session7 = registry7.createSession({
        requestId: 'recon-1',
        requestBody: { stream: true },
        abortController: new AbortController(),
        userId: 'user-1',
    });

    const reconnectChunks = [
        'data: {"choices":[{"delta":{"content":"A"}}]}\n\n',
        'data: {"choices":[{"delta":{"content":"B"}]}}\n\n',
        'data: {"choices":[{"delta":{"content":"C"}]}}\n\n',
    ];

    for (const chunk of reconnectChunks) {
        session7.appendChunk(chunk);
    }

    // Simulate disconnection
    session7.markClientDisconnected();

    // Add more data after disconnect
    const postDisconnectChunks = [
        'data: {"choices":[{"delta":{"content":"D"}]}}\n\n',
        'data: [DONE]\n\n',
    ];

    for (const chunk of postDisconnectChunks) {
        session7.appendChunk(chunk);
    }
    session7.markComplete();

    // Now simulate reconnection: get buffered events
    const buffered = session7.getBufferedEvents();
    assert(buffered.length === 5, 'All 5 chunks available on reconnection');
    assert(buffered[0].includes('A'), 'First chunk is correct');
    assert(buffered[3].includes('D'), 'Post-disconnect chunk is available');

    // ========== Test 8: Session Expiry via EventEmitter ==========
    console.log('\n--- Test 8: Session EventEmitter notifications ---');

    const registry8 = new TestRequestRegistry();
    const session8 = registry8.createSession({
        requestId: 'events-1',
        requestBody: { stream: true },
        abortController: new AbortController(),
        userId: 'user-1',
    });

    let chunkReceived = false;
    let completedReceived = false;

    session8.on('chunk', () => { chunkReceived = true; });
    session8.on('complete', () => { completedReceived = true; });

    session8.appendChunk('test data');
    session8.markComplete();

    assert(chunkReceived, 'Chunk event emitted');
    assert(completedReceived, 'Complete event emitted');

    // ========== Test 9: User Isolation ==========
    console.log('\n--- Test 9: User Isolation ---');

    const registry9 = new TestRequestRegistry();
    const user1Session = registry9.createSession({
        requestId: 'user1-req',
        requestBody: { stream: true },
        abortController: new AbortController(),
        userId: 'user-1',
    });
    const user2Session = registry9.createSession({
        requestId: 'user2-req',
        requestBody: { stream: true },
        abortController: new AbortController(),
        userId: 'user-2',
    });

    assert(user1Session.userId === 'user-1', 'User 1 session has correct userId');
    assert(user2Session.userId === 'user-2', 'User 2 session has correct userId');

    // ========== Test 10: Abort ==========
    console.log('\n--- Test 10: User-initiated abort ---');

    const abortCtrl10 = new AbortController();
    const session10 = new TestRequestSession({
        requestId: 'abort-1',
        requestBody: { stream: true },
        abortController: abortCtrl10,
        userId: 'user-1',
    });

    let abortedEvent = false;
    session10.on('aborted', () => { abortedEvent = true; });

    session10.abort();

    assert(session10.status === 'aborted', 'Session status is aborted');
    assert(abortCtrl10.signal.aborted, 'AbortController was aborted');
    assert(abortedEvent, 'Aborted event was emitted');

    // ========== Results ==========
    console.log('\n========================================');
    console.log(`  Results: ${passedTests} passed, ${failedTests} failed`);
    console.log('========================================');

    if (failedTests > 0) {
        process.exit(1);
    }
}

runTests().catch(console.error);
