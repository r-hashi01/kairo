// The kairo worker protocol (protocol/protocol.go is the reference).
//
// Every message is a 4-byte big-endian length, a 1-byte type and a JSON
// body; the length counts the type byte and the body.
//
//   worker  -> runtime  Hello  {"worker": "ts-1", "actions": [...], "credit": 8, "token": "..."}
//   runtime -> worker   Task   {task}
//   worker  -> runtime  Chunk  {"seq": 17, "data": "<base64>"}
//   worker  -> runtime  Result {"seq": 17, "output": {...}}   (grants 1 credit)
//                              {"seq": 17, "wait": {"until": <ms>, "output": {...}}}   (ADR 0045)
//   worker  -> runtime  Credit {"n": 4}
//   runtime -> worker   Cancel {"seq": 17}                    (ADR 0026)
//   runtime -> worker   RunEnd {"run_id": "r1"}               (ADR 0044, if asked for)

export const MAX_FRAME = 64 << 20;

export const MsgType = {
	Hello: 1,
	Task: 2,
	Result: 3,
	Credit: 4,
	Chunk: 5,
	Cancel: 6,
	RunEnd: 7,
} as const;

/** A task as the runtime sends it (task/task.go). */
export interface Task {
	run_id: string;
	step_id: string;
	act: number;
	attempt: number;
	idempotency_key: string;
	action: string;
	destination?: string;
	tenant?: string;
	effect?: string;
	input: unknown;
	params?: unknown;
	seq: number;
	depth?: number;
	[key: string]: unknown;
}

/**
 * A task's result. `unknown`: the outcome is not known (never treated as
 * success). `retryable`: a definite failure that may be retried. `meta` goes
 * to the step's trace (ADR 0034). `rateLimited`: the destination refused the
 * task for its limits (ADR 0039). `wait`: the step waits until `until` (unix
 * milliseconds) and then ends with `output` (ADR 0045); not with `error` or
 * the result's own `output`.
 */
export interface Result {
	output?: unknown;
	error?: string;
	retryable?: boolean;
	unknown?: boolean;
	tokens?: number;
	errorType?: string;
	meta?: unknown;
	rateLimited?: boolean;
	/** `until`: unix milliseconds (rounded down to an integer). */
	wait?: { until: number; output?: unknown };
}

export class ProtocolError extends Error {}

export function resultBody(seq: number, r: Result): Record<string, unknown> {
	const b: Record<string, unknown> = { seq };
	if (r.error !== undefined || r.unknown) b.error = r.error ?? '';
	else if (r.wait) b.wait = { until: Math.floor(r.wait.until), output: r.wait.output ?? null };
	else b.output = r.output ?? null;
	if (r.retryable) b.retryable = true;
	if (r.unknown) b.unknown = true;
	if (r.tokens) b.tokens = r.tokens;
	if (r.errorType) b.error_type = r.errorType;
	if (r.meta !== undefined) b.meta = r.meta;
	if (r.rateLimited) b.rate_limited = true;
	return b;
}

export function encodeFrame(type: number, body: unknown): Buffer {
	const payload = Buffer.from(JSON.stringify(body), 'utf8');
	if (payload.length + 1 > MAX_FRAME) throw new ProtocolError('frame too large');
	const head = Buffer.alloc(5);
	head.writeUInt32BE(payload.length + 1, 0);
	head.writeUInt8(type, 4);
	return Buffer.concat([head, payload]);
}

/** Splits a byte stream into frames: (type, decoded body). */
export class FrameReader {
	private buf: Buffer = Buffer.alloc(0);

	push(data: Buffer): Array<[number, unknown]> {
		this.buf = this.buf.length === 0 ? data : Buffer.concat([this.buf, data]);
		const out: Array<[number, unknown]> = [];
		while (this.buf.length >= 4) {
			const n = this.buf.readUInt32BE(0);
			if (n === 0 || n > MAX_FRAME) throw new ProtocolError('frame too large');
			if (this.buf.length < 4 + n) break;
			const type = this.buf.readUInt8(4);
			const body = n > 1 ? JSON.parse(this.buf.subarray(5, 4 + n).toString('utf8')) : null;
			out.push([type, body]);
			this.buf = this.buf.subarray(4 + n);
		}
		return out;
	}
}
