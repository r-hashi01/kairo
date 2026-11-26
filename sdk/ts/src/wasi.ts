// The few WASI preview1 functions kairo.wasm imports, in place of node:wasi
// (issue #108): Node.js marks node:wasi experimental and warns on every
// load. The core is pure (ADR 0001, 0051): it reads no files, no
// arguments and no environment, and takes time from events. What it
// imports is the Go runtime's: a clock and randomness for its scheduler
// and hash seeds, stdout and stderr for a panic, and the checks it makes
// of descriptors as it starts. No directory is opened to it.

import { writeSync } from 'node:fs';
import { randomFillSync } from 'node:crypto';

const SUCCESS = 0;
const BADF = 8;
const INVAL = 28;

const CLOCK_REALTIME = 0;
const FILETYPE_CHARACTER_DEVICE = 2;

/** Thrown by proc_exit: the module asked to stop. */
export class WasiExit extends Error {
	readonly code: number;
	constructor(code: number) {
		super(`kairo.wasm exited with code ${code}`);
		this.code = code;
	}
}

/** The import object for kairo.wasm, and how to start it once instantiated. */
export function wasiImports(): { imports: WebAssembly.Imports; start(instance: WebAssembly.Instance): void } {
	let memory: WebAssembly.Memory | undefined;
	const view = () => new DataView(memory!.buffer);
	const bytes = () => new Uint8Array(memory!.buffer);
	const stdio = (fd: number) => fd >= 0 && fd <= 2;
	const none = (countPtr: number, sizePtr: number) => {
		const v = view();
		v.setUint32(countPtr, 0, true);
		v.setUint32(sizePtr, 0, true);
		return SUCCESS;
	};
	const wasi = {
		args_sizes_get: none,
		args_get: () => SUCCESS,
		environ_sizes_get: none,
		environ_get: () => SUCCESS,
		clock_time_get(id: number, _precision: bigint, timePtr: number) {
			const ns = id === CLOCK_REALTIME ? BigInt(Date.now()) * 1_000_000n : process.hrtime.bigint();
			view().setBigUint64(timePtr, ns, true);
			return SUCCESS;
		},
		random_get(ptr: number, len: number) {
			randomFillSync(bytes(), ptr, len);
			return SUCCESS;
		},
		fd_write(fd: number, iovs: number, iovsLen: number, nwrittenPtr: number) {
			if (fd !== 1 && fd !== 2) return BADF;
			const v = view();
			let n = 0;
			for (let i = 0; i < iovsLen; i++) {
				const ptr = v.getUint32(iovs + i * 8, true);
				const len = v.getUint32(iovs + i * 8 + 4, true);
				n += writeSync(fd, bytes(), ptr, len);
			}
			v.setUint32(nwrittenPtr, n, true);
			return SUCCESS;
		},
		fd_read(fd: number, _iovs: number, _iovsLen: number, nreadPtr: number) {
			if (fd !== 0) return BADF;
			view().setUint32(nreadPtr, 0, true); // stdin is empty
			return SUCCESS;
		},
		fd_close: (fd: number) => (stdio(fd) ? SUCCESS : BADF),
		fd_fdstat_get(fd: number, ptr: number) {
			if (!stdio(fd)) return BADF;
			const v = view();
			v.setUint8(ptr, FILETYPE_CHARACTER_DEVICE);
			v.setUint16(ptr + 2, 0, true); // flags
			v.setBigUint64(ptr + 8, 0xffff_ffff_ffff_ffffn, true); // rights
			v.setBigUint64(ptr + 16, 0xffff_ffff_ffff_ffffn, true); // inherited rights
			return SUCCESS;
		},
		fd_fdstat_set_flags: (fd: number) => (stdio(fd) ? SUCCESS : BADF),
		// No preopened directory: the first call says there is none.
		fd_prestat_get: () => BADF,
		fd_prestat_dir_name: () => BADF,
		// The core never waits; the Go scheduler may poll when idle. Every
		// subscription is reported ready at once, which a poller must take
		// as a wake that may come early.
		poll_oneoff(inPtr: number, outPtr: number, nsubs: number, neventsPtr: number) {
			if (nsubs === 0) return INVAL;
			const v = view();
			for (let i = 0; i < nsubs; i++) {
				const sub = inPtr + i * 48;
				const ev = outPtr + i * 32;
				v.setBigUint64(ev, v.getBigUint64(sub, true), true); // userdata
				v.setUint16(ev + 8, SUCCESS, true); // error
				v.setUint8(ev + 10, v.getUint8(sub + 8)); // type: as subscribed
				for (let b = 11; b < 32; b++) v.setUint8(ev + b, 0); // padding, nbytes, flags
			}
			v.setUint32(neventsPtr, nsubs, true);
			return SUCCESS;
		},
		sched_yield: () => SUCCESS,
		proc_exit(code: number): never {
			throw new WasiExit(code);
		},
	};
	return {
		imports: { wasi_snapshot_preview1: wasi },
		start(instance) {
			memory = instance.exports.memory as WebAssembly.Memory;
			(instance.exports._initialize as () => void)();
		},
	};
}
