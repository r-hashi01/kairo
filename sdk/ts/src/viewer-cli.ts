#!/usr/bin/env node
// kairo-view: opens the run viewer (ADR 0088) on a database of the embedded
// runtime. It does not run workflows: the processes that run them go on
// with what it sends.
//
//   kairo-view path/to/kairo.db [--prefix kairo_] [--port 0] [--host 127.0.0.1]
//   kairo-view --postgres postgres://... [--prefix kairo_]   (with "pg" installed where it runs)

import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { parseArgs } from 'node:util';

import { EmbeddedBackend } from './backend.ts';
import { PostgresStore, SQLiteStore, type Store } from './store.ts';
import { serveViewer } from './viewer.ts';
import { Kairo } from './workflow.ts';

const USAGE = `usage: kairo-view <SQLite file> [--prefix kairo_] [--port N] [--host 127.0.0.1|::1]
       kairo-view --postgres <url> [--prefix kairo_] [--port N] [--host 127.0.0.1|::1]`;

async function main(): Promise<void> {
	const { values, positionals } = parseArgs({
		allowPositionals: true,
		options: {
			postgres: { type: 'string' },
			prefix: { type: 'string' },
			port: { type: 'string' },
			host: { type: 'string' },
			help: { type: 'boolean', short: 'h' },
		},
	});
	if (values.help || (!values.postgres && positionals.length !== 1) || (values.postgres && positionals.length > 0)) {
		console.error(USAGE);
		process.exit(values.help ? 0 : 2);
	}
	const host = values.host ?? '127.0.0.1';
	if (host !== '127.0.0.1' && host !== '::1') throw new Error('--host: 127.0.0.1 or ::1 (the viewer listens on loopback only)');
	const port = values.port === undefined ? 0 : Number(values.port);
	if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error(`--port ${values.port}: not a port`);

	let store: Store;
	let pool: { end(): Promise<void> } | undefined;
	if (values.postgres) {
		let pg: any;
		try {
			pg = createRequire(join(process.cwd(), 'x.js'))('pg');
		} catch {
			throw new Error('--postgres needs node-postgres ("pg") installed where kairo-view runs (npm install pg)');
		}
		const p = new pg.Pool({ connectionString: values.postgres, max: 4 });
		pool = p;
		store = new PostgresStore(p, { prefix: values.prefix });
	} else {
		const path = positionals[0]!;
		// Opening a file that is not there would make a new, empty database.
		if (!existsSync(path)) throw new Error(`${path}: no such database`);
		store = new SQLiteStore(path, { prefix: values.prefix, notify: true });
	}

	const k = new Kairo({ backend: await EmbeddedBackend.open({ store }) });
	const viewer = await serveViewer(k, { host, port });
	console.log(`kairo-view: ${viewer.url}`);
	console.log('Anyone with this URL can see the runs and act on them. Ctrl-C stops.');
	let stopping = false;
	const stop = async () => {
		if (stopping) return;
		stopping = true;
		await viewer.close();
		await k.close();
		await pool?.end();
		process.exit(0);
	};
	process.on('SIGINT', () => void stop());
	process.on('SIGTERM', () => void stop());
}

main().catch((e) => {
	console.error(`kairo-view: ${e instanceof Error ? e.message : String(e)}`);
	process.exit(1);
});
