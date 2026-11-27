// Plans made from their name alone (ADR 0059): a wait's, and a workflow
// run's. A process that did not make the run (one that resumes or cancels
// it, the run viewer) can have them.

const PLAN_WAIT = 'kairo.wait/';

/** The root of the plan of a wait for signal name, timing out after timeoutMs (0: never). */
export function waitRoot(name: string, timeoutMs = 0) {
	return { kind: 'wait', id: 'w', signal: name, ...(timeoutMs > 0 ? { timeout: `${timeoutMs}ms` } : {}) };
}

/** A wait's plan from its name (kairo.wait/<signal>[@<ms>]): one made by another process (ADR 0059). */
export function waitPlan(name: string): { name: string; root: unknown } | undefined {
	if (!name.startsWith(PLAN_WAIT)) return undefined;
	const rest = name.slice(PLAN_WAIT.length);
	const i = rest.lastIndexOf('@');
	if (i >= 0 && /^\d+$/.test(rest.slice(i + 1)) && Number(rest.slice(i + 1)) > 0) {
		return { name, root: waitRoot(rest.slice(0, i), Number(rest.slice(i + 1))) };
	}
	return { name, root: waitRoot(rest) };
}

/** A workflow run's plan: it waits for the function's outcome (ADR 0059). */
export const WORKFLOW_PLAN = {
	name: 'kairo.workflow',
	root: { kind: 'wait', id: 'done', signal: 'done' },
	vars: { started_at: { type: 'integer', value: 0 } },
};

/** The plan of name if it is made from its name: a wait's or a workflow run's. */
export function namedPlan(name: string): { name: string; [k: string]: unknown } | undefined {
	return name === WORKFLOW_PLAN.name ? WORKFLOW_PLAN : waitPlan(name);
}
