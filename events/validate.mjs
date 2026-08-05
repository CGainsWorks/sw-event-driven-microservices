/**
 * Event schema loader and validator.
 *
 * This module is the single place that knows how to turn the JSON Schema files in
 * schemas/ into compiled validators. It is used three ways:
 *
 *   - `npm test` in this directory, via schemas.test.mjs
 *   - `npm run validate` as a CLI, so CI and a human get the same answer
 *   - imported by the Node services, so a service validates against exactly the same
 *     schema objects the tests do rather than a drifting copy
 *
 * Deliberately dependency-light and synchronous. Schema loading happens once at
 * process start; nothing here is on a hot path.
 */

import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { parse as parseYaml } from 'yaml';

const HERE = dirname(fileURLToPath(import.meta.url));
export const SCHEMA_DIR = join(HERE, 'schemas');
export const SAMPLE_DIR = join(HERE, 'samples');
export const REGISTRY_DIR = join(HERE, 'registry');

/** Schemas that define shared types rather than a concrete event. */
const SHARED_SCHEMAS = new Set(['common.schema.json', 'envelope.schema.json']);

const readJson = (path) => JSON.parse(readFileSync(path, 'utf8'));

/**
 * Compile every schema in schemas/.
 *
 * Ajv resolves the relative $refs (`common.schema.json#/$defs/uuid`) against each
 * schema's absolute $id, so the shared schemas must be added before anything that
 * references them. addSchema is order-independent for resolution but not for the
 * strict-mode checks, so they go in first.
 *
 * @returns {{ajv: import('ajv').default, eventTypes: string[], validatorFor: (eventType: string) => import('ajv').ValidateFunction}}
 */
export function buildValidators() {
  const ajv = new Ajv2020({
    // Every error, not just the first. A producer fixing an event wants the whole list.
    allErrors: true,
    // Strict mode on: it catches mistyped keywords, which in a schema file fail silently
    // and leave a constraint that looks present but validates nothing.
    strict: true,
    // The samples carry $comment and story metadata; those are standard and allowed.
    validateFormats: true,
  });
  addFormats(ajv);

  const files = readdirSync(SCHEMA_DIR)
    .filter((f) => f.endsWith('.schema.json'))
    .sort();

  // Shared definitions first.
  for (const file of files.filter((f) => SHARED_SCHEMAS.has(f))) {
    ajv.addSchema(readJson(join(SCHEMA_DIR, file)));
  }

  const eventTypes = [];
  for (const file of files.filter((f) => !SHARED_SCHEMAS.has(f))) {
    const schema = readJson(join(SCHEMA_DIR, file));
    const eventType = file.replace(/\.schema\.json$/, '');

    // The filename and the title must agree, otherwise the registry config, the topic
    // file and the schema drift apart without anything noticing.
    if (schema.title !== eventType) {
      throw new Error(
        `schema ${file}: title is "${schema.title}" but the filename says "${eventType}". ` +
          `These must match; the filename is what the loader dispatches on.`,
      );
    }
    ajv.addSchema(schema, eventType);
    eventTypes.push(eventType);
  }

  const validatorFor = (eventType) => {
    const validate = ajv.getSchema(eventType);
    if (!validate) throw new Error(`no schema registered for event type "${eventType}"`);
    return validate;
  };

  return { ajv, eventTypes, validatorFor };
}

/**
 * Validate one event against the schema named by its own `event_type`.
 * @returns {{valid: boolean, errors: import('ajv').ErrorObject[]}}
 */
export function validateEvent(validators, event) {
  const eventType = event?.event_type;
  if (typeof eventType !== 'string') {
    return {
      valid: false,
      errors: [{ instancePath: '', message: 'event has no event_type and cannot be dispatched' }],
    };
  }
  const validate = validators.validatorFor(eventType);
  const valid = validate(event);
  return { valid, errors: valid ? [] : validate.errors };
}

export const loadValidSamples = () => readJson(join(SAMPLE_DIR, 'valid.json'));
export const loadInvalidCases = () => readJson(join(SAMPLE_DIR, 'invalid.json'));
export const loadTopics = () => parseYaml(readFileSync(join(HERE, 'topics.yml'), 'utf8'));
export const loadRegistryConfig = () => parseYaml(readFileSync(join(REGISTRY_DIR, 'config.yml'), 'utf8'));

/** Format Ajv errors into something worth reading in a terminal. */
export const formatErrors = (errors) =>
  (errors ?? []).map((e) => `    ${e.instancePath || '<root>'}: ${e.message}`).join('\n');

// --- CLI ------------------------------------------------------------------

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const validators = buildValidators();
  const { samples } = loadValidSamples();
  const { cases } = loadInvalidCases();

  let failures = 0;

  console.log(`Compiled ${validators.eventTypes.length} event schemas.\n`);

  console.log(`Valid samples (${samples.length}) — all must pass:`);
  for (const { name, event } of samples) {
    const { valid, errors } = validateEvent(validators, event);
    if (valid) {
      console.log(`  PASS  ${name}  (${event.event_type})`);
    } else {
      failures++;
      console.log(`  FAIL  ${name}  (${event.event_type})`);
      console.log(formatErrors(errors));
    }
  }

  console.log(`\nInvalid cases (${cases.length}) — all must be rejected:`);
  for (const { name, schema, event } of cases) {
    const validate = validators.validatorFor(schema);
    if (validate(event)) {
      failures++;
      console.log(`  FAIL  ${name}  — was accepted against ${schema} but should not have been`);
    } else {
      const at = validate.errors.map((e) => e.instancePath || '<root>').join(', ');
      console.log(`  PASS  ${name}  — rejected at ${at}`);
    }
  }

  console.log(
    failures === 0
      ? `\nAll ${samples.length + cases.length} checks passed.`
      : `\n${failures} check(s) failed.`,
  );
  process.exit(failures === 0 ? 0 : 1);
}
