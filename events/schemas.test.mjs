/**
 * Tests for the event contract itself.
 *
 * These are not tests of any service. They test that the schemas, the topic topology and
 * the registry configuration agree with each other and say what they claim to say. The
 * point is that a schema file cannot be added, renamed or altered without the topic file
 * and the registry config being updated to match — the three drift apart silently
 * otherwise, and the first sign of it is a consumer failing in production.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readdirSync } from 'node:fs';

import {
  buildValidators,
  validateEvent,
  loadValidSamples,
  loadInvalidCases,
  loadTopics,
  loadRegistryConfig,
  formatErrors,
  SCHEMA_DIR,
} from './validate.mjs';

const validators = buildValidators();
const { samples } = loadValidSamples();
const { cases } = loadInvalidCases();
const topics = loadTopics();
const registry = loadRegistryConfig();

/** `orders.v1.OrderPlaced` -> `orders.v1.events` */
const topicForEventType = (eventType) => {
  const [context, version] = eventType.split('.');
  return `${context}.${version}.events`;
};

/** Which field in `data` the aggregate_id must equal, per aggregate type. */
const PRIMARY_ID_FIELD = {
  order: 'order_id',
  payment: 'payment_id',
  reservation: 'reservation_id',
  notification: 'notification_id',
};

describe('schema compilation', () => {
  it('compiles every schema in schemas/ under Ajv strict mode', () => {
    // buildValidators() throws on a malformed schema or a title/filename mismatch.
    // Reaching here at all is most of the assertion.
    assert.ok(validators.eventTypes.length > 0, 'no event schemas were found');
  });

  it('has a schema for every event type named in topics.yml and no orphans', () => {
    const declaredInTopics = topics.topics.flatMap((t) => t.schemas).sort();
    const onDisk = [...validators.eventTypes].sort();
    assert.deepEqual(
      onDisk,
      declaredInTopics,
      'schemas/ and topics.yml disagree about which events exist',
    );
  });

  it('names every schema file after the event type it defines', () => {
    const files = readdirSync(SCHEMA_DIR).filter(
      (f) => f.endsWith('.schema.json') && !['common.schema.json', 'envelope.schema.json'].includes(f),
    );
    for (const file of files) {
      const eventType = file.replace(/\.schema\.json$/, '');
      assert.match(
        eventType,
        /^[a-z]+\.v[0-9]+\.[A-Za-z]+$/,
        `${file}: filename is not a valid <context>.v<major>.<Name> event type`,
      );
    }
  });
});

describe('valid samples', () => {
  it('covers every event type with at least one sample', () => {
    const covered = new Set(samples.map((s) => s.event.event_type));
    const missing = validators.eventTypes.filter((t) => !covered.has(t));
    assert.deepEqual(missing, [], `event types with no sample in samples/valid.json: ${missing}`);
  });

  for (const { name, event } of samples) {
    it(`${name} (${event.event_type}) validates`, () => {
      const { valid, errors } = validateEvent(validators, event);
      assert.ok(valid, `expected ${name} to validate but:\n${formatErrors(errors)}`);
    });
  }

  it('keeps aggregate_id equal to the primary id inside data', () => {
    // The Kafka message key is aggregate_id. If it disagrees with the id in the payload,
    // events for one aggregate land on different partitions and lose their ordering —
    // a bug that is invisible until two events race.
    for (const { name, event } of samples) {
      const field = PRIMARY_ID_FIELD[event.aggregate_type];
      assert.ok(field, `${name}: unknown aggregate_type ${event.aggregate_type}`);
      assert.equal(
        event.aggregate_id,
        event.data[field],
        `${name}: aggregate_id does not match data.${field}`,
      );
    }
  });

  it('gives every event in a story the same correlation_id', () => {
    const byStory = new Map();
    for (const { story, event } of samples) {
      if (!byStory.has(story)) byStory.set(story, new Set());
      byStory.get(story).add(event.correlation_id);
    }
    for (const [story, ids] of byStory) {
      assert.equal(ids.size, 1, `story ${story} uses ${ids.size} correlation ids, expected 1`);
    }
  });

  it('only ever cites a causation_id that is an event_id in the same story', () => {
    // Except for the deliberately omitted origin events, documented in valid.json.
    const OMITTED_ORIGIN = /-0{12}$/;
    const byStory = new Map();
    for (const { story, event } of samples) {
      if (!byStory.has(story)) byStory.set(story, new Set());
      byStory.get(story).add(event.event_id);
    }
    for (const { name, story, event } of samples) {
      if (!event.causation_id || OMITTED_ORIGIN.test(event.causation_id)) continue;
      assert.ok(
        byStory.get(story).has(event.causation_id),
        `${name}: causation_id ${event.causation_id} is not an event in story ${story}`,
      );
    }
  });

  it('never lets an effect precede its cause', () => {
    const at = new Map(samples.map(({ event }) => [event.event_id, event.occurred_at]));
    for (const { name, event } of samples) {
      const causeAt = event.causation_id && at.get(event.causation_id);
      if (!causeAt) continue;
      assert.ok(
        event.occurred_at >= causeAt,
        `${name}: occurred_at ${event.occurred_at} is before its cause at ${causeAt}`,
      );
    }
  });
});

describe('invalid cases', () => {
  for (const { name, schema, why, expect_error_at, event } of cases) {
    it(`rejects ${name}: ${why}`, () => {
      const validate = validators.validatorFor(schema);
      const valid = validate(event);
      assert.equal(valid, false, `${name} was accepted against ${schema} but must be rejected`);

      const flagged = validate.errors.map((e) => e.instancePath);
      assert.ok(
        flagged.includes(expect_error_at),
        `${name} was rejected, but not at ${expect_error_at || '<root>'}. ` +
          `Errors were at: ${flagged.map((p) => p || '<root>').join(', ')}. ` +
          `A case that fails for the wrong reason is not evidence the constraint works.`,
      );
    });
  }
});

describe('topic topology', () => {
  it('routes every event type to the topic its name implies', () => {
    for (const topic of topics.topics) {
      for (const eventType of topic.schemas) {
        assert.equal(
          topicForEventType(eventType),
          topic.name,
          `${eventType} is listed on topic ${topic.name} but its name implies ${topicForEventType(eventType)}`,
        );
      }
    }
  });

  it('gives every topic a dead letter topic', () => {
    const dlqs = new Set(topics.dead_letter.topics);
    for (const topic of topics.topics) {
      const expected = `${topic.name}${topics.dead_letter.suffix}`;
      assert.ok(dlqs.has(expected), `topic ${topic.name} has no dead letter topic (${expected})`);
    }
    assert.equal(
      dlqs.size,
      topics.topics.length,
      'dead_letter.topics contains an entry with no matching live topic',
    );
  });

  it('names an owner that matches the producer const in the schemas', () => {
    for (const topic of topics.topics) {
      for (const eventType of topic.schemas) {
        const schema = validators.ajv.getSchema(eventType).schema;
        const refinement = schema.allOf.find((s) => s.properties?.producer);
        assert.equal(
          refinement.properties.producer.const,
          topic.owner,
          `${eventType} declares producer "${refinement.properties.producer.const}" but topic ${topic.name} is owned by ${topic.owner}`,
        );
      }
    }
  });

  it('lists notifications as a consumer of every upstream topic', () => {
    // notifications is the only cross-service subscriber. If it stops being listed on a
    // topic, either the wiring changed or this file went stale.
    for (const topic of topics.topics) {
      if (topic.owner === 'notifications') continue;
      assert.ok(
        topic.consumers.includes('notifications'),
        `topic ${topic.name} does not list notifications as a consumer`,
      );
    }
  });
});

describe('schema registry configuration', () => {
  it('registers every schema exactly once', () => {
    const registered = registry.schemas.map((s) => s.file.replace('schemas/', '').replace(/\.schema\.json$/, ''));
    assert.deepEqual(
      [...registered].sort(),
      [...validators.eventTypes].sort(),
      'registry/config.yml and schemas/ disagree about which schemas exist',
    );
    assert.equal(new Set(registered).size, registered.length, 'a schema is registered twice');
  });

  it('builds every subject with TopicRecordNameStrategy', () => {
    assert.equal(registry.subject_naming_strategy, 'TopicRecordNameStrategy');
    for (const entry of registry.schemas) {
      const eventType = entry.file.replace('schemas/', '').replace(/\.schema\.json$/, '');
      assert.equal(
        entry.subject,
        `${topicForEventType(eventType)}-${eventType}`,
        `subject for ${eventType} does not follow <topic>-<event_type>`,
      );
    }
  });

  it('registers the shared schemas that the event schemas reference', () => {
    const refs = registry.references.map((r) => r.subject);
    assert.ok(refs.includes('common.schema.json'), 'common.schema.json is not registered');
    assert.ok(refs.includes('envelope.schema.json'), 'envelope.schema.json is not registered');
  });

  it('keeps payment schemas on transitive compatibility', () => {
    // Financial topics are replayed from offset 0 during reconciliation, so every
    // historical version must stay readable, not just the previous one.
    const override = registry.overrides.find((o) => o.subject_pattern === 'payments.v1.events-*');
    assert.ok(override, 'payments has no compatibility override');
    assert.equal(override.compatibility, 'BACKWARD_TRANSITIVE');
  });
});
