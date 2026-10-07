const assert = require("node:assert/strict");
const test = require("node:test");
const { scenes } = require("../../services/gateway/web/assets/scenarios.js");
const scene = (id) => scenes.find((item) => item.id === id);
const last = (id) => scene(id).steps.at(-1).state;

test("MQ outage retains accepted work until recovery", () => {
  const pending = scene("outage").steps[2].state;
  assert.equal(pending.stream, 1);
  assert.equal(pending.stock, 2);
  assert.equal(pending.orders, 0);
  assert.equal(last("outage").orders, 1);
  assert.equal(last("outage").stream, 0);
});
test("duplicate clicks, relay retry and consumer restart keep one order", () => {
  for (const id of ["click", "duplicate", "restart"]) {
    assert.equal(last(id).orders, 1);
    assert.equal(last(id).stock, 2);
    assert.equal(last(id).users, 1);
  }
  assert.equal(scene("duplicate").steps[3].state.mq, 2);
});
test("close retries release exactly two unsold units, never accepted work", () => {
  for (const id of ["close", "closefail"]) {
    assert.equal(last(id).product, 4);
    assert.equal(last(id).released, 2);
    assert.equal(last(id).users, 1);
  }
  assert.equal(scene("closefail").steps[2].state.product, 2);
});
test("missing Redis is unknown, never rebuilt to initial stock", () => {
  assert.equal(last("lost").stock, null);
  assert.equal(last("lost").users, null);
  assert.match(last("lost").http, /503/);
});
test("dead letters and backlog do not silently refund or deduct", () => {
  assert.equal(last("dead").stock, 2);
  assert.equal(last("dead").dead, 1);
  assert.equal(last("dead").status, "manual_review");
  assert.equal(last("backlog").stock, 3);
  assert.equal(last("backlog").users, 0);
  assert.equal(last("backlog").stream, 10000);
});
test("every state has a verification and an interview boundary", () => {
  assert.equal(new Set(scenes.map((item) => item.id)).size, scenes.length);
  for (const item of scenes) {
    assert.ok(item.question && item.answer && item.file);
    for (const step of item.steps)
      assert.ok(step.change && step.why && step.verify);
  }
});
