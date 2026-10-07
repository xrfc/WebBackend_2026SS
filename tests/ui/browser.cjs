/* Real Chromium: interactions, narrow layout, live admin API, stale data and XSS. */
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { chromium } = require("playwright");
const { scenes } = require("../../services/gateway/web/assets/scenarios.js");
const env = { ...process.env };
if (fs.existsSync(".env"))
  for (const line of fs.readFileSync(".env", "utf8").split(/\r?\n/)) {
    const match = line.match(/^([A-Z_]+)=(.*)$/);
    if (match && !env[match[1]])
      env[match[1]] = match[2].replace(/^['"]|['"]$/g, "");
  }
const base = env.LAB_BASE_URL || "http://127.0.0.1:8000";
const output = path.resolve("tests/ui/screenshots");
fs.mkdirSync(output, { recursive: true });
async function waitText(page, selector, expected, timeout = 10000) {
  // Poll through locator utilities; waitForFunction uses eval blocked by the page CSP.
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if ((await page.locator(selector).textContent()).includes(expected)) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("Timed out waiting for " + selector + " to show " + expected);
}
async function run() {
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({
      viewport: { width: 1280, height: 1000 },
    });
    const errors = [];
    page.on("pageerror", (error) => errors.push(error.message));
    let demoApiCalls = 0;
    page.on("request", (request) => {
      if (request.url().includes("/seckill/observe/")) demoApiCalls++;
    });
    await page.goto(base + "/lab");
    for (const width of [1280, 360]) {
      await page.setViewportSize({ width, height: 1000 });
      for (const item of scenes) {
        await page.selectOption("#scenario-picker", item.id);
        for (let i = 1; i < item.steps.length; i++)
          await page.click("#step-next");
        assert.ok(await page.isDisabled("#step-next"));
        const state = item.steps.at(-1).state;
        assert.equal(
          await page.textContent("#demo-orders"),
          String(state.orders),
        );
        assert.equal(
          await page.textContent("#demo-stock"),
          state.stock === null ? "未知" : String(state.stock),
        );
        const overflow = await page.evaluate(
          () => document.documentElement.scrollWidth > innerWidth + 1,
        );
        assert.equal(
          overflow,
          false,
          "horizontal overflow in " + item.id + " at " + width,
        );
        await page.click("#step-prev");
        assert.ok(!(await page.isDisabled("#step-next")));
      }
    }
    assert.equal(
      demoApiCalls,
      0,
      "principle simulation must not call live APIs",
    );
    await page.selectOption("#scenario-picker", "outage");
    await page.click("#step-next");
    await page.screenshot({
      path: path.join(output, "mobile-outage.png"),
      fullPage: true,
    });
    await page.setViewportSize({ width: 1280, height: 1000 });
    await page.selectOption("#scenario-picker", "duplicate");
    for (let i = 0; i < 3; i++) await page.click("#step-next");
    await page.screenshot({
      path: path.join(output, "desktop-duplicate.png"),
      fullPage: true,
    });
    await page.click('[data-tab="guide"]');
    assert.ok(await page.isVisible("#panel-guide"));
    await page.click('[data-tab="live"]');
    assert.ok(await page.isVisible("#login-section"));
    assert.ok(
      env.ADMIN_PASSWORD,
      "ADMIN_PASSWORD required for live browser verification",
    );
    await page.fill("#admin-name", env.ADMIN_USERNAME || "admin");
    await page.fill("#admin-password", env.ADMIN_PASSWORD);
    await page.click("#admin-login button");
    await waitText(page, "#live-message", "采样完成。", 20000);
    assert.equal(await page.locator("#service-health .health-chip").count(), 5);
    assert.ok((await page.locator("#live-activities .activity").count()) > 0);
    await page.locator("details:has(#live-orders) > summary").click();
    await page.locator("#live-orders button").first().click();
    await waitText(page, "#inspect-result", "created");
    assert.ok(
      (await page.textContent("#inspect-result")).includes(
        "MQ 逐请求位置：未跟踪",
      ),
    );
    await page.click("#logout-live");
    await waitText(page, "#live-message", "已注销");
    assert.ok(await page.isVisible("#login-section"));
    // Fault samples are explicitly mocked; the preceding check used the actual stack.
    const hostile = '<img src=x onerror="window.attacked=true">';
    const data = {
      sampled_at: new Date().toISOString(),
      relay_connected: false,
      atomic_snapshot: false,
      outbox: {
        available: false,
        length: null,
        pending: null,
        limit: 10000,
        preview: [],
      },
      queues: [
        { name: "order_create_queue", available: false, reason: "unavailable" },
      ],
      recent_orders: [],
      activities: [
        {
          id: "00000000-0000-4000-8000-000000000001",
          product_id: 1,
          product_name: hostile,
          state: "active",
          price: "0.01",
          initial_stock: 3,
          orders_created: 0,
          released_stock: 0,
          product_available_stock: 2,
          ends_at: 1900000000,
          redis: { available: false },
        },
      ],
    };
    const fulfill = (route, data, status = 200) =>
      route.fulfill({
        status,
        contentType: "application/json",
        body: JSON.stringify({ message: "测试故障样本", data }),
      });
    await page.route("**/login", (route) =>
      fulfill(route, {
        token: "mock-token",
        user: { role: "admin", username: "mock-admin" },
      }),
    );
    let phase = "valid";
    await page.route("**/seckill/observe/overview", (route) =>
      fulfill(
        route,
        phase === "valid" ? data : null,
        phase === "valid" ? 200 : phase === "stale" ? 503 : 401,
      ),
    );
    await page.route("**/lab/api/health", (route) =>
      fulfill(route, { services: [] }),
    );
    await page.fill("#admin-password", "mock-password");
    await page.click("#admin-login button");
    await waitText(page, "#live-message", "采样完成。");
    assert.equal(await page.textContent("#live-outbox"), "不可用");
    assert.equal(await page.locator("#live-activities img").count(), 0);
    assert.equal(
      await page.locator("#live-activities h3").textContent(),
      hostile,
    );
    assert.ok(
      (await page.textContent("#live-queues")).includes("当前数量未知"),
    );
    phase = "stale";
    await page.click("#refresh-live");
    await waitText(page, "#live-freshness", "旧数据");
    phase = "expired";
    await page.click("#refresh-live");
    await waitText(page, "#live-message", "会话失效");
    assert.ok(await page.isVisible("#login-section"));
    assert.equal(await page.locator("#live-activities .activity").count(), 0);
    assert.equal(
      await page.evaluate(() => localStorage.length + sessionStorage.length),
      0,
    );
    assert.deepEqual(errors, []);
    console.log(
      "Browser checks passed: 12 scenarios at desktop/mobile, actual admin observation and request lookup, logout, unavailable/stale samples, expiry and XSS.",
    );
  } finally {
    await browser.close();
  }
}
run().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
