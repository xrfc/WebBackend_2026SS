(function () {
  "use strict";
  const $ = (id) => document.getElementById(id);
  const scenes = window.SeckillLabData.scenes;
  const labels = {
    queued: "queued · 等待订单",
    created: "created · 订单已提交",
    manual_review: "manual_review · 待核查",
    inconsistent: "数据不一致 · 需核查",
  };
  let scenario = scenes[0],
    position = 0,
    activeTab = "demo";
  let token = "",
    sessionVersion = 0,
    refreshing = false,
    timer = null,
    lastSample = null;
  const requests = new Set();
  const number = (value) =>
    value === null || value === undefined
      ? "—"
      : Number(value).toLocaleString("zh-CN");
  function node(tag, text, className) {
    const element = document.createElement(tag);
    if (text !== undefined) element.textContent = text;
    if (className) element.className = className;
    return element;
  }
  function badge(container, text, type = "") {
    container.append(node("span", text, "event " + type));
  }
  function renderSteps() {
    $("step-list").replaceChildren();
    scenario.steps.forEach((step, i) => {
      const li = node("li");
      const button = node("button");
      button.type = "button";
      button.classList.toggle("active", i === position);
      button.setAttribute("aria-current", i === position ? "step" : "false");
      button.append(
        node("span", String(i + 1), "step-number"),
        node("span", step.title),
      );
      button.addEventListener("click", () => {
        position = i;
        renderDemo();
      });
      li.append(button);
      $("step-list").append(li);
    });
  }
  function renderDemo() {
    const step = scenario.steps[position],
      state = step.state;
    $("scenario-summary").textContent = scenario.summary;
    $("step-position").textContent =
      `步骤 ${position + 1} / ${scenario.steps.length}`;
    $("step-title").textContent = step.title;
    $("request-status").textContent = labels[state.status] || state.status;
    $("request-status").classList.toggle(
      "warning",
      ["manual_review", "无法确认"].includes(state.status),
    );
    $("step-prev").disabled = position === 0;
    $("step-next").disabled = position === scenario.steps.length - 1;
    $("http-state").textContent = state.http;
    for (const [id, key] of Object.entries({
      "demo-stock": "stock",
      "demo-users": "users",
      "demo-stream": "stream",
      "demo-mq": "mq",
      "demo-dead": "dead",
      "demo-orders": "orders",
      "demo-product": "product",
    }))
      $(id).textContent = state[key] === null ? "未知" : number(state[key]);
    $("redis-state").textContent = state.redis;
    $("mq-state").textContent = state.broker;
    $("sql-state").textContent = state.sql;
    document
      .querySelectorAll("[data-store]")
      .forEach((item) =>
        item.classList.toggle("focus", item.dataset.store === state.focus),
      );
    $("stock-marks").replaceChildren();
    for (let i = 0; i < 3; i++)
      $("stock-marks").append(
        node(
          "span",
          "",
          state.stock === null || i >= state.stock ? "empty" : "",
        ),
      );
    for (const id of ["redis-events", "mq-events", "sql-events"])
      $(id).replaceChildren();
    if (state.users === 1) badge($("redis-events"), "用户 → R1");
    if (state.stream === 1) badge($("redis-events"), "Stream · R1");
    else if (state.stream > 1)
      badge($("redis-events"), `其他积压 ${number(state.stream)} 条`);
    for (let i = 0; i < Math.min(state.mq, 3); i++) badge($("mq-events"), "R1");
    if (state.dead) badge($("mq-events"), "死信 · R1", "dead");
    if (state.orders) badge($("sql-events"), "O1 · R1");
    $("publish-link").textContent = "Redis → MQ：" + state.published;
    $("consume-link").textContent = "MQ → MySQL：" + state.consumed;
    $("inventory-check").textContent =
      state.stock === null || state.users === null
        ? "库存未知：暂停受理，不能从初始额度直接重建。"
        : state.released
          ? `未售额度已释放 ${state.released} 件；商品可用 ${state.product} 件，已受理 ${state.users} 件仍占用。`
          : `本活动初始额度 3 = 剩余 ${state.stock} + 已受理 ${state.users}；订单已提交 ${state.orders} 张。`;
    $("step-change").textContent = step.change;
    $("step-why").textContent = step.why;
    $("step-verify").textContent = step.verify;
    $("step-code").textContent = "代码位置：" + scenario.file;
    $("demo-json").textContent = JSON.stringify(
      {
        mode: "simulation",
        request: { id: "R1", status: state.status, http: state.http },
        activity: {
          initial_stock: 3,
          redis_remaining: state.stock,
          accepted_users: state.users,
          state: state.activity,
          released_stock: state.released,
        },
        outbox_events: state.stream,
        mq_unfinished: state.mq,
        dead_events: state.dead,
        orders_created: state.orders,
        product_available_stock: state.product,
      },
      null,
      2,
    );
    $("interview-question").textContent = scenario.question;
    $("interview-answer").textContent = scenario.answer;
    renderSteps();
  }
  scenes.forEach((item) => {
    const option = node("option", item.name);
    option.value = item.id;
    $("scenario-picker").append(option);
  });
  $("scenario-picker").addEventListener("change", (event) => {
    scenario = scenes.find((item) => item.id === event.target.value);
    position = 0;
    renderDemo();
  });
  $("step-next").addEventListener("click", () => {
    if (position < scenario.steps.length - 1) {
      position++;
      renderDemo();
    }
  });
  $("step-prev").addEventListener("click", () => {
    if (position > 0) {
      position--;
      renderDemo();
    }
  });
  $("step-reset").addEventListener("click", () => {
    position = 0;
    renderDemo();
  });

  async function api(path, options = {}) {
    const controller = new AbortController();
    requests.add(controller);
    const deadline = setTimeout(() => controller.abort(), 10000);
    try {
      const response = await fetch(path, {
        ...options,
        headers: {
          ...(options.body ? { "Content-Type": "application/json" } : {}),
          ...(token ? { Authorization: "Bearer " + token } : {}),
          ...options.headers,
        },
        signal: controller.signal,
        cache: "no-store",
        credentials: "same-origin",
      });
      const payload = await response.json();
      if (!response.ok) {
        const error = new Error(payload.message || "请求失败");
        error.status = response.status;
        throw error;
      }
      return payload.data;
    } finally {
      clearTimeout(deadline);
      requests.delete(controller);
    }
  }
  function message(text, error = false) {
    $("live-message").textContent = text;
    $("live-message").classList.toggle("error", error);
  }
  function stopTimer() {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
  }
  function schedule() {
    stopTimer();
    if (
      token &&
      activeTab === "live" &&
      !document.hidden &&
      $("auto-refresh").checked
    )
      timer = setTimeout(refreshLive, 5000);
  }
  function clearSession() {
    sessionVersion++;
    token = "";
    lastSample = null;
    stopTimer();
    requests.forEach((controller) => controller.abort());
    requests.clear();
    $("login-section").hidden = false;
    $("live-session").hidden = true;
    $("auto-refresh").checked = false;
    for (const id of [
      "service-health",
      "live-queues",
      "live-activities",
      "live-orders",
      "inspect-result",
    ])
      $(id).replaceChildren();
    $("inspect-id").value = "";
    $("live-stream-preview").textContent = "";
    $("live-outbox").textContent = "—";
    $("live-outbox-detail").textContent = "";
    $("live-relay").textContent = "";
    $("live-freshness").textContent = "";
    refreshing = false;
    $("refresh-live").disabled = false;
  }
  function formatTime(value) {
    return new Date(value).toLocaleString("zh-CN", { hour12: false });
  }
  function renderHealth(services) {
    $("service-health").replaceChildren();
    const names = {
      user: "用户",
      product: "商品",
      order: "订单",
      seckill: "秒杀",
      ai: "AI",
    };
    for (const item of services)
      $("service-health").append(
        node(
          "span",
          `${names[item.name] || item.name} · ${item.ready ? "就绪" : "不可用"} · ${item.duration_ms} ms`,
          "health-chip" + (item.ready ? "" : " down"),
        ),
      );
  }
  function renderOverview(data) {
    const outbox = data.outbox;
    $("live-outbox").textContent = outbox.available
      ? number(outbox.length) + " 条"
      : "不可用";
    $("live-outbox-detail").textContent = outbox.available
      ? `pending ${number(outbox.pending)} 条 · 上限 ${number(outbox.limit)} 条`
      : "数据无法读取，不能解释为积压为零。";
    $("live-relay").textContent =
      "relay 最近连接状态：" + (data.relay_connected ? "已连接" : "未连接");
    $("live-queues").replaceChildren();
    const names = {
      order_create_queue: "订单主队列",
      order_retry_queue: "延迟重试队列",
      order_dead_queue: "死信队列",
    };
    for (const item of data.queues) {
      const row = node("div", undefined, "queue-row");
      row.append(node("strong", names[item.name] || item.name));
      row.append(
        node(
          "p",
          item.available
            ? `Ready ${number(item.ready)} · Unacked ${number(item.unacknowledged)} · Total ${number(item.total)}`
            : item.reason === "statistics_pending"
              ? "统计尚未就绪；当前数量未知。"
              : "队列不可读取；当前数量未知。",
          item.available ? "" : "error",
        ),
      );
      $("live-queues").append(row);
    }
    $("live-activities").replaceChildren();
    if (!data.activities.length)
      $("live-activities").append(
        node("p", "还没有活动。可在 API 文档创建商品与活动。", "muted"),
      );
    for (const item of data.activities) {
      const card = node("article", undefined, "activity");
      card.append(
        node("h3", item.product_name),
        node("p", `商品 #${item.product_id} · ${item.state} · ¥${item.price}`),
        node("p", item.id, "identifier"),
      );
      const dl = node("dl");
      const values = [
        ["初始额度", item.initial_stock],
        [
          "Redis 剩余",
          item.redis.available ? item.redis.remaining_stock : null,
        ],
        ["已受理用户", item.redis.available ? item.redis.accepted_users : null],
        ["最终订单", item.orders_created],
        ["已释放", item.released_stock],
        ["商品可用", item.product_available_stock],
      ];
      for (const [label, value] of values) {
        const group = node("div");
        group.append(node("dt", label), node("dd", number(value)));
        dl.append(group);
      }
      card.append(dl);
      card.append(
        node(
          "p",
          item.redis.available
            ? `Redis 状态 ${item.redis.state}；跨库数值需考虑采样时间差。`
            : "Redis 记录缺失或异常；不能按初始额度自动重建。",
          item.redis.available ? "muted" : "error",
        ),
      );
      if (item.state === "closed")
        card.append(
          node(
            "p",
            "活动已关闭：Redis 历史剩余额度可能已释放，不能再次当作可售库存。",
            "muted",
          ),
        );
      card.append(
        node("p", `结束时间 ${formatTime(item.ends_at * 1000)}`, "muted"),
      );
      $("live-activities").append(card);
    }
    $("live-orders").replaceChildren();
    if (!data.recent_orders.length)
      $("live-orders").append(node("p", "暂无已提交的秒杀订单。", "muted"));
    for (const order of data.recent_orders) {
      const row = node("div", undefined, "order-row");
      row.append(
        node(
          "strong",
          `订单 #${order.id} · ${order.product_name} · ¥${order.price}`,
        ),
        node("div", `用户 #${order.user_id} · 请求 ${order.request_id}`),
      );
      const button = node("button", "核查此请求");
      button.type = "button";
      button.addEventListener("click", () => {
        $("inspect-id").value = order.request_id;
        inspectRequest();
      });
      row.append(button);
      $("live-orders").append(row);
    }
    $("live-stream-preview").textContent = JSON.stringify(
      outbox.preview,
      null,
      2,
    );
    lastSample = data.sampled_at;
    $("live-freshness").textContent =
      `最近成功采样：${formatTime(lastSample)}。来自实际服务，跨存储非原子快照。`;
  }
  async function refreshLive() {
    if (!token || refreshing) return;
    refreshing = true;
    stopTimer();
    $("refresh-live").disabled = true;
    const version = sessionVersion;
    try {
      const [overview, health] = await Promise.allSettled([
        api("/seckill/observe/overview"),
        api("/lab/api/health"),
      ]);
      if (version !== sessionVersion) return;
      const authFailure = [overview, health].find(
        (item) =>
          item.status === "rejected" && [401, 403].includes(item.reason.status),
      );
      if (authFailure) {
        clearSession();
        message("会话失效或权限不足，请重新登录管理员。", true);
        return;
      }
      if (overview.status === "fulfilled") {
        renderOverview(overview.value);
        message(
          health.status === "fulfilled"
            ? "采样完成。"
            : "库存与订单已采样；服务就绪检查失败。",
          health.status !== "fulfilled",
        );
      } else {
        $("live-freshness").textContent = lastSample
          ? `采样失败，下面保留 ${formatTime(lastSample)} 的旧数据，不能代表当前状态。`
          : "采样失败，尚无有效数据。";
        message(
          "观测失败：" +
            (overview.reason.name === "AbortError"
              ? "请求超时或已取消。"
              : overview.reason.message),
          true,
        );
      }
      if (health.status === "fulfilled") renderHealth(health.value.services);
      else {
        $("service-health").replaceChildren(
          node("span", "服务就绪状态未知", "health-chip down"),
        );
      }
    } finally {
      if (version === sessionVersion) {
        refreshing = false;
        $("refresh-live").disabled = false;
        schedule();
      }
    }
  }
  $("admin-login").addEventListener("submit", async (event) => {
    event.preventDefault();
    const button = event.submitter;
    button.disabled = true;
    message("正在验证管理员身份…");
    const version = sessionVersion;
    try {
      const data = await api("/login", {
        method: "POST",
        body: JSON.stringify({
          username: $("admin-name").value,
          password: $("admin-password").value,
        }),
      });
      if (version !== sessionVersion) return;
      if (data.user.role !== "admin")
        throw new Error(
          "真实观测仅允许管理员；普通用户 Token 不会保存在本页。",
        );
      token = data.token;
      sessionVersion++;
      $("admin-password").value = "";
      $("live-user").textContent = "管理员：" + data.user.username;
      $("login-section").hidden = true;
      $("live-session").hidden = false;
      await refreshLive();
    } catch (error) {
      message(
        error.name === "AbortError" ? "登录超时，请重试。" : error.message,
        true,
      );
    } finally {
      button.disabled = false;
    }
  });
  $("logout-live").addEventListener("click", async () => {
    stopTimer();
    let revoked = false;
    try {
      await api("/logout", { method: "POST" });
      revoked = true;
    } catch (error) {
      /* Clear local credentials even when dependency unavailable. */
    } finally {
      clearSession();
      message(
        revoked
          ? "已注销，服务器已撤销 Token。"
          : "已退出本页；服务器注销失败，原 Token 可能仍有效。",
        !revoked,
      );
    }
  });
  $("refresh-live").addEventListener("click", refreshLive);
  $("auto-refresh").addEventListener("change", schedule);
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) stopTimer();
    else schedule();
  });
  window.addEventListener("pagehide", () => {
    clearSession();
  });
  async function inspectRequest() {
    if (!token) return;
    const id = $("inspect-id").value.trim();
    if (
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
        id,
      )
    ) {
      $("inspect-result").replaceChildren(
        node("p", "请输入完整的请求 UUID。", "error"),
      );
      return;
    }
    const version = sessionVersion;
    $("inspect-result").replaceChildren(node("p", "正在核查…", "muted"));
    try {
      const data = await api(
        "/seckill/observe/requests/" + encodeURIComponent(id),
      );
      if (version !== sessionVersion) return;
      const result = $("inspect-result");
      result.replaceChildren(
        node("strong", labels[data.status] || data.status),
        node(
          "p",
          `受理证据：${data.accepted_evidence ? "存在" : "未知"} · SQL 订单：${data.order ? "#" + data.order.id : "尚无"} · MQ 逐请求位置：未跟踪`,
        ),
      );
      result.append(
        node(
          "p",
          data.order
            ? "最终订单已提交，以 SQL 为准。"
            : data.status === "inconsistent"
              ? "Redis 标记 created，但 SQL 没有对应订单。需人工核查，不能宣称订单成功或自动退库存。"
              : "尚无最终订单。queued / manual_review 不是退款或库存释放依据。",
        ),
      );
      const details = node("details");
      details.append(
        node("summary", "查看核查证据"),
        node("pre", JSON.stringify(data, null, 2)),
      );
      result.append(details);
    } catch (error) {
      if (version === sessionVersion)
        $("inspect-result").replaceChildren(
          node(
            "p",
            error.name === "AbortError" ? "核查超时或取消。" : error.message,
            "error",
          ),
        );
    }
  }
  $("inspect-form").addEventListener("submit", (event) => {
    event.preventDefault();
    inspectRequest();
  });
  document.querySelectorAll("[data-tab]").forEach((button) =>
    button.addEventListener("click", () => {
      activeTab = button.dataset.tab;
      document.querySelectorAll("[data-tab]").forEach((tab) => {
        tab.classList.toggle("active", tab === button);
        tab.setAttribute("aria-pressed", String(tab === button));
      });
      for (const key of ["demo", "live", "guide"])
        $("panel-" + key).hidden = key !== activeTab;
      if (activeTab === "live" && token) refreshLive();
      else stopTimer();
    }),
  );
  renderDemo();
})();
