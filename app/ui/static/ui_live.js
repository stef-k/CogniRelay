(function () {
  var LIVE_BASE_DELAY_MS = 1000;
  var LIVE_MAX_DELAY_MS = 16000;
  var LIVE_OFFLINE_THRESHOLD = 4;
  var UI_THEME_KEY = "cognirelay-ui-theme";

  function setText(root, selector, value) {
    var node = root.querySelector(selector);
    if (node && node.textContent !== value) {
      node.textContent = value;
    }
  }

  function setHTML(root, selector, value) {
    var node = root.querySelector(selector);
    if (node && typeof value === "string" && node.innerHTML !== value) {
      node.innerHTML = value;
    }
  }

  function setState(root, state, message) {
    if (root.getAttribute("data-live-state") !== state) {
      root.setAttribute("data-live-state", state);
    }
    setText(root, "[data-live-connection]", message);
  }

  function formatRecentChange(change) {
    if (!change) {
      return "No recent continuity change in the current view.";
    }
    return change.subject_kind + "/" + change.subject_id + " [" + change.artifact_state + "] at " + change.recorded_at;
  }

  function formatDelay(delayMs) {
    return (delayMs / 1000).toFixed(delayMs % 1000 === 0 ? 0 : 1) + "s";
  }

  function backoffDelay(attempt) {
    var exponent = attempt - 1;
    if (exponent < 0) {
      exponent = 0;
    }
    return Math.min(LIVE_MAX_DELAY_MS, LIVE_BASE_DELAY_MS * Math.pow(2, exponent));
  }

  function reconnectState(attempt) {
    return attempt >= LIVE_OFFLINE_THRESHOLD ? "offline" : "reconnecting";
  }

  function applyOverview(root, payload) {
    if (!payload.overview) {
      return;
    }
    setText(root, "[data-live-service-version]", payload.overview.version || "unavailable");
    setText(root, "[data-live-git-initialized]", String(payload.overview.git_initialized || false));
    setText(root, "[data-live-latest-commit]", payload.overview.latest_commit || "unavailable");
    setText(root, "[data-live-reported-at]", payload.overview.reported_at || "unavailable");
    if (payload.overview.continuity_counts) {
      setText(root, "[data-live-active-count]", String(payload.overview.continuity_counts.active || 0));
      setText(root, "[data-live-fallback-count]", String(payload.overview.continuity_counts.fallback || 0));
      setText(root, "[data-live-archived-count]", String(payload.overview.continuity_counts.archived || 0));
      setText(root, "[data-live-cold-count]", String(payload.overview.continuity_counts.cold || 0));
      var kinds = payload.overview.continuity_counts.by_subject_kind || {};
      setText(root, "[data-live-user-count]", String(kinds.user || 0));
      setText(root, "[data-live-peer-count]", String(kinds.peer || 0));
      setText(root, "[data-live-thread-count]", String(kinds.thread || 0));
      setText(root, "[data-live-task-count]", String(kinds.task || 0));
    }
  }

  function applyContinuity(root, payload) {
    if (!payload.continuity) {
      return;
    }
    if (payload.continuity.artifact_counts) {
      setText(root, "[data-live-active-count]", String(payload.continuity.artifact_counts.active || 0));
      setText(root, "[data-live-fallback-count]", String(payload.continuity.artifact_counts.fallback || 0));
      setText(root, "[data-live-archived-count]", String(payload.continuity.artifact_counts.archived || 0));
      setText(root, "[data-live-cold-count]", String(payload.continuity.artifact_counts.cold || 0));
    }
    setText(root, "[data-live-displayed-count]", String(payload.continuity.displayed_count || 0));
    setText(root, "[data-live-matched-count]", String(payload.continuity.matched_count || 0));
    setText(root, "[data-live-result-truncated]", String(payload.continuity.result_truncated || false));
    setText(root, "[data-live-latest-recorded-at]", payload.continuity.latest_recorded_at || "unavailable");
    setText(root, "[data-live-recent-change]", formatRecentChange(payload.continuity.recent_change || null));
    setHTML(root, "[data-live-continuity-table]", payload.continuity.table_html || "");
  }

  function applyDetail(root, payload) {
    if (!payload.detail) {
      return;
    }
    if (payload.detail.artifact_counts) {
      setText(root, "[data-live-active-count]", String(payload.detail.artifact_counts.active || 0));
      setText(root, "[data-live-fallback-count]", String(payload.detail.artifact_counts.fallback || 0));
      setText(root, "[data-live-archived-count]", String(payload.detail.artifact_counts.archived || 0));
      setText(root, "[data-live-cold-count]", String(payload.detail.artifact_counts.cold || 0));
    }
    setText(root, "[data-live-detail-source-state]", payload.detail.source_state || "unavailable");
    setText(root, "[data-live-detail-updated-at]", payload.detail.updated_at || "unavailable");
    setText(root, "[data-live-detail-verified-at]", payload.detail.verified_at || "unavailable");
    setText(root, "[data-live-detail-warning-count]", String(payload.detail.recovery_warning_count || 0));
    setText(root, "[data-live-latest-recorded-at]", payload.detail.latest_recorded_at || "unavailable");
    if (payload.detail.sections) {
      setHTML(root, "[data-live-detail-related-artifacts]", payload.detail.sections.related_artifact_rows || "");
      setHTML(root, "[data-live-detail-recovery-warnings]", payload.detail.sections.recovery_warnings_html || "");
      setHTML(root, "[data-live-detail-startup-summary]", payload.detail.sections.startup_summary_html || "");
      setHTML(root, "[data-live-detail-trust-signals]", payload.detail.sections.trust_signals_html || "");
      setHTML(root, "[data-live-detail-top-priorities]", payload.detail.sections.top_priorities_html || "");
      setHTML(root, "[data-live-detail-active-concerns]", payload.detail.sections.active_concerns_html || "");
      setHTML(root, "[data-live-detail-active-constraints]", payload.detail.sections.active_constraints_html || "");
      setHTML(root, "[data-live-detail-open-loops]", payload.detail.sections.open_loops_html || "");
      setHTML(root, "[data-live-detail-session-trajectory]", payload.detail.sections.session_trajectory_html || "");
      setHTML(root, "[data-live-detail-stance-summary]", payload.detail.sections.stance_summary_html || "");
      setHTML(root, "[data-live-detail-related-documents]", payload.detail.sections.related_documents_html || "");
      setHTML(root, "[data-live-detail-thread-descriptor]", payload.detail.sections.thread_descriptor_section || "");
      setHTML(root, "[data-live-detail-stable-preferences]", payload.detail.sections.stable_preferences_html || "");
      setHTML(root, "[data-live-detail-negative-decisions]", payload.detail.sections.negative_decisions_html || "");
      setHTML(root, "[data-live-detail-rationale-entries]", payload.detail.sections.rationale_entries_html || "");
    }
  }

  function applyGraph(root, payload) {
    if (!payload.graph || !payload.graph.sections) {
      return;
    }
    setHTML(root.ownerDocument, "[data-live-graph-anchor]", payload.graph.sections.anchor_html || "");
    setHTML(root.ownerDocument, "[data-live-graph-source-status]", payload.graph.sections.source_status_html || "");
    setHTML(root.ownerDocument, "[data-live-graph-warnings]", payload.graph.sections.warnings_html || "");
    setHTML(root.ownerDocument, "[data-live-graph-nodes]", payload.graph.sections.nodes_html || "");
    setHTML(root.ownerDocument, "[data-live-graph-edges]", payload.graph.sections.edges_html || "");
  }

  function applySnapshot(root, payload) {
    setText(root, "[data-live-generated-at]", payload.generated_at || "unavailable");
    var page = root.getAttribute("data-live-page");
    if (page === "overview") {
      applyOverview(root, payload);
    } else if (page === "continuity") {
      applyContinuity(root, payload);
    } else if (page === "detail") {
      applyDetail(root, payload);
    } else if (page === "graph") {
      applyGraph(root, payload);
    }
  }

  function connectLiveRegion(root) {
    if (!("EventSource" in window)) {
      setState(root, "offline", "Live updates unavailable in this browser.");
      return;
    }

    var streamUrl = root.getAttribute("data-live-stream") || "/ui/events";
    var source = null;
    var reconnectTimer = null;
    var reconnectAttempt = 0;

    function clearReconnectTimer() {
      if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
    }

    function disconnectCurrentSource() {
      if (source !== null) {
        source.close();
        source = null;
      }
    }

    function scheduleReconnect() {
      if (reconnectTimer !== null) {
        return;
      }
      reconnectAttempt += 1;
      var delayMs = backoffDelay(reconnectAttempt);
      var state = reconnectState(reconnectAttempt);
      setState(root, state, "Live updates reconnecting in " + formatDelay(delayMs) + ".");
      reconnectTimer = window.setTimeout(function () {
        reconnectTimer = null;
        connect();
      }, delayMs);
    }

    function connect() {
      clearReconnectTimer();
      disconnectCurrentSource();
      source = new EventSource(streamUrl);
      setState(root, "reconnecting", "Live updates connecting.");

      source.onopen = function () {
        reconnectAttempt = 0;
        setState(root, "connected", "Live updates connected.");
      };

      source.onerror = function () {
        disconnectCurrentSource();
        scheduleReconnect();
      };

      source.addEventListener("ui-snapshot", function (event) {
        var payload;
        try {
          payload = JSON.parse(event.data);
        } catch (_err) {
          setState(root, "degraded", "Live updates degraded; malformed snapshot ignored.");
          disconnectCurrentSource();
          scheduleReconnect();
          return;
        }

        applySnapshot(root, payload);
        if (payload.ok === false) {
          setState(root, "degraded", "Live updates connected with degraded snapshot data.");
        } else {
          setState(root, "connected", "Live updates connected.");
        }
      });
    }

    connect();
  }

  function applyTheme(theme) {
    var value = theme === "light" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", value);
    try {
      window.localStorage.setItem(UI_THEME_KEY, value);
    } catch (_err) {}
    return value;
  }

  function initThemeSelect() {
    var selector = document.querySelector("[data-theme-select]");
    if (!selector) {
      return;
    }
    var currentTheme = document.documentElement.getAttribute("data-theme") || "dark";
    selector.value = currentTheme === "light" ? "light" : "dark";
    selector.addEventListener("change", function (event) {
      applyTheme(event.target.value);
    });
  }

  function initBackToTop() {
    var button = document.querySelector("[data-back-to-top]");
    if (!button) {
      return;
    }

    function updateVisibility() {
      if (window.scrollY > 360) {
        button.classList.add("is-visible");
      } else {
        button.classList.remove("is-visible");
      }
    }

    updateVisibility();
    window.addEventListener("scroll", updateVisibility, { passive: true });
  }

  window.addEventListener("DOMContentLoaded", function () {
    initThemeSelect();
    initBackToTop();
    var roots = document.querySelectorAll("[data-live-page]");
    for (var idx = 0; idx < roots.length; idx += 1) {
      connectLiveRegion(roots[idx]);
    }
  });
})();
