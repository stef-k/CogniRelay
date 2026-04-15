(function () {
  function setText(root, selector, value) {
    var node = root.querySelector(selector);
    if (node) {
      node.textContent = value;
    }
  }

  function formatRecentChange(change) {
    if (!change) {
      return "No recent continuity change in the current view.";
    }
    return (
      change.subject_kind +
      "/" +
      change.subject_id +
      " [" +
      change.artifact_state +
      "] at " +
      change.recorded_at
    );
  }

  function connectLiveRegion(root) {
    if (!("EventSource" in window)) {
      setText(root, "[data-live-connection]", "Live updates unavailable in this browser.");
      return;
    }

    var streamUrl = "/ui/events";
    if (root.getAttribute("data-live-page") === "continuity" && window.location.search) {
      streamUrl += window.location.search;
    }

    var source = new EventSource(streamUrl);

    source.onopen = function () {
      setText(root, "[data-live-connection]", "Live updates connected.");
    };

    source.onerror = function () {
      setText(root, "[data-live-connection]", "Live updates disconnected; browser will retry.");
    };

    source.addEventListener("ui-snapshot", function (event) {
      var payload;
      try {
        payload = JSON.parse(event.data);
      } catch (_err) {
        setText(root, "[data-live-connection]", "Live updates unavailable due to malformed snapshot.");
        return;
      }

      setText(root, "[data-live-generated-at]", payload.generated_at || "unavailable");
      if (payload.ok === false) {
        setText(root, "[data-live-connection]", "Live updates degraded; retrying with bounded snapshots.");
      } else {
        setText(root, "[data-live-connection]", "Live updates connected.");
      }

      if (root.getAttribute("data-live-page") === "overview" && payload.overview) {
        setText(root, "[data-live-latest-commit]", payload.overview.latest_commit || "unavailable");
        setText(root, "[data-live-reported-at]", payload.overview.reported_at || "unavailable");
        if (payload.overview.continuity_counts) {
          setText(root, "[data-live-active-count]", String(payload.overview.continuity_counts.active || 0));
          setText(root, "[data-live-fallback-count]", String(payload.overview.continuity_counts.fallback || 0));
          setText(root, "[data-live-archived-count]", String(payload.overview.continuity_counts.archived || 0));
          setText(root, "[data-live-cold-count]", String(payload.overview.continuity_counts.cold || 0));
        }
      }

      if (root.getAttribute("data-live-page") === "continuity" && payload.continuity) {
        if (payload.continuity.artifact_counts) {
          setText(root, "[data-live-active-count]", String(payload.continuity.artifact_counts.active || 0));
          setText(root, "[data-live-fallback-count]", String(payload.continuity.artifact_counts.fallback || 0));
          setText(root, "[data-live-archived-count]", String(payload.continuity.artifact_counts.archived || 0));
          setText(root, "[data-live-cold-count]", String(payload.continuity.artifact_counts.cold || 0));
        }
        setText(root, "[data-live-latest-recorded-at]", payload.continuity.latest_recorded_at || "unavailable");
        setText(root, "[data-live-recent-change]", formatRecentChange(payload.continuity.recent_change || null));
      }
    });
  }

  window.addEventListener("DOMContentLoaded", function () {
    var roots = document.querySelectorAll("[data-live-page]");
    for (var idx = 0; idx < roots.length; idx += 1) {
      connectLiveRegion(roots[idx]);
    }
  });
})();
