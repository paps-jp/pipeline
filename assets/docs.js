/* docs.js — 全ドキュメントページ共通の外装。
   各ページは <body class="docs" data-page="<key>"> と、本文を
   <article class="docs-article"> に置くだけでよい。サイドバー / 右目次 /
   テーマ切替 / 検索 / prev-next は ここで生成する。 */
(function () {
  "use strict";

  // ---- サイドバー定義 (グループ → ページ) ---------------------------------
  // key は data-page と 突き合わせて active 判定に使う。
  var NAV = [
    { title: "はじめに", items: [
      { key: "introduction", label: "Introduction", href: "manual.html" },
      { key: "setup",        label: "セットアップ",  href: "setup.html" },
    ]},
    { title: "コアコンセプト", items: [
      { key: "concepts", label: "用語とアーキテクチャ", href: "concepts.html" },
    ]},
    { title: "Web UI", items: [
      { key: "web-ui", label: "画面ガイド", href: "web-ui.html" },
    ]},
    { title: "プラグイン開発", items: [
      { key: "plugin-sdk",  label: "プラグインSDK",  href: "plugin-sdk.html" },
      { key: "storage-sdk", label: "Storage SDK",    href: "storage-sdk.html" },
      { key: "executors",   label: "Executor種別",   href: "executors.html" },
    ]},
    { title: "リファレンス", items: [
      { key: "rest-api",      label: "REST API",       href: "rest-api.html" },
      { key: "configuration", label: "設定リファレンス", href: "configuration.html" },
    ]},
    { title: "運用", items: [
      { key: "operations", label: "運用Tips", href: "operations.html" },
    ]},
  ];

  var current = document.body.getAttribute("data-page") || "";
  var flat = [];
  NAV.forEach(function (g) { g.items.forEach(function (it) { flat.push(it); }); });

  // ---- テーマ切替 ----------------------------------------------------------
  var THEME_KEY = "pipeline-docs-theme";
  function applyTheme(t) {
    if (t === "light" || t === "dark") document.documentElement.setAttribute("data-theme", t);
    else document.documentElement.removeAttribute("data-theme");
  }
  applyTheme(localStorage.getItem(THEME_KEY));
  function currentTheme() {
    var t = document.documentElement.getAttribute("data-theme");
    if (t) return t;
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }

  // ---- サイドバー描画 ------------------------------------------------------
  var sidebar = document.getElementById("docs-sidebar");
  if (sidebar) {
    var html = "";
    NAV.forEach(function (g) {
      html += '<div class="sb-group"><p class="sb-group-title">' + g.title + "</p><ul>";
      g.items.forEach(function (it) {
        var active = it.key === current ? ' class="active"' : "";
        html += '<li data-label="' + it.label.toLowerCase() + '"><a href="' + it.href + '"' + active + ">" + it.label + "</a></li>";
      });
      html += "</ul></div>";
    });
    html += '<p class="sb-empty" hidden>該当なし</p>';
    sidebar.innerHTML = html;

    // 絞り込み (ヘッダの検索ボックスと連動)
    var input = document.getElementById("docs-search-input");
    var empty = sidebar.querySelector(".sb-empty");
    if (input) {
      input.addEventListener("input", function () {
        var q = input.value.trim().toLowerCase();
        var anyShown = false;
        sidebar.querySelectorAll(".sb-group").forEach(function (grp) {
          var groupShown = false;
          grp.querySelectorAll("li").forEach(function (li) {
            var hit = !q || (li.getAttribute("data-label") || "").indexOf(q) !== -1;
            li.hidden = !hit;
            if (hit) groupShown = true;
          });
          grp.hidden = !groupShown;
          if (groupShown) anyShown = true;
        });
        empty.hidden = anyShown;
        // モバイルでは検索時にサイドバーを開く
        if (q && window.matchMedia("(max-width: 900px)").matches) sidebar.classList.add("open");
      });
    }
    // "/" でヘッダ検索にフォーカス
    document.addEventListener("keydown", function (e) {
      if (e.key === "/" && input && document.activeElement !== input &&
          !/^(INPUT|TEXTAREA)$/.test((document.activeElement || {}).tagName || "")) {
        e.preventDefault(); input.focus();
      }
    });
  }

  // ---- 右「このページの内容」 + 見出しアンカー -----------------------------
  var toc = document.getElementById("docs-toc");
  var article = document.querySelector(".docs-article");
  var headings = [];
  if (article) {
    var hs = article.querySelectorAll("h2, h3");
    hs.forEach(function (h) {
      if (!h.id) {
        h.id = (h.textContent || "").trim().toLowerCase()
          .replace(/[^\w぀-ヿ㐀-鿿\- ]/g, "")
          .replace(/\s+/g, "-").slice(0, 64) || ("sec-" + headings.length);
      }
      // hover アンカー
      var a = document.createElement("a");
      a.href = "#" + h.id; a.className = "header-anchor"; a.setAttribute("aria-hidden", "true");
      a.textContent = "#";
      h.insertBefore(a, h.firstChild);
      headings.push(h);
    });
  }
  if (toc && headings.length) {
    var t = '<p class="toc-title">このページの内容</p><ul>';
    headings.forEach(function (h) {
      var lvl = h.tagName === "H3" ? " lvl-3" : "";
      var label = (h.textContent || "").replace(/^#/, "").trim();
      t += '<li><a class="toc-link' + lvl + '" href="#' + h.id + '">' + label + "</a></li>";
    });
    t += "</ul>";
    toc.innerHTML = t;

    // scrollspy
    var links = {};
    toc.querySelectorAll("a.toc-link").forEach(function (a) {
      links[a.getAttribute("href").slice(1)] = a;
    });
    var spy = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) {
          Object.keys(links).forEach(function (k) { links[k].classList.remove("active"); });
          if (links[e.target.id]) links[e.target.id].classList.add("active");
        }
      });
    }, { rootMargin: "-70px 0px -70% 0px", threshold: 0 });
    headings.forEach(function (h) { spy.observe(h); });
  } else if (toc) {
    toc.classList.add("empty");
  }

  // ---- prev / next ---------------------------------------------------------
  var pager = document.getElementById("docs-pager");
  if (pager) {
    var idx = flat.findIndex(function (it) { return it.key === current; });
    var prev = idx > 0 ? flat[idx - 1] : null;
    var next = idx >= 0 && idx < flat.length - 1 ? flat[idx + 1] : null;
    var ph = "";
    ph += prev
      ? '<a class="prev" href="' + prev.href + '"><span class="dir">← 前へ</span><span class="ttl">' + prev.label + "</span></a>"
      : '<a class="prev placeholder" href="#"></a>';
    ph += next
      ? '<a class="next" href="' + next.href + '"><span class="dir">次へ →</span><span class="ttl">' + next.label + "</span></a>"
      : '<a class="next placeholder" href="#"></a>';
    pager.innerHTML = ph;
  }

  // ---- ヘッダのイベント (テーマ / モバイルメニュー) ------------------------
  document.addEventListener("click", function (e) {
    var tt = e.target.closest && e.target.closest(".theme-toggle");
    if (tt) { applyTheme(currentTheme() === "dark" ? "light" : "dark"); localStorage.setItem(THEME_KEY, currentTheme()); return; }
    var hb = e.target.closest && e.target.closest(".hamburger");
    if (hb && sidebar) {
      sidebar.classList.toggle("open");
      var bd = document.querySelector(".sb-backdrop");
      if (bd) bd.classList.toggle("show", sidebar.classList.contains("open"));
      return;
    }
    var bd2 = e.target.closest && e.target.closest(".sb-backdrop");
    if (bd2 && sidebar) { sidebar.classList.remove("open"); bd2.classList.remove("show"); }
  });
})();
