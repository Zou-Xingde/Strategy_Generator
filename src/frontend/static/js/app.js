/*
 * Responsive Toolbar — Refactored v2 (2025-08-11)
 * Fix: dropdowns not opening in some templates.
 * - Bind directly to the buttons (#symbol-dropdown, #algorithm-dropdown)
 * - Also support delegated clicks on document
 * - Toggle aria-expanded on BOTH the container and the button (defensive)
 * - Close on outside click & Escape
 * - Prevent immediate-close race by ordering and stopPropagation
 */
(function () {
  if (window.responsiveToolbarInitialized) return;
  window.responsiveToolbarInitialized = true;

  const qs = (sel, root = document) => root.querySelector(sel);
  const qsa = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const on = (el, evt, fn, opts) => el && el.addEventListener(evt, fn, opts);

  const dispatch = (name, detail) => document.dispatchEvent(new CustomEvent(name, { detail }));

  const setExpanded = (container, button, expanded) => {
    if (container) container.setAttribute('aria-expanded', expanded ? 'true' : 'false');
    if (button) button.setAttribute('aria-expanded', expanded ? 'true' : 'false');
  };

  const isWithinAny = (el, sel) => !!(el && el.closest(sel));

  const closeAllDropdowns = () => {
    qsa('.symbol-selector, .algorithm-selector').forEach((s) => setExpanded(s, s.querySelector('button'), false));
  };

  class ResponsiveToolbar {
    constructor() {
      this.root = qs('header.main-toolbar');
      if (!this.root) {
        console.warn('ResponsiveToolbar: .main-toolbar not found.');
        return;
      }

      this.hooks = {
        onTimeframeChange: null,
        onSymbolChange: null,
        onAlgorithmChange: null,
        onGenerate: null,
        onClear: null,
        onShowToggle: null,
        onList: null,
        onZoomIn: null,
        onZoomOut: null,
        onZoomReset: null,
        onCrosshairToggle: null,
        onMeasureToggle: null,
        onFullscreen: null,
        onParamsOpen: null,
        onParamsConfirm: null,
        onParamsCancel: null,
      };

      this._bindDropdowns();
      this._bindTimeframes();
      this._bindButtons();
      this._bindParamsPopup();
      this._tickClock();
      this._attachDefaultBridgesToSwingChart();

      window.responsiveToolbar = this;
      console.log('✅ ResponsiveToolbar ready');
    }

    _bindDropdowns() {
      const symbolSelector = qs('.symbol-selector');
      const algoSelector = qs('.algorithm-selector');
      const symbolBtn = qs('#symbol-dropdown');
      const algoBtn = qs('#algorithm-dropdown');

      const toggle = (container, button) => {
        if (!container || !button) return;
        const open = container.getAttribute('aria-expanded') === 'true';
        closeAllDropdowns();
        setExpanded(container, button, !open);
      };

      // Direct bindings (most reliable)
      on(symbolBtn, 'click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        toggle(symbolSelector, symbolBtn);
      });
      on(algoBtn, 'click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        toggle(algoSelector, algoBtn);
      });

      // Keyboard support on buttons
      const keyActivate = (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          e.stopPropagation();
          const isSymbol = e.currentTarget === symbolBtn;
          toggle(isSymbol ? symbolSelector : algoSelector, e.currentTarget);
        }
      };
      on(symbolBtn, 'keydown', keyActivate);
      on(algoBtn, 'keydown', keyActivate);

      // Delegate clicks (fallback if DOM is re-rendered)
      on(document, 'click', (e) => {
        const btn = e.target.closest('#symbol-dropdown, #algorithm-dropdown');
        if (!btn) return;
        e.preventDefault();
        e.stopPropagation();
        const isSymbol = btn.id === 'symbol-dropdown';
        toggle(isSymbol ? qs('.symbol-selector') : qs('.algorithm-selector'), btn);
      }, true); // capture to run before global close

      // Do not close when clicking inside the open dropdowns
      on(document, 'click', (e) => {
        if (isWithinAny(e.target, '.symbol-selector, .algorithm-selector')) return;
        closeAllDropdowns();
      });

      on(document, 'keydown', (e) => {
        if (e.key === 'Escape') closeAllDropdowns();
      });

      // Select options
      on(document, 'click', (e) => {
        const opt = e.target.closest('.symbol-option');
        if (!opt) return;
        const value = opt.dataset.symbol;
        qsa('.symbol-option').forEach((o) => o.setAttribute('aria-selected', o === opt ? 'true' : 'false'));
        const label = qs('#current-symbol');
        if (label && value) label.textContent = value;
        closeAllDropdowns();
        dispatch('toolbar:symbol', { symbol: value });
        this.hooks.onSymbolChange?.(value);
      });

      on(document, 'click', (e) => {
        const opt = e.target.closest('.algorithm-option');
        if (!opt) return;
        const value = opt.dataset.algorithm;
        qsa('.algorithm-option').forEach((o) => o.setAttribute('aria-selected', o === opt ? 'true' : 'false'));
        const label = qs('#current-algorithm');
        if (label && value) label.textContent = value;
        closeAllDropdowns();
        dispatch('toolbar:algorithm', { algorithm: value });
        this.hooks.onAlgorithmChange?.(value);
      });
    }

    _bindTimeframes() {
      const container = qs('.timeframe-buttons');
      if (!container) return;
      on(container, 'click', (e) => {
        const btn = e.target.closest('.tf-btn');
        if (!btn) return;
        const timeframe = btn.dataset.timeframe;
        qsa('.tf-btn').forEach((b) => {
          b.classList.toggle('active', b === btn);
          b.setAttribute('aria-selected', b === btn ? 'true' : 'false');
        });
        dispatch('toolbar:timeframe', { timeframe });
        this.hooks.onTimeframeChange?.(timeframe);
      });
    }

    _bindButtons() {
      const id = (x) => qs(`#${x}`);
      const click = (el, fn) => on(el, 'click', fn);
      click(id('generate-swing'), () => { dispatch('toolbar:generate'); this.hooks.onGenerate?.(); });
      click(id('clear-swing'), () => { dispatch('toolbar:clear'); this.hooks.onClear?.(); });
      click(id('show-swing'), () => { dispatch('toolbar:showToggle'); this.hooks.onShowToggle?.(); });
      click(id('swing-list'), () => { dispatch('toolbar:list'); this.hooks.onList?.(); });
      click(id('zoom-in'), () => { dispatch('toolbar:zoomIn'); this.hooks.onZoomIn?.(); });
      click(id('zoom-out'), () => { dispatch('toolbar:zoomOut'); this.hooks.onZoomOut?.(); });
      click(id('zoom-reset'), () => { dispatch('toolbar:zoomReset'); this.hooks.onZoomReset?.(); });
      click(id('crosshair'), () => { const a = id('crosshair')?.classList.toggle('active'); dispatch('toolbar:crosshair', { active: a }); this.hooks.onCrosshairToggle?.(a); });
      click(id('measure'), () => { const a = id('measure')?.classList.toggle('active'); dispatch('toolbar:measure', { active: a }); this.hooks.onMeasureToggle?.(a); });
      click(id('fullscreen'), () => { dispatch('toolbar:fullscreen'); this.hooks.onFullscreen?.(); });

      const paramsBtn = qs('#swing-params');
      on(paramsBtn, 'click', () => { this._openParams(); dispatch('toolbar:paramsOpen'); this.hooks.onParamsOpen?.(); });
    }

    _bindParamsPopup() {
      const dlg = qs('#params-popup');
      if (!dlg) return;
      const close = () => this._closeParams();

      // Close button (direct)
      on(qs('#close-params'), 'click', (e) => { e.preventDefault(); e.stopPropagation(); close(); });
      // Close button (delegated fallback)
      on(document, 'click', (e) => {
        if (e.target.closest('#close-params') || e.target.closest('.close-btn')) {
          e.preventDefault();
          close();
        }
      });

      // Click outside dialog closes
      on(dlg, 'click', (e) => {
        if (e.target === dlg) close();
      });

      // Cancel / Confirm
      on(qs('#cancel-params'), 'click', (e) => {
        e.preventDefault();
        close();
        dispatch('toolbar:paramsCancel');
        this.hooks.onParamsCancel?.();
      });
      on(qs('#confirm-params'), 'click', (e) => {
        e.preventDefault();
        const params = this._collectParams();
        if (window.swingChart) {
          window.swingChart.algoParamsState = { ...(window.swingChart.algoParamsState || {}), ...params };
          if (typeof window.swingChart.showNotification === 'function') window.swingChart.showNotification('參數已更新', 'success');
        }
        dispatch('toolbar:paramsConfirm', { params });
        this.hooks.onParamsConfirm?.(params);
        close();
      });

      // Escape to close
      on(document, 'keydown', (e) => { if (e.key === 'Escape') close(); });
    }

    _openParams() {
      const state = (window.swingChart && window.swingChart.algoParamsState) || {};
      const map = { deviation: qs('#deviation-param'), depth: qs('#depth-param'), backstep: qs('#backstep-param') };
      Object.entries(map).forEach(([k, input]) => { if (input && state[k] != null) input.value = state[k]; });
      const dlg = qs('#params-popup');
      if (dlg) {
        dlg.setAttribute('aria-hidden', 'false');
        // ALSO set inline style for compatibility with old code using style.display
        dlg.style.display = 'flex';
        // prevent background scroll/layout shift while dialog open
        document.documentElement.style.overflow = 'hidden';
        document.body.style.overscrollBehavior = 'contain';
      }
    }

    _closeParams() {
      const dlg = qs('#params-popup');
      if (dlg) {
        dlg.setAttribute('aria-hidden', 'true');
        // Ensure hidden even if someone set inline style to block
        dlg.style.display = 'none';
      }
      // restore scroll
      document.documentElement.style.overflow = '';
      document.body.style.overscrollBehavior = '';
    }

    _tickClock() {
      const el = qs('#current-time');
      if (!el) return;
      const pad = (n) => String(n).padStart(2, '0');
      const tick = () => {
        const now = new Date();
        const s = `${now.getFullYear()}/${pad(now.getMonth() + 1)}/${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
        el.textContent = s;
      };
      tick();
      this._clock = setInterval(tick, 1000);
    }

    _attachDefaultBridgesToSwingChart() {
      // 測量彈窗關閉事件處理
      const setupMeasurementPopupEvents = () => {
        const closeBtn = qs('#close-measurement');
        const confirmBtn = qs('#confirm-measurement');
        const popup = qs('#measurement-popup');
        
        const closeMeasurementPopup = () => {
          if (popup) {
            popup.setAttribute('aria-hidden', 'true');
            popup.style.display = 'none';
          }
        };
        
        if (closeBtn) {
          on(closeBtn, 'click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            closeMeasurementPopup();
          });
        }
        
        if (confirmBtn) {
          on(confirmBtn, 'click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            closeMeasurementPopup();
          });
        }
        
        // 點擊彈窗外部關閉
        if (popup) {
          on(popup, 'click', (e) => {
            if (e.target === popup) {
              closeMeasurementPopup();
            }
          });
        }
        
        // ESC鍵關閉
        on(document, 'keydown', (e) => {
          if (e.key === 'Escape' && popup && popup.getAttribute('aria-hidden') === 'false') {
            closeMeasurementPopup();
          }
        });
      };
      
      // 初始化測量彈窗事件
      setupMeasurementPopupEvents();
      
      // 監聽測量彈窗的顯示，確保aria-hidden同步
      const setupMeasurementPopupObserver = () => {
        const popup = qs('#measurement-popup');
        if (!popup) return;
        
        // 使用MutationObserver監聽style.display的變化
        const observer = new MutationObserver((mutations) => {
          mutations.forEach((mutation) => {
            if (mutation.type === 'attributes' && mutation.attributeName === 'style') {
              const display = popup.style.display;
              if (display === 'block' || display === 'flex') {
                // 當swing.js設置display=block時，同步設置aria-hidden=false
                popup.setAttribute('aria-hidden', 'false');
              } else if (display === 'none') {
                // 當設置display=none時，同步設置aria-hidden=true
                popup.setAttribute('aria-hidden', 'true');
              }
            }
          });
        });
        
        observer.observe(popup, { 
          attributes: true, 
          attributeFilter: ['style'] 
        });
      };
      
      // 初始化測量彈窗監聽器
      setupMeasurementPopupObserver();

      if (!window.swingChart) return;
      document.addEventListener('toolbar:timeframe', (e) => { const tf = e.detail?.timeframe; if (tf && typeof window.swingChart.loadChart === 'function') { clearTimeout(this._tfDebounce); this._tfDebounce = setTimeout(() => window.swingChart.loadChart(tf), 0); } });
      document.addEventListener('toolbar:symbol', (e) => { const v = e.detail?.symbol; if (v && typeof window.swingChart.switchSymbol === 'function') window.swingChart.switchSymbol(v); });
      document.addEventListener('toolbar:algorithm', (e) => { const v = e.detail?.algorithm; if (v && typeof window.swingChart.switchAlgorithm === 'function') window.swingChart.switchAlgorithm(v); });
      document.addEventListener('toolbar:generate', () => { if (typeof window.swingChart.startSwingGeneration === 'function') window.swingChart.startSwingGeneration(); });
      document.addEventListener('toolbar:clear', () => { if (typeof window.swingChart.clearSwings === 'function') window.swingChart.clearSwings(); if (typeof window.swingChart.clearSwingStatus === 'function') window.swingChart.clearSwingStatus(); if (typeof window.swingChart.stopSwingGeneration === 'function') window.swingChart.stopSwingGeneration(); });
      document.addEventListener('toolbar:showToggle', () => { if (typeof window.swingChart.toggleSwingVisibility === 'function') window.swingChart.toggleSwingVisibility(); });
      document.addEventListener('toolbar:list', () => { if (typeof window.swingChart.showSwingList === 'function') window.swingChart.showSwingList(); });
      document.addEventListener('toolbar:zoomIn', () => { if (typeof window.swingChart.performZoom === 'function') window.swingChart.performZoom(0.8); });
      document.addEventListener('toolbar:zoomOut', () => { if (typeof window.swingChart.performZoom === 'function') window.swingChart.performZoom(1.25); });
      document.addEventListener('toolbar:zoomReset', () => { if (typeof window.swingChart.resetChart === 'function') window.swingChart.resetChart(); });
      document.addEventListener('toolbar:crosshair', () => { if (typeof window.swingChart.toggleCrosshair === 'function') window.swingChart.toggleCrosshair(qs('#crosshair')); });
      document.addEventListener('toolbar:measure', (e) => {if (typeof window.swingChart.toggleMeasurementMode === 'function') window.swingChart.toggleMeasurementMode(e.detail?.active); });
      document.addEventListener('toolbar:fullscreen', () => { if (typeof window.swingChart.toggleFullscreen === 'function') window.swingChart.toggleFullscreen(); });
    }
  }

  document.addEventListener('DOMContentLoaded', () => new ResponsiveToolbar());
})();
