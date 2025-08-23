/* ==============================================
   響應式波段生成器 - 通用互動與委派處理
   ============================================== */

class ResponsiveToolbar {
    constructor() {
        console.log('響應式工具欄初始化開始...');
        this.init();
    }

    init() {
        // 等待DOM載入完成
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => {
                this.setupAllComponents();
            });
        } else {
            this.setupAllComponents();
        }
    }

    setupAllComponents() {
        this.setupEventDelegation();
        this.setupDropdownHandling();
        this.setupTimeframeHandling();
        this.setupParamsPopup();
        this.setupKeyboardNavigation();
        console.log('響應式工具欄初始化完成');
    }

    /* ==============================================
       事件委派處理
       ============================================== */
    
    setupEventDelegation() {
        console.log('設置事件委派...');
        // 使用事件委派避免與現有事件衝突
        document.addEventListener('click', (e) => {
            // 處理更多工具選單的關閉
            this.handleMoreToolsToggle(e);
            
            // 處理下拉選單的關閉
            this.handleDropdownToggle(e);
        });

        // 鍵盤事件委派
        document.addEventListener('keydown', (e) => {
            this.handleKeyboardNavigation(e);
        });
    }

    /* ==============================================
       Details/Summary 更多工具選單處理
       ============================================== */
    
    setupDetailsHandling() {
        console.log('設置Details選單處理...');
        const moreTools = document.getElementById('more-tools');
        if (!moreTools) {
            console.warn('找不到 #more-tools 元素');
            return;
        }

        // 防止選單內容點擊時關閉
        const summary = moreTools.querySelector('summary');
        if (summary) {
            summary.addEventListener('click', (e) => {
                e.stopPropagation();
            });
        }

        // 監聽開關事件
        moreTools.addEventListener('toggle', (e) => {
            if (moreTools.open) {
                this.closeAllDropdowns();
            }
        });
    }

    handleMoreToolsToggle(e) {
        console.log('處理更多工具選單切換...');
        const moreTools = document.getElementById('more-tools');
        if (!moreTools) return;

        // 如果點擊的是選單內容，不關閉
        if (e.target.closest('#more-tools')) {
            return;
        }

        // 點擊外部時關閉選單
        if (moreTools.open) {
            moreTools.open = false;
        }
    }

    /* ==============================================
       時間框架按鈕處理
       ============================================== */
    
    setupTimeframeHandling() {
        console.log('設置時間框架處理...');
        // 時間框架按鈕點擊處理
        document.addEventListener('click', (e) => {
            if (e.target.matches('.tf-btn')) {
                this.handleTimeframeClick(e);
            }
        });
    }

    handleTimeframeClick(e) {
        console.log('處理時間框架點擊...');
        const clickedBtn = e.target;
        const timeframe = clickedBtn.dataset.timeframe;
        
        if (!timeframe) return;

        // 移除所有按鈕的活動狀態
        document.querySelectorAll('.tf-btn').forEach(btn => {
            btn.classList.remove('active');
            btn.setAttribute('aria-selected', 'false');
        });

        // 設置點擊按鈕為活動狀態
        clickedBtn.classList.add('active');
        clickedBtn.setAttribute('aria-selected', 'true');
        
        console.log(`切換到時間框架: ${timeframe}`);
        
        // 觸發時間框架變更事件
        this.onTimeframeChange(timeframe);
    }

    onTimeframeChange(timeframe) {
        // 這個方法會被 SwingChart 覆蓋
        console.log(`時間框架變更: ${timeframe}`);
    }

    /* ==============================================
       下拉選單處理
       ============================================== */
    
    setupDropdownHandling() {
        console.log('設置下拉選單處理...');
        // 下拉選單按鈕點擊處理
        document.addEventListener('click', (e) => {
            if (e.target.matches('#symbol-dropdown, #algorithm-dropdown')) {
                console.log('下拉選單按鈕被點擊:', e.target.id);
                this.toggleDropdown(e.target);
                e.stopPropagation();
            }
        });
        
        // 下拉選單選項點擊處理
        document.addEventListener('click', (e) => {
            if (e.target.matches('.symbol-option')) {
                console.log('品種選項被點擊:', e.target.dataset.symbol);
                this.handleSymbolSelect(e.target);
                e.stopPropagation();
            } else if (e.target.matches('.algorithm-option')) {
                console.log('演算法選項被點擊:', e.target.dataset.algorithm);
                this.handleAlgorithmSelect(e.target);
                e.stopPropagation();
            }
        });
    }

    toggleDropdown(button) {
        console.log('切換下拉選單:', button.id);
        const buttonId = button.id;
        const selector = buttonId === 'symbol-dropdown' ? '.symbol-selector' : '.algorithm-selector';
        const dropdown = document.querySelector(selector);
        
        if (!dropdown) {
            console.warn('找不到下拉選單容器:', selector);
            return;
        }

        const isExpanded = dropdown.getAttribute('aria-expanded') === 'true';
        
        // 關閉所有其他下拉選單
        this.closeAllDropdowns();
        
        if (!isExpanded) {
            // 打開當前下拉選單
            dropdown.setAttribute('aria-expanded', 'true');
            button.setAttribute('aria-expanded', 'true');
            console.log('下拉選單已打開');
        }
    }

    handleDropdownToggle(e) {
        // 如果點擊的是下拉選單內容，不處理
        if (e.target.closest('.symbol-dropdown, .algorithm-dropdown')) {
            return;
        }

        // 點擊其他地方時關閉所有下拉選單
        this.closeAllDropdowns();
    }

    closeAllDropdowns() {
        console.log('關閉所有下拉選單');
        document.querySelectorAll('.symbol-selector, .algorithm-selector').forEach(selector => {
            selector.setAttribute('aria-expanded', 'false');
        });
        
        document.querySelectorAll('#symbol-dropdown, #algorithm-dropdown').forEach(button => {
            button.setAttribute('aria-expanded', 'false');
        });
    }

    handleSymbolSelect(option) {
        const symbol = option.dataset.symbol;
        const symbolText = option.textContent;
        
        console.log('選擇品種:', symbol);
        
        // 更新顯示的品種
        const currentSymbol = document.getElementById('current-symbol');
        if (currentSymbol) {
            currentSymbol.textContent = symbol;
        }
        
        // 關閉下拉選單
        this.closeAllDropdowns();
        
        // 觸發品種變更事件
        this.onSymbolChange(symbol);
    }

    handleAlgorithmSelect(option) {
        const algorithm = option.dataset.algorithm;
        const algorithmText = option.textContent;
        
        console.log('選擇演算法:', algorithm);
        
        // 更新顯示的演算法
        const currentAlgorithm = document.getElementById('current-algorithm');
        if (currentAlgorithm) {
            currentAlgorithm.textContent = algorithm;
        }
        
        // 關閉下拉選單
        this.closeAllDropdowns();
        
        // 觸發演算法變更事件
        this.onAlgorithmChange(algorithm);
    }

    onSymbolChange(symbol) {
        // 這個方法會被 SwingChart 覆蓋
        console.log(`品種變更: ${symbol}`);
    }

    onAlgorithmChange(algorithm) {
        // 這個方法會被 SwingChart 覆蓋
        console.log(`演算法變更: ${algorithm}`);
    }

    /* ==============================================
       參數彈窗處理
       ============================================== */
    
    setupParamsPopup() {
        console.log('設置參數彈窗處理...');
        
        // 參數按鈕點擊
        document.addEventListener('click', (e) => {
            if (e.target.matches('#swing-params, .params-btn')) {
                console.log('參數按鈕被點擊');
                this.showParamsPopup();
                e.stopPropagation();
            }
        });

        // 彈窗關閉按鈕
        document.addEventListener('click', (e) => {
            if (e.target.matches('.close-params, #cancel-params')) {
                console.log('關閉參數彈窗');
                this.hideParamsPopup();
                e.stopPropagation();
            }
        });

        // 確認按鈕
        document.addEventListener('click', (e) => {
            if (e.target.matches('#confirm-params')) {
                console.log('確認參數設置');
                this.handleParamsConfirm();
                e.stopPropagation();
            }
        });

        // 點擊彈窗外部關閉
        document.addEventListener('click', (e) => {
            const modal = document.querySelector('.params-modal');
            if (modal && e.target === modal) {
                this.hideParamsPopup();
            }
        });
    }

    showParamsPopup() {
        console.log('顯示參數彈窗');
        const modal = document.querySelector('.params-modal');
        if (modal) {
            modal.style.display = 'flex';
            modal.setAttribute('aria-hidden', 'false');
            
            // 聚焦到第一個輸入框
            const firstInput = modal.querySelector('input, select');
            if (firstInput) {
                firstInput.focus();
            }
        }
    }

    hideParamsPopup() {
        console.log('隱藏參數彈窗');
        const modal = document.querySelector('.params-modal');
        if (modal) {
            modal.style.display = 'none';
            modal.setAttribute('aria-hidden', 'true');
        }
    }

    handleParamsConfirm() {
        console.log('處理參數確認');
        // 這裡可以添加參數驗證和保存邏輯
        this.hideParamsPopup();
    }

    /* ==============================================
       鍵盤導航處理
       ============================================== */
    
    setupKeyboardNavigation() {
        console.log('設置鍵盤導航...');
        // 已在 setupEventDelegation 中設置
    }

    handleKeyboardNavigation(e) {
        // ESC 鍵關閉所有下拉選單和彈窗
        if (e.key === 'Escape') {
            console.log('ESC鍵被按下，關閉所有彈窗');
            this.closeAllDropdowns();
            
            const moreTools = document.getElementById('more-tools');
            if (moreTools && moreTools.open) {
                moreTools.open = false;
            }
            
            // 關閉參數彈窗
            this.hideParamsPopup();
        }

        // Tab 鍵導航
        if (e.key === 'Tab') {
            this.handleTabNavigation(e);
        }

        // 箭頭鍵導航
        if (['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(e.key)) {
            this.handleArrowNavigation(e);
        }
    }

    handleTabNavigation(e) {
        // Tab 鍵焦點管理
        const focusableElements = document.querySelectorAll(
            'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
        );
        
        const focusedIndex = Array.from(focusableElements).indexOf(document.activeElement);
        
        if (e.shiftKey) {
            // Shift+Tab 向前
            if (focusedIndex <= 0) {
                focusableElements[focusableElements.length - 1].focus();
                e.preventDefault();
            }
        } else {
            // Tab 向後
            if (focusedIndex >= focusableElements.length - 1) {
                focusableElements[0].focus();
                e.preventDefault();
            }
        }
    }

    handleArrowNavigation(e) {
        const activeElement = document.activeElement;
        
        // 時間框架按鈕間的箭頭導航
        if (activeElement.matches('.tf-btn')) {
            const buttons = Array.from(document.querySelectorAll('.tf-btn'));
            const currentIndex = buttons.indexOf(activeElement);
            
            if (e.key === 'ArrowLeft' && currentIndex > 0) {
                buttons[currentIndex - 1].focus();
                e.preventDefault();
            } else if (e.key === 'ArrowRight' && currentIndex < buttons.length - 1) {
                buttons[currentIndex + 1].focus();
                e.preventDefault();
            }
        }
    }

    /* ==============================================
       工具方法
       ============================================== */

    getCurrentTimeframe() {
        const activeBtn = document.querySelector('.tf-btn.active');
        return activeBtn ? activeBtn.dataset.timeframe : 'D1';
    }

    getCurrentSymbol() {
        const symbolElement = document.getElementById('current-symbol');
        return symbolElement ? symbolElement.textContent : 'XAUUSD';
    }

    getCurrentAlgorithm() {
        const algorithmElement = document.getElementById('current-algorithm');
        return algorithmElement ? algorithmElement.textContent : 'ZigZag';
    }
}

/* ==============================================
   初始化
   ============================================== */

// 確保只初始化一次
if (!window.responsiveToolbarInitialized) {
    window.responsiveToolbarInitialized = true;
    
    document.addEventListener('DOMContentLoaded', () => {
        console.log('DOM載入完成，初始化響應式工具欄...');
        window.responsiveToolbar = new ResponsiveToolbar();
    });

    // 如果DOM已經載入完成
    if (document.readyState !== 'loading') {
        console.log('DOM已載入，立即初始化響應式工具欄...');
        window.responsiveToolbar = new ResponsiveToolbar();
    }
}
