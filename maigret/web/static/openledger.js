(function () {
    const html = document.documentElement;
    const shell = document.getElementById('appShell');
    const sidebarToggle = document.getElementById('sidebarToggle');
    const sidebarClose = document.getElementById('sidebarClose');
    const sidebarBackdrop = document.getElementById('sidebarBackdrop');
    const themeToggle = document.getElementById('themeToggle');
    const desktopQuery = window.matchMedia('(min-width: 992px)');

    function setTheme(theme) {
        html.setAttribute('data-bs-theme', theme);
        localStorage.setItem('openledger-theme', theme);
        if (themeToggle) {
            themeToggle.setAttribute(
                'aria-label',
                theme === 'dark' ? 'Use light theme' : 'Use dark theme'
            );
        }
    }

    function closeMobileSidebar() {
        if (!shell || !sidebarToggle) return;
        shell.classList.remove('sidebar-open');
        sidebarToggle.setAttribute('aria-expanded', 'false');
    }

    function toggleSidebar() {
        if (!shell || !sidebarToggle) return;
        if (desktopQuery.matches) {
            const collapsed = shell.classList.toggle('sidebar-collapsed');
            localStorage.setItem('openledger-sidebar-collapsed', String(collapsed));
            sidebarToggle.setAttribute('aria-expanded', String(!collapsed));
        } else {
            const opened = shell.classList.toggle('sidebar-open');
            sidebarToggle.setAttribute('aria-expanded', String(opened));
        }
    }

    const storedTheme = localStorage.getItem('openledger-theme');
    setTheme(storedTheme || 'dark');

    if (
        shell &&
        sidebarToggle &&
        desktopQuery.matches &&
        localStorage.getItem('openledger-sidebar-collapsed') === 'true'
    ) {
        shell.classList.add('sidebar-collapsed');
        sidebarToggle.setAttribute('aria-expanded', 'false');
    } else if (sidebarToggle && desktopQuery.matches) {
        sidebarToggle.setAttribute('aria-expanded', 'true');
    }

    if (sidebarToggle) sidebarToggle.addEventListener('click', toggleSidebar);
    if (sidebarClose) sidebarClose.addEventListener('click', closeMobileSidebar);
    if (sidebarBackdrop) {
        sidebarBackdrop.addEventListener('click', closeMobileSidebar);
    }
    if (themeToggle) {
        themeToggle.addEventListener('click', function () {
            setTheme(html.getAttribute('data-bs-theme') === 'dark' ? 'light' : 'dark');
        });
    }
    if (shell) desktopQuery.addEventListener('change', closeMobileSidebar);

    document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape') closeMobileSidebar();
    });

    document.querySelectorAll('.case-delete-form').forEach(function (form) {
        form.addEventListener('submit', function (event) {
            const expected = form.dataset.caseTitle || '';
            const typed = window.prompt(
                'This deletion is permanent and cannot be undone.\n\n'
                + 'Type the exact case name to continue:\n' + expected
            );
            if (typed === null || typed !== expected) {
                event.preventDefault();
                if (typed !== null) window.alert('The case name did not match. Nothing was deleted.');
                return;
            }
            const confirmation = form.querySelector('input[name="confirmation_name"]');
            if (!confirmation) {
                event.preventDefault();
                return;
            }
            confirmation.value = typed;
        });
    });

    if (window.lucide) window.lucide.createIcons({ attrs: { 'stroke-width': 1.8 } });
})();
