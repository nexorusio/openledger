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
        themeToggle.setAttribute(
            'aria-label',
            theme === 'dark' ? 'Use light theme' : 'Use dark theme'
        );
    }

    function closeMobileSidebar() {
        shell.classList.remove('sidebar-open');
        sidebarToggle.setAttribute('aria-expanded', 'false');
    }

    function toggleSidebar() {
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
    setTheme(storedTheme || 'light');

    if (
        desktopQuery.matches &&
        localStorage.getItem('openledger-sidebar-collapsed') === 'true'
    ) {
        shell.classList.add('sidebar-collapsed');
        sidebarToggle.setAttribute('aria-expanded', 'false');
    } else if (desktopQuery.matches) {
        sidebarToggle.setAttribute('aria-expanded', 'true');
    }

    sidebarToggle.addEventListener('click', toggleSidebar);
    sidebarClose.addEventListener('click', closeMobileSidebar);
    sidebarBackdrop.addEventListener('click', closeMobileSidebar);
    themeToggle.addEventListener('click', function () {
        setTheme(html.getAttribute('data-bs-theme') === 'dark' ? 'light' : 'dark');
    });
    desktopQuery.addEventListener('change', closeMobileSidebar);

    document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape') closeMobileSidebar();
    });

    if (window.lucide) window.lucide.createIcons({ attrs: { 'stroke-width': 1.8 } });
})();
